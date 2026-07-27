#!/usr/bin/env python3
"""Stateless news-feed builder for GitHub Actions.

Pipeline (no database, no Redis):

  1. Load the list of publisher sitemaps from a Google Sheet
     (falls back to the committed ``sitemap_backup.csv``).
  2. For each sitemap, pull articles published within the recent window.
  3. Scrape OpenGraph metadata (title / description / image) for each article
     and tag categories/places from the URL.
  4. Merge with the previously committed ``feeds/feed_<lang>.json.gz`` files,
     dedupe by URL, drop stale entries, and write the gzipped feeds back
     (CDN-ready for GitHub Pages; the app fetches and gunzips them directly).

The committed feed files ARE the state, so consecutive hourly runs accumulate
articles instead of losing them. Everything is configurable via env vars so the
same script works locally and in CI.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from PIL import Image

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from content_filters import has_banned_content, is_banned_url  # noqa: E402

# --------------------------------------------------------------------------- #
# Configuration (env-overridable)
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parent.parent
FEEDS_DIR = ROOT / "feeds"

IST = timezone(timedelta(hours=5, minutes=30))

# A sitemap entry counts as "recent" if published within this many minutes.
# Kept generous so delayed scheduled runs don't miss articles (dupes are merged).
RECENT_MINUTES = int(os.getenv("RECENT_MINUTES", "90"))
MAX_PER_SITEMAP = int(os.getenv("MAX_PER_SITEMAP", "50"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
MAX_PER_FEED = int(os.getenv("MAX_PER_FEED", "2000"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
# Parallelism for network-bound sitemap + article fetching.
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
# Webstory sitemaps update slowly; use retention window (default 7d), not 90min.
WEBSTORY_RECENT_HOURS = float(
    os.getenv("WEBSTORY_RECENT_HOURS", str(RETENTION_DAYS * 24))
)
# Dominant-color extraction (tiny download + resize). Disable with COLOR_EXTRACT=0.
COLOR_EXTRACT = os.getenv("COLOR_EXTRACT", "1") != "0"
COLOR_DOWNLOAD_MAX_BYTES = int(os.getenv("COLOR_DOWNLOAD_MAX_BYTES", "98304"))
COLOR_SAMPLE_SIZE = int(os.getenv("COLOR_SAMPLE_SIZE", "64"))
COLOR_WORKERS = int(os.getenv("COLOR_WORKERS", "10"))
# Per-language cap for backfilling colors on existing default-gray articles.
COLOR_BACKFILL_MAX = int(os.getenv("COLOR_BACKFILL_MAX", "350"))

# Categories / title keywords to hard-drop from the feed. Daily राशिफल is
# published by many Hindi outlets at once, so corroboration would otherwise
# treat it as "popular" and flood the morning swipe. Comma-separated; empty
# string disables. Override via EXCLUDED_CATEGORIES / EXCLUDED_TITLE_KEYWORDS.
def _csv_set(env_name: str, default: str) -> set[str]:
    raw = os.getenv(env_name)
    if raw is None:
        raw = default
    return {p.strip().lower() for p in raw.split(",") if p.strip()}


EXCLUDED_CATEGORIES = _csv_set(
    "EXCLUDED_CATEGORIES",
    "astrology,horoscope,rashifal,panchang,jyotish",
)
EXCLUDED_TITLE_KEYWORDS = _csv_set(
    "EXCLUDED_TITLE_KEYWORDS",
    "राशिफल,पंचांग,कुंडली,ज्योतिष,rashifal,horoscope,panchang,astrology,"
    "aaj ka rashifal,aaj ka panchang,today's horoscope",
)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpdatoFeedBot/1.0; +https://updato.app)"}

SITEMAP_SHEET_URL = os.getenv(
    "SITEMAP_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1zya07VJMAhOGbjZh4LLnjgxzGlgA9yBFLqB2KJjDNT0/export?format=csv",
)
SITEMAP_BACKUP_CSV = ROOT / "sitemap_backup.csv"

CATEGORY_SHEET_URL = os.getenv(
    "CATEGORY_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1cmT_50D3vSSuduPErZeMbZmmfVUy8FieeMIyZNB9CGE/export?format=csv&gid=553342751",
)
CATEGORY_KEYWORDS_CSV = ROOT / "category_keywords.csv"
LOCATION_CSV = ROOT / "india_pincode_locations.csv"

SITEMAP_NS = {
    "ns": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}

PLACE_ABBREVIATIONS = {
    "uttar pradesh": "up", "madhya pradesh": "mp", "maharashtra": "mh",
    "bihar": "br", "uttarakhand": "uk", "andhra pradesh": "ap",
    "telangana": "tg", "tamil nadu": "tn", "karnataka": "ka", "kerala": "kl",
    "west bengal": "wb", "gujarat": "gj", "punjab": "pb", "haryana": "hr",
    "rajasthan": "rj", "chhattisgarh": "cg", "jharkhand": "jh", "odisha": "od",
    "delhi": "dl", "himachal pradesh": "hp", "new delhi": "delhi", "orisa": "odisha",
}

# Populated once by build_category_index().
PLACE_ALIAS_MAP: dict[str, str] = {}             # phrase -> full place name
KEYWORD_TO_CATEGORIES: dict[str, set[str]] = {}  # phrase -> {category names}
MAX_NGRAM = 1                                     # longest alias/keyword (words)


# --------------------------------------------------------------------------- #
# Category / place tagging
# --------------------------------------------------------------------------- #
def _load_csv_with_fallback(url: str, local_path: Path) -> pd.DataFrame:
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        local_path.write_bytes(resp.content)
        print(f"[ok] loaded sheet -> {local_path.name}")
        return pd.read_csv(local_path)
    except Exception as exc:
        print(f"[warn] sheet load failed ({exc}); using local {local_path.name}")
        return pd.read_csv(local_path)


def build_category_index() -> None:
    """Build fast lookup tables for place/category tagging.

    Instead of regex-testing every URL against ~10k aliases (the old slow path),
    all aliases and keywords are indexed into dicts keyed by the exact phrase.
    Tagging then only checks the word n-grams of each URL segment against these
    dicts -- a handful of hash lookups per article instead of tens of thousands
    of regex operations.
    """
    global PLACE_ALIAS_MAP, KEYWORD_TO_CATEGORIES, MAX_NGRAM

    try:
        location_df = pd.read_csv(LOCATION_CSV)
    except Exception as exc:
        print(f"[warn] could not read {LOCATION_CSV.name}: {exc}")
        location_df = pd.DataFrame()

    keyword_df = _load_csv_with_fallback(CATEGORY_SHEET_URL, CATEGORY_KEYWORDS_CSV)

    alias_map: dict[str, str] = {}
    for col in ("place_name", "admin_name1"):
        if col in location_df.columns:
            for name in location_df[col].dropna().str.lower().str.strip():
                alias_map[name] = name
                abbr = PLACE_ABBREVIATIONS.get(name)
                if abbr:
                    alias_map[abbr] = name
    PLACE_ALIAS_MAP = alias_map

    kw_to_cat: dict[str, set[str]] = {}
    for col in keyword_df.columns:
        cat = col.strip().lower()
        for kw in keyword_df[col].dropna().astype(str).str.strip().str.lower():
            if kw:
                kw_to_cat.setdefault(kw, set()).add(cat)
    KEYWORD_TO_CATEGORIES = kw_to_cat

    # Longest phrase (in words) we must consider when scanning URL segments.
    phrases = list(PLACE_ALIAS_MAP.keys()) + list(KEYWORD_TO_CATEGORIES.keys())
    MAX_NGRAM = max((len(p.split()) for p in phrases), default=1)

    n_cats = len({c for cats in kw_to_cat.values() for c in cats})
    print(f"[ok] categories={n_cats} place_aliases={len(PLACE_ALIAS_MAP)} "
          f"keywords={len(KEYWORD_TO_CATEGORIES)} max_ngram={MAX_NGRAM}")


def extract_categories_from_url(url: str) -> list[str]:
    try:
        path = urlparse(url).path.strip("/")
        segments = [s for s in path.split("/") if s]
        categories: set[str] = set()
        fallback: list[str] = []

        for seg in segments:
            s = seg.lower()
            if s.replace("-", "").isnumeric():
                continue
            if re.match(r"^(cid\d+|articleshow|videoshow|photoshow)[-_]?.*", s):
                continue
            if re.search(r"\.(cms|html|htm)$", s):
                continue

            tokens = s.replace("-", " ").split()
            if not tokens:
                continue

            matched = False
            max_n = min(MAX_NGRAM, len(tokens))
            for n in range(1, max_n + 1):
                for i in range(len(tokens) - n + 1):
                    phrase = " ".join(tokens[i:i + n])
                    place = PLACE_ALIAS_MAP.get(phrase)
                    if place:
                        categories.add("places")
                        categories.add(place)
                        matched = True
                    cats = KEYWORD_TO_CATEGORIES.get(phrase)
                    if cats:
                        categories.update(cats)
                        matched = True

            if not matched:
                fallback.append(s)

        if not categories and fallback:
            return fallback[:2]
        return sorted(categories)
    except Exception as exc:
        print(f"[warn] category extraction failed for {url}: {exc}")
        return []


# --------------------------------------------------------------------------- #
# Dominant color (lightweight image sample for card gradients)
# --------------------------------------------------------------------------- #
DEFAULT_DOMINANT_COLOR = "#444444"
DEFAULT_TEXT_COLOR = "#FFFFFF"  # readable on the dark default background


def _is_default_color(hex_color: str | None) -> bool:
    if not hex_color:
        return True
    return hex_color.strip().lower() in ("", DEFAULT_DOMINANT_COLOR.lower())


def _parse_rgb(hex_color: str) -> tuple[int, int, int] | None:
    try:
        h = hex_color.strip().lstrip("#")
        if len(h) != 6:
            return None
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    except Exception:
        return None


def _luminance(r: int, g: int, b: int) -> float:
    return 0.299 * r + 0.587 * g + 0.114 * b


def _chroma(r: int, g: int, b: int) -> int:
    return max(r, g, b) - min(r, g, b)


def _contrast_text_color(r: int, g: int, b: int) -> str:
    # Match app luminance threshold (~160 on 0–255).
    return "#000000" if _luminance(r, g, b) > 160 else "#FFFFFF"


def _needs_color_refresh(hex_color: str | None) -> bool:
    """True if missing, default gray, near-black/white, or flat gray (looks B&W)."""
    if _is_default_color(hex_color):
        return True
    rgb = _parse_rgb(str(hex_color))
    if not rgb:
        return True
    r, g, b = rgb
    lum = _luminance(r, g, b)
    if lum < 50 or lum > 230:
        return True
    if _chroma(r, g, b) < 18:
        return True
    return False


def _for_card_ui(r: int, g: int, b: int) -> tuple[int, int, int]:
    """Lift dark / mute extremes so card gradients read as a tint, not B&W."""
    lum = _luminance(r, g, b)
    # Pull very dark colors up toward a mid tone while keeping hue.
    if lum < 85:
        target = 110.0
        scale = target / max(lum, 1.0)
        r = min(255, int(r * scale))
        g = min(255, int(g * scale))
        b = min(255, int(b * scale))
        # Blend a little toward soft gray-blue so it never looks pure black.
        r = int(r * 0.82 + 55)
        g = int(g * 0.82 + 60)
        b = int(b * 0.82 + 70)
    elif lum > 210:
        r = int(r * 0.75 + 30)
        g = int(g * 0.75 + 30)
        b = int(b * 0.75 + 35)
    # Mild saturation bump for muddy photos.
    avg = (r + g + b) / 3.0
    if _chroma(r, g, b) < 40:
        r = int(max(0, min(255, avg + (r - avg) * 1.45)))
        g = int(max(0, min(255, avg + (g - avg) * 1.45)))
        b = int(max(0, min(255, avg + (b - avg) * 1.45)))
    return r, g, b


def extract_dominant_color(image_url: str | None) -> tuple[str, str]:
    """Download a small prefix of the image and pick a colorful UI tint.

    Returns (dominant_hex, text_hex). Falls back to defaults on any failure.
    """
    if not COLOR_EXTRACT or not image_url or not str(image_url).startswith("http"):
        return DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR
    try:
        headers = {
            **HEADERS,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        with requests.get(
            image_url,
            timeout=min(REQUEST_TIMEOUT, 10),
            headers=headers,
            stream=True,
        ) as res:
            if res.status_code != 200:
                return DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR
            chunks: list[bytes] = []
            total = 0
            for chunk in res.iter_content(8192):
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total >= COLOR_DOWNLOAD_MAX_BYTES:
                    break
        data = b"".join(chunks)
        if len(data) < 64:
            return DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR

        img = Image.open(BytesIO(data))
        img = img.convert("RGB")
        img.thumbnail((COLOR_SAMPLE_SIZE, COLOR_SAMPLE_SIZE))

        # Prefer chromatic mid-tones over muddy average / near-black.
        scored: list[tuple[float, int, int, int]] = []
        for r, g, b in img.getdata():
            ch = _chroma(r, g, b)
            lum = _luminance(r, g, b)
            if ch < 22:
                continue
            if lum < 35 or lum > 225:
                continue
            # Weight: chroma first, prefer mid luminance.
            mid = 1.0 - abs(lum - 120) / 120.0
            scored.append((ch * (0.55 + 0.45 * mid), r, g, b))

        if scored:
            scored.sort(reverse=True)
            top = scored[: max(8, len(scored) // 5)]
            r = sum(x[1] for x in top) // len(top)
            g = sum(x[2] for x in top) // len(top)
            b = sum(x[3] for x in top) // len(top)
        else:
            # Fallback: median-cut palette, skip neutrals.
            quantized = img.quantize(colors=8, method=Image.Quantize.MEDIANCUT)
            palette = quantized.getpalette() or []
            counts: dict[int, int] = {}
            for px in quantized.getdata():
                counts[px] = counts.get(px, 0) + 1
            candidates: list[tuple[float, int, int, int]] = []
            for idx, count in counts.items():
                base = idx * 3
                if base + 2 >= len(palette):
                    continue
                r, g, b = palette[base], palette[base + 1], palette[base + 2]
                ch = _chroma(r, g, b)
                lum = _luminance(r, g, b)
                if lum < 30 or lum > 230:
                    continue
                candidates.append((count * (1 + ch / 64.0), r, g, b))
            if not candidates:
                return DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR
            candidates.sort(reverse=True)
            _, r, g, b = candidates[0]

        r, g, b = _for_card_ui(r, g, b)
        hex_color = f"#{r:02x}{g:02x}{b:02x}"
        return hex_color, _contrast_text_color(r, g, b)
    except Exception:
        return DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR


def load_color_cache() -> dict[str, tuple[str, str]]:
    """image_url -> (dominant_color, text_color) from existing feeds."""
    cache: dict[str, tuple[str, str]] = {}
    for path in FEEDS_DIR.glob("feed_*.json.gz"):
        lang = path.name[len("feed_") : -len(".json.gz")]
        for art in load_existing_feed(lang):
            img = art.get("image_url")
            dc = art.get("dominant_color")
            if not img or _needs_color_refresh(dc):
                continue
            tc = art.get("text_color") or DEFAULT_TEXT_COLOR
            cache[str(img)] = (str(dc), str(tc))
    return cache


def apply_dominant_colors(
    articles: list[dict],
    cache: dict[str, tuple[str, str]] | None = None,
    *,
    limit: int | None = None,
) -> dict[str, tuple[str, str]]:
    """Fill dominant_color / text_color for articles still on gray / near-B&W.

    Reuses ``cache`` (mutated with new extractions). Only fetches unique image
    URLs, capped by ``limit`` when set.
    """
    if not COLOR_EXTRACT or not articles:
        return cache or {}
    cache = dict(cache or {})

    # Apply cached colors first.
    need_urls: list[str] = []
    seen_need: set[str] = set()
    for art in articles:
        img = art.get("image_url")
        if not img or not str(img).startswith("http"):
            continue
        img = str(img)
        if img in cache:
            art["dominant_color"], art["text_color"] = cache[img]
            continue
        if not _needs_color_refresh(art.get("dominant_color")):
            cache[img] = (
                str(art["dominant_color"]),
                str(art.get("text_color") or DEFAULT_TEXT_COLOR),
            )
            continue
        if img not in seen_need:
            seen_need.add(img)
            need_urls.append(img)

    if limit is not None:
        need_urls = need_urls[: max(0, limit)]

    if not need_urls:
        return cache

    print(f"[..] extracting dominant colors for {len(need_urls)} images "
          f"({COLOR_WORKERS} workers)")
    extracted = 0
    with ThreadPoolExecutor(max_workers=max(1, COLOR_WORKERS)) as pool:
        futures = {pool.submit(extract_dominant_color, url): url for url in need_urls}
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                dc, tc = fut.result()
            except Exception:
                dc, tc = DEFAULT_DOMINANT_COLOR, DEFAULT_TEXT_COLOR
            cache[url] = (dc, tc)
            if not _is_default_color(dc):
                extracted += 1

    for art in articles:
        img = art.get("image_url")
        if img and str(img) in cache:
            art["dominant_color"], art["text_color"] = cache[str(img)]

    print(f"    colors: {extracted}/{len(need_urls)} non-default")
    return cache


def _merge_article(existing: dict | None, incoming: dict) -> dict:
    """Merge feed rows; keep a good dominant color if the new scrape is weak."""
    if not existing:
        return {k: v for k, v in incoming.items()
                if not str(k).startswith("_")
                and k not in ("score", "source_count", "id", "published", "comments")}
    merged = {**existing, **incoming}
    old_dc = existing.get("dominant_color")
    new_dc = incoming.get("dominant_color")
    if _needs_color_refresh(new_dc) and not _needs_color_refresh(old_dc):
        merged["dominant_color"] = old_dc
        merged["text_color"] = existing.get("text_color") or merged.get("text_color")
    return {k: v for k, v in merged.items()
            if not str(k).startswith("_")
            and k not in ("score", "source_count", "id", "published", "comments")}


# --------------------------------------------------------------------------- #
# Sitemap + article scraping
# --------------------------------------------------------------------------- #
def load_sitemaps() -> list[dict]:
    try:
        print("[..] fetching sitemap list from Google Sheet")
        df = pd.read_csv(SITEMAP_SHEET_URL)
        df.to_csv(SITEMAP_BACKUP_CSV, index=False)
        print(f"[ok] {len(df)} sitemaps from sheet")
        return df.to_dict(orient="records")
    except Exception as exc:
        print(f"[warn] sheet failed ({exc}); using {SITEMAP_BACKUP_CSV.name}")
        try:
            return pd.read_csv(SITEMAP_BACKUP_CSV).to_dict(orient="records")
        except Exception as exc2:
            print(f"[error] no sitemap list available: {exc2}")
            return []


def _parse_xml(content: bytes):
    """Parse sitemap XML, tolerating the malformed markup many publishers ship."""
    try:
        return ET.fromstring(content)
    except ET.ParseError:
        pass

    try:  # lxml's recover mode salvages most real-world sitemaps.
        from lxml import etree

        root = etree.fromstring(content, parser=etree.XMLParser(recover=True, resolve_entities=False))
        if root is not None:
            return root
    except Exception:
        pass

    # Last resort: strip control chars + escape stray ampersands, then retry.
    text = content.decode("utf-8", errors="ignore")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    text = re.sub(r"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;", text)
    return ET.fromstring(text)


def fetch_articles_from_sitemap(sitemap_url: str) -> list[dict]:
    try:
        res = requests.get(sitemap_url, timeout=30, headers=HEADERS)
        if res.status_code != 200:
            print(f"[warn] HTTP {res.status_code} for {sitemap_url}")
            return []
        # Some "sitemap" URLs (e.g. newsnation date links) return HTML pages.
        ctype = (res.headers.get("content-type") or "").lower()
        head = res.content.lstrip()[:64].lower()
        if "text/html" in ctype or head.startswith(b"<!doctype") or head.startswith(b"<html"):
            print(f"[warn] not XML sitemap (got HTML) for {sitemap_url}")
            return []
        root = _parse_xml(res.content)
    except Exception as exc:
        print(f"[warn] failed to parse sitemap {sitemap_url}: {exc}")
        return []

    is_ws_sitemap = _is_webstory_sitemap(sitemap_url)
    window = (
        timedelta(hours=WEBSTORY_RECENT_HOURS)
        if is_ws_sitemap
        else timedelta(minutes=RECENT_MINUTES)
    )

    # Collect everything inside the window first, then take the newest N.
    # (Sitemap order is not always newest-first, and early-break dropped webstories.)
    candidates: list[dict] = []
    for url_tag in root.findall("ns:url", SITEMAP_NS):
        loc_tag = url_tag.find("ns:loc", SITEMAP_NS)
        # NB: avoid `a or b` on XML elements -- lxml treats a childless element
        # as falsy even when it has text, which would skip valid dates.
        pub_date_tag = url_tag.find("news:news/news:publication_date", SITEMAP_NS)
        if pub_date_tag is None:
            pub_date_tag = url_tag.find("ns:lastmod", SITEMAP_NS)
        if loc_tag is None or pub_date_tag is None:
            continue

        try:
            pub_time = date_parser.parse(pub_date_tag.text.strip())
            now = datetime.now(pub_time.tzinfo) if pub_time.tzinfo else datetime.now()
            if (now - pub_time) > window:
                continue
        except Exception:
            continue

        title_tag = url_tag.find("news:news/news:title", SITEMAP_NS)
        image_tag = url_tag.find("image:image/image:loc", SITEMAP_NS)
        candidates.append({
            "url": loc_tag.text.strip(),
            "title": title_tag.text.strip() if title_tag is not None else "",
            "image": image_tag.text.strip() if image_tag is not None else "",
            "published_time": pub_time,
            "from_webstory_sitemap": is_ws_sitemap,
        })

    candidates.sort(key=lambda a: a["published_time"], reverse=True)
    return candidates[:MAX_PER_SITEMAP]


def _is_webstory_sitemap(url: str) -> bool:
    u = (url or "").lower()
    return any(
        m in u
        for m in (
            "webstory", "webstories", "web-stories", "visualstories", "visual-stories",
        )
    )


def _decode_html(res: requests.Response) -> str:
    """Decode article HTML safely for Indic publishers.

    Many sites (e.g. andhrajyothy.com) ship UTF-8 bodies but omit charset in
    Content-Type. requests then defaults to ISO-8859-1, turning Telugu into
    mojibake like 'à°\x88à°¸à°¾…'. Prefer UTF-8 / meta charset over that guess.
    """
    raw = res.content or b""
    ctype = (res.headers.get("content-type") or "").lower()

    declared = None
    m = re.search(r"charset=([\w-]+)", ctype)
    if m:
        declared = m.group(1)
    else:
        hm = re.search(br"charset=['\"]?([\w-]+)", raw[:8192], re.I)
        if hm:
            declared = hm.group(1).decode("ascii", errors="ignore")

    candidates: list[str] = []
    if declared:
        candidates.append(declared)
    candidates.extend(["utf-8", "utf-8-sig"])
    if res.apparent_encoding:
        candidates.append(res.apparent_encoding)

    seen: set[str] = set()
    for enc in candidates:
        key = enc.lower().replace("_", "-")
        if key in seen:
            continue
        seen.add(key)
        # Skip the broken default when charset was missing.
        if key in {"iso-8859-1", "latin-1", "latin1"} and not declared:
            continue
        try:
            return raw.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue

    return raw.decode("utf-8", errors="replace")


def _parse_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _dims_from_image_url(image_url: str | None) -> tuple[int, int] | None:
    """Extract width/height from CDN query strings like width=1200&height=675."""
    if not image_url:
        return None
    lower = image_url.lower()
    w_m = re.search(r"(?:[?&]|[,/])width[=:_-]?(\d{2,5})", lower)
    h_m = re.search(r"(?:[?&]|[,/])height[=:_-]?(\d{2,5})", lower)
    if not w_m or not h_m:
        return None
    w, h = _parse_int(w_m.group(1)), _parse_int(h_m.group(1))
    if not w or not h or w <= 0 or h <= 0:
        return None
    return w, h


def _detect_image_vertical(image_url: str | None, soup: BeautifulSoup) -> bool | None:
    """Return True/False when dimensions are known; None if unknown."""

    def og_int(prop: str) -> int | None:
        tag = soup.find("meta", property=prop)
        if not tag or not tag.get("content"):
            return None
        return _parse_int(tag["content"])

    w, h = og_int("og:image:width"), og_int("og:image:height")
    if w and h:
        return h > w
    dims = _dims_from_image_url(image_url)
    if dims:
        return dims[1] > dims[0]
    return None


def scrape_article(entry: dict, lang: str) -> dict | None:
    url = entry["url"]
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        if res.status_code != 200:
            print(f"[warn] HTTP {res.status_code} for {url}")
            return None
        soup = BeautifulSoup(_decode_html(res), "html.parser")

        def og(prop: str) -> str | None:
            tag = soup.find("meta", property=prop)
            return tag["content"].strip() if tag and tag.get("content") else None

        title = og("og:title") or entry.get("title") or ""
        description = og("og:description") or ""
        image_url = og("og:image") or entry.get("image") or None

        # If scrape still looks like latin1-mojibake, prefer sitemap title.
        if _looks_like_mojibake(title) and entry.get("title"):
            title = entry["title"]
        title = _repair_mojibake(title)
        description = clean_description(_repair_mojibake(description))

        pub_time = entry["published_time"]
        raw_time = (pub_time.astimezone(IST) if pub_time.tzinfo else pub_time.replace(tzinfo=IST)).isoformat()

        categories = extract_categories_from_url(url)
        is_ws = bool(
            entry.get("from_webstory_sitemap")
            or _is_webstory({"url": url, "categories": categories})
        )

        # Portrait vs landscape from OG dims / image URL (not merely "is webstory").
        detected = _detect_image_vertical(image_url, soup)
        if detected is not None:
            is_vertical = detected
        else:
            # Unknown dims: real AMP web stories are usually portrait; else not.
            is_vertical = bool(is_ws)

        # Only vertical cards get the webstories tag; landscape → secondary/misc.
        categories = [c for c in categories if str(c).lower() != "webstories"]
        if is_vertical and is_ws:
            categories = list(categories) + ["webstories"]
        elif not categories:
            categories = ["misc"]

        return {
            "url": url,
            "title": title,
            "description": description,
            "image_url": image_url,
            "dominant_color": DEFAULT_DOMINANT_COLOR,
            "text_color": DEFAULT_TEXT_COLOR,
            "is_vertical": is_vertical,
            "lang": lang,
            "categories": categories,
            "raw_time": raw_time,
        }
    except Exception as exc:
        print(f"[warn] scrape failed for {url}: {exc}")
        return None


def _looks_like_mojibake(text: str) -> bool:
    if not text:
        return False
    # UTF-8 Indic misread as Latin-1 commonly starts with these digraphs.
    markers = ("à¤", "à¥", "à¦", "à§", "à¨", "à©", "àª", "à«", "à®", "à¯", "à°", "à±", "à²", "à³", "à´", "àµ")
    return any(m in text for m in markers)


def _repair_mojibake(text: str) -> str:
    """Undo UTF-8-as-Latin-1 mojibake when present; otherwise return unchanged."""
    if not text or not _looks_like_mojibake(text):
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
        # Prefer repaired if it gained Indic characters.
        if sum("\u0900" <= c <= "\u0d7f" for c in repaired) >= sum(
            "\u0900" <= c <= "\u0d7f" for c in text
        ):
            return repaired
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return text


# Common publisher / wire / category tails publishers append to OG descriptions.
_KNOWN_PUBLISHER_TAILS = (
    "times now", "times now navbharat", "navbharat", "times of india", "toi",
    "ndtv", "aaj tak", "india today", "moneycontrol", "hindustan times",
    "the hindu", "indian express", "ani", "pti", "reuters", "bloomberg",
    "bbc", "cnn", "zee news", "abp", "news18", "firstpost", "scroll",
    "the wire", "print", "livemint", "economic times", "business standard",
    "dnaindia", "oneindia", "jagran", "amar ujala", "bhaskar", "patrika",
)


def _looks_like_attribution_segment(segment: str) -> bool:
    """True for trailing bits like ``Times Now Navbharat`` or ``लाइफस्टाइल News``."""
    s = segment.strip(" .·•|/-—–")
    if not s or len(s) > 55:
        return False
    low = s.lower()
    if any(p in low for p in _KNOWN_PUBLISHER_TAILS):
        return True
    # ``Lifestyle News`` / ``लाइफस्टाइल News`` category tags
    if re.search(r"\bnews\b", low) and len(s.split()) <= 4:
        return True
    # Short Title-Case Latin brand: ``Moneycontrol``, ``The Hindu``
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9&'.\-\s]{0,48}", s):
        words = [w for w in s.split() if w]
        if 1 <= len(words) <= 5:
            titled = sum(1 for w in words if w[:1].isupper())
            if titled >= max(1, len(words) // 2):
                return True
    return False


def clean_description(text: str) -> str:
    """Strip trailing publisher / category attributions from OG descriptions.

    e.g. ``…आवाज बनीं बेटियां, लाइफस्टाइल News, Times Now Navbharat.``
      → ``…आवाज बनीं बेटियां``
    """
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return text

    # Peel ``, Foo`` / ``| Foo`` / ``— Foo`` / `` - Foo`` tails.
    # Do NOT treat mid-word hyphens (rate-cut) as separators.
    tail_re = re.compile(
        r"(?:,\s*|\|\s*|[•·]\s*|/\s*|—\s*|–\s*|\s+-\s+)"
        r"([^,|•·/—–]{1,60})$"
    )
    peeled = False
    for _ in range(4):
        m = tail_re.search(text)
        if not m:
            break
        if not _looks_like_attribution_segment(m.group(1)):
            break
        text = text[: m.start()].rstrip(" ,.|•·/—–")
        peeled = True

    # ``. Moneycontrol`` / ``. Times of India`` (period + Latin brand, no comma)
    m = re.search(r"\.\s+([A-Z][A-Za-z0-9&'.\-\s]{1,40})\.?$", text)
    if m and _looks_like_attribution_segment(m.group(1)):
        text = text[: m.start()].rstrip()
        peeled = True

    # ``(ANI)`` / ``(PTI)`` wire credit at the end
    wired = re.sub(
        r"\s*\((?:ANI|PTI|IANS|AFP|AP|Reuters)\)\s*\.?$",
        "",
        text,
        flags=re.I,
    )
    if wired != text:
        peeled = True
        text = wired

    # Only strip a trailing period if we removed an attribution (keep normal
    # sentence-final periods otherwise).
    if peeled:
        text = text.rstrip(" ,.|")
    return text.strip()


def is_acceptable(article: dict) -> bool:
    if not article or not article.get("title"):
        return False
    if is_banned_url(article["url"]):
        return False
    if has_banned_content(f"{article.get('title', '')} {article.get('description', '')}"):
        return False
    return True


# --------------------------------------------------------------------------- #
# Feed persistence (merge / dedupe / retention)
# --------------------------------------------------------------------------- #
def _parse_ist(ts: str) -> datetime:
    try:
        dt = date_parser.parse(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=IST)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=IST)


def _relative_time(ts: str) -> str:
    minutes = (datetime.now(IST) - _parse_ist(ts)).total_seconds() // 60
    if minutes < 0:
        return "just now"
    if minutes < 60:
        return f"{int(minutes)} minutes ago"
    if minutes < 1440:
        return f"{int(minutes // 60)} hours ago"
    return f"{int(minutes // 1440)} days ago"


def load_existing_feed(lang: str) -> list[dict]:
    path = FEEDS_DIR / f"feed_{lang}.json.gz"
    if not path.exists():
        return []
    try:
        raw = gzip.decompress(path.read_bytes())
        return json.loads(raw.decode("utf-8")).get("feed", [])
    except Exception as exc:
        print(f"[warn] could not read {path.name}: {exc}")
        return []


# --------------------------------------------------------------------------- #
# Popularity ranking + topic diversification (URL-token based)
# --------------------------------------------------------------------------- #
# Cross-publisher "same story" detection uses English/romanized URL slugs
# (reliable across Hindi/Tamil/etc.). One best article per topic cluster is
# kept so the swipe feed never shows the same story from 3 publishers in a row.
LANG_SUBDOMAINS = {
    "hindi", "english", "bangla", "bengali", "tamil", "telugu", "kannada",
    "malayalam", "hin", "tam", "tel", "kan", "ben", "mal", "eng", "mr", "guj",
}
HIGH_REPUTATION = {
    "moneycontrol.com", "livemint.com", "ndtvprofit.com", "indiatoday.in",
    "hindustantimes.com", "indianexpress.com", "newindianexpress.com",
    "timesnownews.com", "tribuneindia.com", "businesstoday.in", "cnbctv18.com",
    "economictimes.com", "news18.com", "aajtak.in", "zeenews.india.com",
    "jagran.com", "eenadu.net", "andhrajyothy.com", "vijaykarnataka.com",
    "dinamalar.com", "dinamani.com", "madhyamam.com", "thelallantop.com",
    "downtoearth.org.in", "indiatimes.com", "navbharattimes.indiatimes.com",
    "samayam.com", "news9live.com", "abplive.com", "india.com",
}
LOW_REPUTATION = {
    "koimoi.com", "pinkvilla.com", "bollywoodhungama.com", "mayapuri.com",
    "herzindagi.com", "samacharnama.com", "lalluram.com", "icifmede.com",
    "bhadas4media.com", "sachkahoon.com", "ekhabartoday.com",
    "keralaonlinenews.com", "sathyamonline.com", "vishwavani.news",
    "news7tamil.live", "people.com",
}
CATEGORY_WEIGHTS = {
    "national": 1.0, "nation": 1.0, "india": 1.0, "indian": 0.95, "politics": 1.0,
    "world": 1.0, "world-news": 1.0, "international": 1.0, "defence": 0.9, "defense": 0.9,
    "business": 0.9, "economy": 0.9, "finance": 0.9, "market": 0.9, "markets": 0.9,
    "science": 0.85, "health": 0.85,
    "technology": 0.75, "tech": 0.75, "education": 0.75,
    "sports": 0.6, "sport": 0.6, "cricket": 0.6,
    "auto": 0.5, "automobile": 0.5,
    "entertainment": 0.4, "bollywood": 0.4, "movies": 0.4,
    "lifestyle": 0.3, "viral": 0.3, "trending": 0.3, "misc": 0.25,
    "astrology": 0.15, "horoscope": 0.15,
    # Webstories compete in the swipe feed (same ballpark as lifestyle/entertainment).
    "webstories": 0.45, "photos": 0.35, "gallery": 0.35, "videos": 0.35,
}
URL_STOPWORDS = {
    "news", "story", "stories", "article", "articleshow", "videoshow",
    "photoshow", "live", "liveblog", "video", "videos", "photo", "photos",
    "gallery", "web", "webstory", "webstories", "visualstories", "slideshow",
    "amp", "html", "htm", "cms", "www", "com", "latest", "breaking", "update",
    "updates", "watch", "read", "big", "exclusive", "detail", "details",
    "content", "index", "home", "google", "sitemap", "feed", "rss", "post",
    "the", "and", "for", "with", "from", "this", "that", "into", "over",
    "after", "before", "amid", "will", "says", "said", "why", "how", "what",
    "when", "who", "top", "new", "get", "you", "your", "his", "her", "are",
    "world", "india", "national", "international", "state", "states", "city",
    "sports", "sport", "business", "entertainment", "tech", "technology",
    "lifestyle", "health", "education", "auto", "astrology", "viral",
    "trending", "bollywood", "movies", "cricket", "nation", "regional",
}
WEBSTORY_URL_MARKERS = (
    "web-stories", "webstory", "webstories", "visualstories", "photo-story",
    "slideshow", "/photos/", "/gallery/", "/videos/",
)
SIM_THRESHOLD = float(os.getenv("SIM_THRESHOLD", "0.5"))
MIN_SHARED = int(os.getenv("MIN_SHARED", "2"))
RANK_WINDOW_HOURS = float(os.getenv("RANK_WINDOW_HOURS", "48"))


def _publisher(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if parts and parts[0] in LANG_SUBDOMAINS:
        parts = parts[1:]
    return ".".join(parts)


def _source_weight(url: str) -> float:
    pub = _publisher(url)
    if pub in HIGH_REPUTATION:
        return 1.0
    if pub in LOW_REPUTATION:
        return 0.25
    return 0.6


def _url_tokens(url: str) -> set[str]:
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return set()
    tokens: set[str] = set()
    for seg in path.split("/"):
        seg = re.sub(r"\.(cms|html?|amp)$", "", seg)
        if not seg or seg.replace("-", "").replace("_", "").isnumeric():
            continue
        for tok in re.split(r"[-_]", seg):
            if len(tok) < 3 or not tok.isalpha() or tok in URL_STOPWORDS:
                continue
            tokens.add(tok)
    return tokens


def _is_webstory(article: dict) -> bool:
    cats = article.get("categories") or []
    if isinstance(cats, list) and any(str(c).lower() == "webstories" for c in cats):
        return True
    url = (article.get("url") or "").lower()
    return any(m in url for m in WEBSTORY_URL_MARKERS)


def _is_excluded(article: dict) -> bool:
    """True if this article's category or title matches a blocked category/keyword."""
    if EXCLUDED_CATEGORIES:
        cats = article.get("categories") or []
        if isinstance(cats, str):
            cats = [cats]
        for c in cats:
            cl = str(c).lower().strip()
            if cl in EXCLUDED_CATEGORIES:
                return True
            # Partial match so "aaj-ka-rashifal" style tags still hit.
            if any(x in cl for x in EXCLUDED_CATEGORIES):
                return True

    if EXCLUDED_TITLE_KEYWORDS:
        blob = f"{article.get('title', '')} {article.get('description', '')} {article.get('url', '')}".lower()
        if any(kw in blob for kw in EXCLUDED_TITLE_KEYWORDS):
            return True
    return False


def _category_weight(article: dict) -> float:
    cats = article.get("categories") or []
    if isinstance(cats, str):
        cats = [cats]
    weights = [
        CATEGORY_WEIGHTS[str(c).lower()]
        for c in cats
        if str(c).lower() in CATEGORY_WEIGHTS
    ]
    return max(weights) if weights else 0.55


def _age_hours(article: dict) -> float:
    ts = str(article.get("raw_time", "")).strip()
    if not ts:
        return 1e9
    try:
        age = (datetime.now(IST) - _parse_ist(ts)).total_seconds() / 3600.0
        return max(0.0, age)
    except Exception:
        return 1e9


def _primary_category(article: dict) -> str:
    cats = article.get("categories") or []
    if isinstance(cats, str) and cats:
        return cats.split(",")[0].strip().lower()
    if isinstance(cats, list) and cats:
        return str(cats[0]).lower()
    return ""


def _best_rep(arts: list[dict]) -> dict:
    return max(arts, key=lambda a: (
        1 if (a.get("image_url") or "").startswith("http") else 0,
        _source_weight(a.get("url", "")),
        min(len(a.get("title") or ""), 90),
        -_age_hours(a),
    ))


def rank_and_diversify(articles: list[dict], limit: int) -> list[dict]:
    """Collapse same-topic clusters, rank by popularity, diversify feed order.

    - Same story from many publishers → one best article (no back-to-back repeats).
    - Score = corroboration + source reputation + category + recency.
    - Ordering avoids same publisher / category streaks so the swipe feed
      feels varied rather than three World Cup cards in a row.
    """
    if not articles:
        return []

    # Drop blocked categories/keywords only — webstories stay in the swipe feed.
    pool = [
        a for a in articles
        if (a.get("title") or "").strip() and not _is_excluded(a)
    ]
    dropped = len(articles) - len(pool)
    if not pool:
        pool = [a for a in articles if (a.get("title") or "").strip()]
    if dropped:
        print(f"    excluded {dropped} articles (blocked categories/keywords)")

    for a in pool:
        a["_tokens"] = _url_tokens(a.get("url", ""))
        a["_domain"] = _publisher(a.get("url", ""))

    # Newest-first helps greedy clustering attach to fresher seeds.
    pool.sort(key=lambda a: _parse_ist(a.get("raw_time", "")), reverse=True)

    clusters: list[dict] = []
    for art in pool:
        toks = art["_tokens"]
        best, best_ov = None, 0.0
        if toks:
            for c in clusters:
                inter = len(toks & c["tokens"])
                if inter < MIN_SHARED:
                    continue
                ov = inter / (min(len(toks), len(c["tokens"])) or 1)
                if ov >= SIM_THRESHOLD and ov > best_ov:
                    best, best_ov = c, ov
        if best is not None:
            best["arts"].append(art)
            best["tokens"] |= toks
        else:
            clusters.append({"tokens": set(toks), "arts": [art]})

    scored: list[dict] = []
    for i, c in enumerate(clusters):
        sources = {a["_domain"] for a in c["arts"] if a["_domain"]}
        rep = dict(_best_rep(c["arts"]))  # copy so we don't mutate shared refs oddly
        newest_age = min(_age_hours(a) for a in c["arts"])
        recency = max(0.0, 1.0 - newest_age / RANK_WINDOW_HOURS) if RANK_WINDOW_HOURS else 0.0
        score = (
            2.0 * len(sources)
            + 1.5 * max(_source_weight(a.get("url", "")) for a in c["arts"])
            + 1.0 * _category_weight(rep)
            + 0.8 * recency
            + 0.3 * (1.0 if (rep.get("image_url") or "").startswith("http") else 0.0)
        )
        # Strip helper keys before writing to the CDN feed.
        for key in ("_tokens", "_domain"):
            rep.pop(key, None)
        rep["source_count"] = len(sources)
        rep["score"] = round(score, 2)
        scored.append({
            "id": i,
            "score": score,
            "domain": _publisher(rep.get("url", "")),
            "category": _primary_category(rep),
            "article": rep,
        })

    # Greedy diversify: always take the highest remaining score that doesn't
    # repeat the previous publisher or category when alternatives exist.
    remaining = sorted(scored, key=lambda x: x["score"], reverse=True)
    ordered: list[dict] = []
    last_domain = None
    last_category = None
    while remaining and len(ordered) < limit:
        pick_idx = 0
        for i, cand in enumerate(remaining):
            same_dom = last_domain and cand["domain"] == last_domain
            same_cat = last_category and cand["category"] and cand["category"] == last_category
            if not same_dom and not same_cat:
                pick_idx = i
                break
            # Soft preference: allow same category if different publisher,
            # but still avoid same publisher back-to-back when possible.
            if same_dom:
                continue
            if not same_dom:
                pick_idx = i
                break
        chosen = remaining.pop(pick_idx)
        ordered.append(chosen["article"])
        last_domain = chosen["domain"]
        last_category = chosen["category"] or last_category

    multi = sum(1 for a in ordered if (a.get("source_count") or 1) >= 2)
    print(f"    ranked: {len(pool)} -> {len(clusters)} topics -> {len(ordered)} "
          f"(multi-source={multi})")
    return ordered


def write_feed(
    lang: str,
    articles: list[dict],
    color_cache: dict[str, tuple[str, str]] | None = None,
) -> int:
    cutoff = datetime.now(IST) - timedelta(days=RETENTION_DAYS)

    merged: dict[str, dict] = {}
    for art in load_existing_feed(lang) + articles:
        url = art.get("url")
        if not url:
            continue
        merged[url] = _merge_article(merged.get(url), art)

    kept = [a for a in merged.values() if _parse_ist(a.get("raw_time", "")) >= cutoff]

    # Backfill colors for articles still on the gray default (capped per run).
    apply_dominant_colors(kept, color_cache, limit=COLOR_BACKFILL_MAX)

    kept = rank_and_diversify(kept, MAX_PER_FEED)

    for a in kept:
        a["id"] = hashlib.md5(a["url"].encode("utf-8")).hexdigest()[:12]
        a["published"] = _relative_time(a.get("raw_time", ""))
        a.setdefault("comments", 0)

    FEEDS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"feed": kept, "ts": datetime.now(timezone.utc).isoformat()}
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    (FEEDS_DIR / f"feed_{lang}.json.gz").write_bytes(gz)

    # Remove any legacy uncompressed feed so the CDN only serves .json.gz.
    legacy = FEEDS_DIR / f"feed_{lang}.json"
    if legacy.exists():
        legacy.unlink()
    return len(kept)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    build_category_index()

    sitemaps = load_sitemaps()
    if not sitemaps:
        print("[error] nothing to do: no sitemaps")
        return

    color_cache = load_color_cache()
    if color_cache:
        print(f"[ok] color cache: {len(color_cache)} image URLs")

    sources: list[tuple[str, str]] = []
    for source in sitemaps:
        sitemap_url = str(source.get("url", "")).strip()
        lang = str(source.get("lang", "")).strip() or "unknown"
        if sitemap_url and sitemap_url.lower() != "nan":
            sources.append((sitemap_url, lang))

    # 1) Fetch all sitemaps in parallel -> flat list of (entry, lang) to scrape.
    tasks: list[tuple[dict, str]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_articles_from_sitemap, url): (url, lang)
                   for url, lang in sources}
        for fut in as_completed(futures):
            url, lang = futures[fut]
            try:
                entries = fut.result()
            except Exception as exc:
                print(f"[warn] sitemap failed {url}: {exc}")
                entries = []
            print(f"[..] {url} -> {len(entries)} recent (lang={lang})")
            tasks.extend((entry, lang) for entry in entries)

    # 2) Scrape all articles in parallel (this is the network-heavy part).
    print(f"[..] scraping {len(tasks)} articles with {MAX_WORKERS} workers")
    by_lang: dict[str, list[dict]] = {}
    done = 0
    scrape_ok = 0
    scrape_fail = 0
    rejected = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(scrape_article, entry, lang) for entry, lang in tasks]
        for fut in as_completed(futures):
            done += 1
            try:
                article = fut.result()
            except Exception:
                article = None
            if article is None:
                scrape_fail += 1
            elif is_acceptable(article):
                scrape_ok += 1
                by_lang.setdefault(article["lang"], []).append(article)
            else:
                scrape_fail += 1  # counted as fail for "got usable article"
                rejected += 1
            if done % 50 == 0 or done == len(tasks):
                print(f"    scraped {done}/{len(tasks)}")

    total_attempted = len(tasks)
    usable = scrape_ok
    failed_or_rejected = scrape_fail
    print(
        f"[stats] scrape attempts={total_attempted} "
        f"usable={usable} "
        f"failed_or_rejected={failed_or_rejected} "
        f"(of which banned/no-title={rejected}) "
        f"success_rate="
        f"{(100.0 * usable / total_attempted) if total_attempted else 0:.1f}%"
    )
    # 3) Dominant colors for new scrapes (reuse cache; extract only unknowns).
    all_new = [a for arts in by_lang.values() for a in arts]
    if all_new:
        color_cache = apply_dominant_colors(all_new, color_cache)

    if not by_lang:
        print("[done] no new acceptable articles this run")
        # Still rewrite existing feeds so relative timestamps stay fresh
        # (and backfill a batch of missing colors).
        for path in FEEDS_DIR.glob("feed_*.json.gz"):
            lang = path.name[len("feed_"):-len(".json.gz")]
            write_feed(lang, [], color_cache)
        return

    total = 0
    for lang, arts in by_lang.items():
        count = write_feed(lang, arts, color_cache)
        total += len(arts)
        print(f"[ok] feed_{lang}.json -> {count} articles ({len(arts)} new this run)")

    print(f"[done] processed {total} new articles across {len(by_lang)} languages")


if __name__ == "__main__":
    main()
