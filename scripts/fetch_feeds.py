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
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

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
MAX_PER_FEED = int(os.getenv("MAX_PER_FEED", "500"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
# Parallelism for network-bound sitemap + article fetching.
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))

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
# Static image defaults (per-image downloading/analysis was removed for speed)
# --------------------------------------------------------------------------- #
DEFAULT_DOMINANT_COLOR = "#444444"
DEFAULT_TEXT_COLOR = "#FFFFFF"  # readable on the dark default background


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
        root = _parse_xml(res.content)
    except Exception as exc:
        print(f"[warn] failed to parse sitemap {sitemap_url}: {exc}")
        return []

    articles: list[dict] = []
    for url_tag in root.findall("ns:url", SITEMAP_NS):
        if len(articles) >= MAX_PER_SITEMAP:
            break

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
            if (now - pub_time) > timedelta(minutes=RECENT_MINUTES):
                continue
        except Exception:
            continue

        title_tag = url_tag.find("news:news/news:title", SITEMAP_NS)
        image_tag = url_tag.find("image:image/image:loc", SITEMAP_NS)
        articles.append({
            "url": loc_tag.text.strip(),
            "title": title_tag.text.strip() if title_tag is not None else "",
            "image": image_tag.text.strip() if image_tag is not None else "",
            "published_time": pub_time,
        })
    return articles


def scrape_article(entry: dict, lang: str) -> dict | None:
    url = entry["url"]
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        if res.status_code != 200:
            print(f"[warn] HTTP {res.status_code} for {url}")
            return None
        soup = BeautifulSoup(res.text, "html.parser")

        def og(prop: str) -> str | None:
            tag = soup.find("meta", property=prop)
            return tag["content"].strip() if tag and tag.get("content") else None

        title = og("og:title") or entry.get("title") or ""
        description = og("og:description") or ""
        image_url = og("og:image") or entry.get("image") or None

        pub_time = entry["published_time"]
        raw_time = (pub_time.astimezone(IST) if pub_time.tzinfo else pub_time.replace(tzinfo=IST)).isoformat()

        return {
            "url": url,
            "title": title,
            "description": description,
            "image_url": image_url,
            "dominant_color": DEFAULT_DOMINANT_COLOR,
            "text_color": DEFAULT_TEXT_COLOR,
            "is_vertical": False,
            "lang": lang,
            "categories": extract_categories_from_url(url),
            "raw_time": raw_time,
        }
    except Exception as exc:
        print(f"[warn] scrape failed for {url}: {exc}")
        return None


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


def write_feed(lang: str, articles: list[dict]) -> int:
    cutoff = datetime.now(IST) - timedelta(days=RETENTION_DAYS)

    merged: dict[str, dict] = {}
    for art in load_existing_feed(lang) + articles:
        url = art.get("url")
        if not url:
            continue
        merged[url] = {**merged.get(url, {}), **art}  # newer scrape wins

    kept = [a for a in merged.values() if _parse_ist(a.get("raw_time", "")) >= cutoff]
    kept.sort(key=lambda a: _parse_ist(a.get("raw_time", "")), reverse=True)
    kept = kept[:MAX_PER_FEED]

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
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(scrape_article, entry, lang) for entry, lang in tasks]
        for fut in as_completed(futures):
            done += 1
            try:
                article = fut.result()
            except Exception:
                article = None
            if article and is_acceptable(article):
                by_lang.setdefault(article["lang"], []).append(article)
            if done % 50 == 0 or done == len(tasks):
                print(f"    scraped {done}/{len(tasks)}")

    if not by_lang:
        print("[done] no new acceptable articles this run")
        # Still rewrite existing feeds so relative timestamps stay fresh.
        for path in FEEDS_DIR.glob("feed_*.json.gz"):
            lang = path.name[len("feed_"):-len(".json.gz")]
            write_feed(lang, [])
        return

    total = 0
    for lang, arts in by_lang.items():
        count = write_feed(lang, arts)
        total += len(arts)
        print(f"[ok] feed_{lang}.json -> {count} articles ({len(arts)} new this run)")

    print(f"[done] processed {total} new articles across {len(by_lang)} languages")


if __name__ == "__main__":
    main()
