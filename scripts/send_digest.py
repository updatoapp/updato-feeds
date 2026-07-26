#!/usr/bin/env python3
"""Serverless single-story push for GitHub Actions.

For each supported language it:
  1. Downloads the committed GitHub Pages feed (feed_<lang>.json.gz),
  2. Selects the highest-ranked recent article,
  3. Sends its headline, description/image and deep link as an FCM notification
     to the topic ``news_<lang>`` via the
     FCM HTTP v1 API.

No database, no server, no stored user tokens -- FCM topic fan-out handles
delivery. Authentication uses a Firebase service-account key supplied via the
``FCM_SERVICE_ACCOUNT`` env var (raw JSON) or a file path in
``SERVICE_ACCOUNT_FILE``.

Everything is env-overridable so the same script runs locally and in CI:

  SLOT               morning|afternoon|evening|night|auto  (default: auto -> from IST clock)
  LANGS              comma list (default: en,hi,ta,bn,kn,te,ml)
  TOP_N              retained for compatibility; single-story mode always uses 1
  FEED_BASE_URL      default: https://updatoapp.github.io/updato-feeds/feeds
  DRY_RUN            "1" to build + print but NOT send
  SERVICE_ACCOUNT_FILE   path to key json (default: service_account.json)
  FCM_SERVICE_ACCOUNT    raw key json (used if the file is absent)
  R2_PUBLIC_BASE / R2_*  bake headline into image and host on Cloudflare R2
  BAKE_NOTIF_IMAGE       "0" to disable baking (default: on when R2 is configured)
"""

from __future__ import annotations

import gzip
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

# Allow `python scripts/send_digest.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from notif_image import bake_and_upload, cleanup_old_notif_images, r2_configured

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))

LANGS = [c.strip() for c in os.getenv("LANGS", "en,hi,ta,bn,kn,te,ml").split(",") if c.strip()]
TOP_N = 1
FEED_BASE_URL = os.getenv("FEED_BASE_URL", "https://updatoapp.github.io/updato-feeds/feeds").rstrip("/")
DRY_RUN = os.getenv("DRY_RUN", "").strip() in ("1", "true", "yes")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
_BAKE_ENV = os.getenv("BAKE_NOTIF_IMAGE", "").strip().lower()
BAKE_NOTIF_IMAGE = (
    True if _BAKE_ENV in ("1", "true", "yes")
    else False if _BAKE_ENV in ("0", "false", "no")
    else None  # auto: on when R2 is configured
)

FCM_SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
REQUEST_TIMEOUT = 15


def _should_bake() -> bool:
    if BAKE_NOTIF_IMAGE is False:
        return False
    if BAKE_NOTIF_IMAGE is True:
        return True
    return r2_configured() or DRY_RUN


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default


# --- Importance ranking (URL-based; no API key, no NLP on native scripts) --- #
# The trending signal is CROSS-SOURCE CORROBORATION: a story many publishers
# run at once is important. "Same story" is detected from URL slug tokens
# (English/romanized even on Hindi/Tamil sites), which is far more reliable
# than fuzzy-matching native-script headlines. All knobs are env-overridable.
RANKING = os.getenv("RANKING", "").strip().lower() not in ("0", "false", "no")
RANK_WINDOW_HOURS = _env_float("RANK_WINDOW_HOURS", 24.0)
SIM_THRESHOLD = _env_float("SIM_THRESHOLD", 0.5)   # URL-token overlap coefficient
MIN_SHARED = _env_int("MIN_SHARED", 2)             # min shared URL tokens to merge
MIN_SOURCES = _env_int("MIN_SOURCES", 1)           # min publishers per story
W_CORROB = _env_float("W_CORROB", 2.0)
W_SOURCE = _env_float("W_SOURCE", 1.5)
W_CATEGORY = _env_float("W_CATEGORY", 1.0)
W_RECENCY = _env_float("W_RECENCY", 0.8)
W_IMAGE = _env_float("W_IMAGE", 0.3)

# Localized notification titles per time-of-day slot.
SLOT_TITLES: dict[str, dict[str, str]] = {
    "morning": {
        "en": "\u2600\ufe0f Top stories this morning",
        "hi": "\u2600\ufe0f \u0906\u091c \u0938\u0941\u092c\u0939 \u0915\u0940 \u092c\u0921\u093c\u0940 \u0916\u092c\u0930\u0947\u0902",
        "ta": "\u2600\ufe0f \u0b87\u0ba9\u0bcd\u0bb1\u0bc1 \u0b95\u0bbe\u0bb2\u0bc8 \u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf\u0b9a\u0bcd \u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bbf\u0b95\u0bb3\u0bcd",
        "bn": "\u2600\ufe0f \u0986\u099c \u09b8\u0995\u09be\u09b2\u09c7\u09b0 \u09b6\u09c0\u09b0\u09cd\u09b7 \u0996\u09ac\u09b0",
        "kn": "\u2600\ufe0f \u0c87\u0c82\u0ca6\u0cc1 \u0cac\u0cc6\u0cb3\u0c97\u0cbf\u0ca8 \u0caa\u0ccd\u0cb0\u0cae\u0cc1\u0c96 \u0cb8\u0cc1\u0ca6\u0ccd\u0ca6\u0cbf\u0c97\u0cb3\u0cc1",
        "te": "\u2600\ufe0f \u0c08 \u0c09\u0c26\u0c2f\u0c2a\u0c41 \u0c2a\u0c4d\u0c30\u0c27\u0c3e\u0c28 \u0c35\u0c3e\u0c30\u0c4d\u0c24\u0c32\u0c41",
        "ml": "\u2600\ufe0f \u0d07\u0d28\u0d4d\u0d28\u0d4d \u0d30\u0d3e\u0d35\u0d3f\u0d32\u0d46 \u0d2a\u0d4d\u0d30\u0d27\u0d3e\u0d28 \u0d35\u0d3e\u0d7c\u0d24\u0d4d\u0d24\u0d15\u0d33\u0d4d",
    },
    "afternoon": {
        "en": "\U0001f324\ufe0f Top stories this afternoon",
        "hi": "\U0001f324\ufe0f \u0926\u094b\u092a\u0939\u0930 \u0915\u0940 \u092c\u0921\u093c\u0940 \u0916\u092c\u0930\u0947\u0902",
        "ta": "\U0001f324\ufe0f \u0b87\u0ba9\u0bcd\u0bb1\u0bc1 \u0bae\u0ba4\u0bbf\u0baf \u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf\u0b9a\u0bcd \u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bbf\u0b95\u0bb3\u0bcd",
        "bn": "\U0001f324\ufe0f \u0986\u099c \u09a6\u09c1\u09aa\u09c1\u09b0\u09c7\u09b0 \u09b6\u09c0\u09b0\u09cd\u09b7 \u0996\u09ac\u09b0",
        "kn": "\U0001f324\ufe0f \u0c87\u0c82\u0ca6\u0cc1 \u0cae\u0ceb\u0ca7\u0ccd\u0caf\u0cbe\u0cb9\u0ccd\u0ca8\u0ca6 \u0caa\u0ccd\u0cb0\u0cae\u0cc1\u0c96 \u0cb8\u0cc1\u0ca6\u0ccd\u0ca6\u0cbf\u0c97\u0cb3\u0cc1",
        "te": "\U0001f324\ufe0f \u0c08 \u0c2e\u0c27\u0c4d\u0c2f\u0c3e\u0c39\u0c4d\u0c28\u0c2a\u0c41 \u0c2a\u0c4d\u0c30\u0c27\u0c3e\u0c28 \u0c35\u0c3e\u0c30\u0c4d\u0c24\u0c32\u0c41",
        "ml": "\U0001f324\ufe0f \u0d07\u0d28\u0d4d\u0d28\u0d4d \u0d09\u0d1a\u0d4d\u0d1a\u0d15\u0d4d\u0d15\u0d41\u0d33\u0d4d\u0d33 \u0d2a\u0d4d\u0d30\u0d27\u0d3e\u0d28 \u0d35\u0d3e\u0d7c\u0d24\u0d4d\u0d24\u0d15\u0d33\u0d4d",
    },
    "evening": {
        "en": "\U0001f307 Top stories this evening",
        "hi": "\U0001f307 \u0936\u093e\u092e \u0915\u0940 \u092c\u0921\u093c\u0940 \u0916\u092c\u0930\u0947\u0902",
        "ta": "\U0001f307 \u0b87\u0ba9\u0bcd\u0bb1\u0bc1 \u0bae\u0bbe\u0bb2\u0bc8 \u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf\u0b9a\u0bcd \u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bbf\u0b95\u0bb3\u0bcd",
        "bn": "\U0001f307 \u0986\u099c \u09b8\u09a8\u09cd\u09a7\u09cd\u09af\u09be\u09b0 \u09b6\u09c0\u09b0\u09cd\u09b7 \u0996\u09ac\u09b0",
        "kn": "\U0001f307 \u0c87\u0c82\u0ca6\u0cc1 \u0cb8\u0c82\u0c9c\u0cc6\u0caf \u0caa\u0ccd\u0cb0\u0cae\u0cc1\u0c96 \u0cb8\u0cc1\u0ca6\u0ccd\u0ca6\u0cbf\u0c97\u0cb3\u0cc1",
        "te": "\U0001f307 \u0c08 \u0c38\u0c3e\u0c2f\u0c02\u0c24\u0c4d\u0c30\u0c2a\u0c41 \u0c2a\u0c4d\u0c30\u0c27\u0c3e\u0c28 \u0c35\u0c3e\u0c30\u0c4d\u0c24\u0c32\u0c41",
        "ml": "\U0001f307 \u0d07\u0d28\u0d4d\u0d28\u0d4d \u0d35\u0d48\u0d15\u0d41\u0d28\u0d4d\u0d28\u0d47\u0d30\u0d24\u0d4d\u0d24\u0d46 \u0d2a\u0d4d\u0d30\u0d27\u0d3e\u0d28 \u0d35\u0d3e\u0d7c\u0d24\u0d4d\u0d24\u0d15\u0d33\u0d4d",
    },
    "night": {
        "en": "\U0001f319 Today's top stories",
        "hi": "\U0001f319 \u0906\u091c \u0915\u0940 \u092c\u0921\u093c\u0940 \u0916\u092c\u0930\u0947\u0902",
        "ta": "\U0001f319 \u0b87\u0ba9\u0bcd\u0bb1\u0bc8\u0baf \u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf\u0b9a\u0bcd \u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bbf\u0b95\u0bb3\u0bcd",
        "bn": "\U0001f319 \u0986\u099c\u0995\u09c7\u09b0 \u09b6\u09c0\u09b0\u09cd\u09b7 \u0996\u09ac\u09b0",
        "kn": "\U0001f319 \u0c87\u0c82\u0ca6\u0cbf\u0ca8 \u0caa\u0ccd\u0cb0\u0cae\u0cc1\u0c96 \u0cb8\u0cc1\u0ca6\u0ccd\u0ca6\u0cbf\u0c97\u0cb3\u0cc1",
        "te": "\U0001f319 \u0c08 \u0c30\u0c4b\u0c1c\u0c41 \u0c2a\u0c4d\u0c30\u0c27\u0c3e\u0c28 \u0c35\u0c3e\u0c30\u0c4d\u0c24\u0c32\u0c41",
        "ml": "\U0001f319 \u0d07\u0d28\u0d4d\u0d28\u0d24\u0d4d\u0d24\u0d46 \u0d2a\u0d4d\u0d30\u0d27\u0d3e\u0d28 \u0d35\u0d3e\u0d7c\u0d24\u0d4d\u0d24\u0d15\u0d33\u0d4d",
    },
}


def resolve_slot() -> str:
    slot = os.getenv("SLOT", "auto").strip().lower()
    if slot in SLOT_TITLES:
        return slot
    hour = datetime.now(IST).hour
    if 5 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 16:
        return "afternoon"
    if 17 <= hour <= 20:
        return "evening"
    return "night"


def load_credentials() -> service_account.Credentials:
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=FCM_SCOPES
        )
    raw = os.getenv("FCM_SERVICE_ACCOUNT", "").strip()
    if raw:
        return service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=FCM_SCOPES
        )
    raise SystemExit(
        "[error] No credentials: set SERVICE_ACCOUNT_FILE to a key file or "
        "provide FCM_SERVICE_ACCOUNT with the raw service-account JSON."
    )


def fetch_feed(lang: str) -> list[dict]:
    url = f"{FEED_BASE_URL}/feed_{lang}.json.gz"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    try:
        raw = gzip.decompress(resp.content)
    except OSError:
        raw = resp.content  # already decompressed by the client
    data = json.loads(raw.decode("utf-8"))
    return data.get("feed", []) if isinstance(data, dict) else data


def build_body(articles: list[dict]) -> str:
    lines = []
    for art in articles:
        title = (art.get("title") or "").strip()
        if title:
            lines.append(f"\u2022 {title}")
    return "\n".join(lines)


def first_image(articles: list[dict]) -> str | None:
    for art in articles:
        img = (art.get("image_url") or art.get("image") or "").strip()
        if img.startswith("http"):
            return img
    return None


# --------------------------------------------------------------------------- #
# Importance ranking
# --------------------------------------------------------------------------- #
# Language subdomains collapsed so bangla.aajtak.in and aajtak.in count as one
# publisher (for corroboration) and share a reputation weight.
LANG_SUBDOMAINS = {
    "hindi", "english", "bangla", "bengali", "tamil", "telugu", "kannada",
    "malayalam", "hin", "tam", "tel", "kan", "ben", "mal", "eng", "mr", "guj",
}

DEFAULT_SOURCE_WEIGHT = 0.6
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
    "national": 1.0, "nation": 1.0, "india": 1.0, "politics": 1.0,
    "world": 1.0, "international": 1.0, "defence": 0.9, "defense": 0.9,
    "business": 0.9, "economy": 0.9, "finance": 0.9, "market": 0.9, "markets": 0.9,
    "science": 0.85, "health": 0.85,
    "technology": 0.75, "tech": 0.75, "education": 0.75,
    "sports": 0.6, "sport": 0.6, "cricket": 0.6,
    "auto": 0.5,
    "entertainment": 0.4, "bollywood": 0.4, "movies": 0.4,
    "lifestyle": 0.3, "viral": 0.3, "trending": 0.3,
    "astrology": 0.15, "horoscope": 0.15,
    "webstories": 0.1, "photos": 0.15, "gallery": 0.15, "videos": 0.2,
}
DEFAULT_CATEGORY_WEIGHT = 0.55  # untagged / place-only (local) news

# Structural / section words that must not drive story matching. Distinctive
# entity tokens (names, places, events) do the clustering instead.
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

# Same defaults as fetch_feeds — daily राशिफल is multi-publisher noise.
EXCLUDED_CATEGORIES = {
    p.strip().lower()
    for p in os.getenv(
        "EXCLUDED_CATEGORIES",
        "astrology,horoscope,rashifal,panchang,jyotish",
    ).split(",")
    if p.strip()
}
EXCLUDED_TITLE_KEYWORDS = {
    p.strip().lower()
    for p in os.getenv(
        "EXCLUDED_TITLE_KEYWORDS",
        "राशिफल,पंचांग,कुंडली,ज्योतिष,rashifal,horoscope,panchang,astrology,"
        "aaj ka rashifal,aaj ka panchang",
    ).split(",")
    if p.strip()
}


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
    return DEFAULT_SOURCE_WEIGHT


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
    if EXCLUDED_CATEGORIES:
        cats = article.get("categories") or []
        if isinstance(cats, str):
            cats = [cats]
        for c in cats:
            cl = str(c).lower().strip()
            if cl in EXCLUDED_CATEGORIES or any(x in cl for x in EXCLUDED_CATEGORIES):
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
    weights = [CATEGORY_WEIGHTS[c.lower()] for c in cats if str(c).lower() in CATEGORY_WEIGHTS]
    return max(weights) if weights else DEFAULT_CATEGORY_WEIGHT


def _age_hours(article: dict) -> float:
    ts = str(article.get("raw_time", "")).strip()
    if not ts:
        return 1e9
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
    except Exception:
        return 1e9
    return (datetime.now(IST) - dt).total_seconds() / 3600.0


def _recency_decay(age_hours: float) -> float:
    if RANK_WINDOW_HOURS <= 0:
        return 0.0
    return max(0.0, 1.0 - age_hours / RANK_WINDOW_HOURS)


def _best_rep(arts: list[dict]) -> dict:
    """Pick the nicest article in a cluster: prefer image, reputable source, title."""
    return max(arts, key=lambda a: (
        1 if (a.get("image_url") or "").startswith("http") else 0,
        _source_weight(a.get("url", "")),
        min(len(a.get("title") or ""), 90),
    ))


def select_articles(all_articles: list[dict], n: int) -> list[dict]:
    """Return the top-N articles to feature, ranked by importance (or recency)."""
    base = [
        a for a in all_articles
        if (a.get("title") or "").strip() and not _is_webstory(a) and not _is_excluded(a)
    ]
    if not base:  # nothing but web stories / excluded -> fall back so we never send empty
        base = [a for a in all_articles if (a.get("title") or "").strip() and not _is_excluded(a)]
    if not base:
        base = [a for a in all_articles if (a.get("title") or "").strip()]

    if not RANKING:
        return base[:n]  # feed is already newest-first

    # Restrict to the recent window, but never starve the digest.
    windowed = [a for a in base if _age_hours(a) <= RANK_WINDOW_HOURS]
    pool = windowed if len(windowed) >= n else base

    for a in pool:
        a["_tokens"] = _url_tokens(a.get("url", ""))
        a["_domain"] = _publisher(a.get("url", ""))

    # Greedy single-pass clustering on URL-token overlap (pool is newest-first).
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
        else:
            clusters.append({"tokens": toks, "arts": [art]})

    scored: list[tuple[float, int, dict]] = []
    for c in clusters:
        sources = {a["_domain"] for a in c["arts"] if a["_domain"]}
        rep = dict(_best_rep(c["arts"]))
        newest_age = min(_age_hours(a) for a in c["arts"])
        score = (
            W_CORROB * len(sources)
            + W_SOURCE * max(_source_weight(a.get("url", "")) for a in c["arts"])
            + W_CATEGORY * _category_weight(rep)
            + W_RECENCY * _recency_decay(newest_age)
            + W_IMAGE * (1.0 if (rep.get("image_url") or "").startswith("http") else 0.0)
        )
        rep["_sources"] = len(sources)
        rep["_score"] = round(score, 2)
        scored.append((score, len(sources), rep))

    eligible = [t for t in scored if t[1] >= MIN_SOURCES]
    eligible.sort(key=lambda t: t[0], reverse=True)
    picked = [t[2] for t in eligible[:n]]

    # Fallback fill (keeps small-language feeds full) by recency.
    if len(picked) < n:
        seen = {a.get("url") for a in picked}
        for a in pool:
            if a.get("url") in seen:
                continue
            picked.append(a)
            seen.add(a.get("url"))
            if len(picked) >= n:
                break
    return picked[:n]


def send_to_topic(access_token: str, project_id: str, topic: str,
                  title: str, body: str, image_url: str | None,
                  deep_link: str | None) -> tuple[int, str]:
    notification = {"title": title, "body": body}
    if image_url:
        notification["image"] = image_url

    message: dict = {
        "topic": topic,
        "notification": notification,
        "data": {
            "type": "single_article",
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
            "deep_link": deep_link or "explore",
            "article_url": deep_link or "",
        },
        "android": {
            "priority": "high",
            "notification": {"click_action": "FLUTTER_NOTIFICATION_CLICK"},
        },
        "apns": {
            "payload": {"aps": {"mutable-content": 1, "alert": {"title": title, "body": body}}},
        },
    }
    if image_url:
        message["apns"]["fcm_options"] = {"image": image_url}

    resp = requests.post(
        f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; UTF-8",
        },
        data=json.dumps({"message": message}),
        timeout=REQUEST_TIMEOUT,
    )
    return resp.status_code, resp.text


def main() -> None:
    slot = resolve_slot()
    bake = _should_bake()
    print(
        f"[..] slot={slot} langs={LANGS} top_n={TOP_N} dry_run={DRY_RUN} "
        f"bake_image={bake} r2={r2_configured()}"
    )

    access_token = None
    project_id = None
    if not DRY_RUN:
        credentials = load_credentials()
        credentials.refresh(Request())
        project_id = credentials.project_id
        access_token = credentials.token
        print(f"[ok] authenticated project={project_id}")
    else:
        print("[..] dry-run: skipping FCM auth")

    mode = "importance" if RANKING else "recency"
    print(f"[..] ranking={mode} window={RANK_WINDOW_HOURS}h min_sources={MIN_SOURCES}")

    sent = 0
    for lang in LANGS:
        slot_title = SLOT_TITLES[slot].get(lang) or SLOT_TITLES[slot]["en"]
        try:
            feed = fetch_feed(lang)
        except Exception as exc:
            print(f"[warn] {lang}: could not fetch feed: {exc}")
            continue

        if not feed:
            print(f"[warn] {lang}: feed empty, skipping")
            continue

        articles = select_articles(feed, 1)
        article = articles[0]
        title = (article.get("title") or "").strip()
        description = re.sub(r"\s+", " ", (article.get("description") or "").strip())
        body = description[:220].rstrip() if description else slot_title
        raw_image = (article.get("image_url") or "").strip() or None
        deep_link = (article.get("url") or "").strip() or "explore"
        topic = f"news_{lang}"

        image_url = raw_image
        if bake and raw_image:
            image_url = bake_and_upload(
                raw_image,
                title,
                lang,
                slot,
                dry_run=DRY_RUN,
                preview_dir=Path("notif_previews"),
            )

        if DRY_RUN:
            print(f"\n----- DRY RUN [{topic}] slot={slot} -----\n{title}")
            src, sc = article.get("_sources"), article.get("_score")
            tag = f"  [sources={src}, score={sc}]" if src is not None else ""
            print(f"  \u2022 {title}{tag}")
            print(
                f"body={body}\nraw_image={raw_image}\n"
                f"image={image_url}\ndeep_link={deep_link}\n"
            )
            continue

        status, text = send_to_topic(
            access_token, project_id, topic, title, body, image_url, deep_link
        )
        if status == 200:
            sent += 1
            print(f"[ok] {topic}: sent single story -> {deep_link}")
        else:
            print(f"[error] {topic}: HTTP {status} -> {text}")

    if bake and not DRY_RUN:
        cleanup_old_notif_images()

    print(f"[done] slot={slot} sent={sent}/{len(LANGS)}")
    # Fail the CI job if nothing went out (so a broken key/feed is visible).
    if not DRY_RUN and sent == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
