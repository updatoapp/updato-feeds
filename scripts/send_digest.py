#!/usr/bin/env python3
"""Serverless "Top stories" digest push for GitHub Actions.

For each supported language it:
  1. Downloads the committed GitHub Pages feed (feed_<lang>.json.gz),
  2. Takes the top N newest headlines (the feed is already sorted newest-first),
  3. Builds a localized, time-of-day digest (morning / afternoon / evening / night),
  4. Sends it as an FCM notification to the topic ``news_<lang>`` via the
     FCM HTTP v1 API.

No database, no server, no stored user tokens -- FCM topic fan-out handles
delivery. Authentication uses a Firebase service-account key supplied via the
``FCM_SERVICE_ACCOUNT`` env var (raw JSON) or a file path in
``SERVICE_ACCOUNT_FILE``.

Everything is env-overridable so the same script runs locally and in CI:

  SLOT               morning|afternoon|evening|night|auto  (default: auto -> from IST clock)
  LANGS              comma list (default: en,hi,ta,bn,kn,te,ml)
  TOP_N              headlines per digest (default: 5)
  FEED_BASE_URL      default: https://updatoapp.github.io/updato-feeds/feeds
  DRY_RUN            "1" to build + print but NOT send
  SERVICE_ACCOUNT_FILE   path to key json (default: service_account.json)
  FCM_SERVICE_ACCOUNT    raw key json (used if the file is absent)
"""

from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))

LANGS = [c.strip() for c in os.getenv("LANGS", "en,hi,ta,bn,kn,te,ml").split(",") if c.strip()]
TOP_N = int(os.getenv("TOP_N", "5"))
FEED_BASE_URL = os.getenv("FEED_BASE_URL", "https://updatoapp.github.io/updato-feeds/feeds").rstrip("/")
DRY_RUN = os.getenv("DRY_RUN", "").strip() in ("1", "true", "yes")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

FCM_SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
REQUEST_TIMEOUT = 15

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


def fetch_top_articles(lang: str, n: int) -> list[dict]:
    url = f"{FEED_BASE_URL}/feed_{lang}.json.gz"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    try:
        raw = gzip.decompress(resp.content)
    except OSError:
        raw = resp.content  # already decompressed by the client
    data = json.loads(raw.decode("utf-8"))
    articles = data.get("feed", []) if isinstance(data, dict) else data
    return articles[:n]


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
            "type": "digest",
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
            "deep_link": deep_link or "explore",
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
    print(f"[..] slot={slot} langs={LANGS} top_n={TOP_N} dry_run={DRY_RUN}")

    credentials = load_credentials()
    credentials.refresh(Request())
    project_id = credentials.project_id
    access_token = credentials.token
    print(f"[ok] authenticated project={project_id}")

    sent = 0
    for lang in LANGS:
        title = SLOT_TITLES[slot].get(lang) or SLOT_TITLES[slot]["en"]
        try:
            articles = fetch_top_articles(lang, TOP_N)
        except Exception as exc:
            print(f"[warn] {lang}: could not fetch feed: {exc}")
            continue

        if not articles:
            print(f"[warn] {lang}: feed empty, skipping")
            continue

        body = build_body(articles)
        image_url = first_image(articles)
        deep_link = (articles[0].get("url") or "").strip() or "explore"
        topic = f"news_{lang}"

        if DRY_RUN:
            print(f"\n----- DRY RUN [{topic}] -----\n{title}\n{body}\nimage={image_url}\n")
            continue

        status, text = send_to_topic(
            access_token, project_id, topic, title, body, image_url, deep_link
        )
        if status == 200:
            sent += 1
            print(f"[ok] {topic}: sent ({len(articles)} headlines)")
        else:
            print(f"[error] {topic}: HTTP {status} -> {text}")

    print(f"[done] slot={slot} sent={sent}/{len(LANGS)}")
    # Fail the CI job if nothing went out (so a broken key/feed is visible).
    if not DRY_RUN and sent == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
