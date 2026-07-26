#!/usr/bin/env python3
"""Bake Inshorts-style notification images and upload them to Cloudflare R2.

Produces a 16:9 JPEG with a dark bottom gradient and white headline so FCM's
standard big-picture notification shows text *inside* the image.
"""

from __future__ import annotations

import io
import os
import re
import time
from pathlib import Path

import requests

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
NOTIF_WIDTH = int(os.getenv("NOTIF_IMG_WIDTH", "1200"))
NOTIF_HEIGHT = int(os.getenv("NOTIF_IMG_HEIGHT", "675"))
NOTIF_JPEG_QUALITY = int(os.getenv("NOTIF_JPEG_QUALITY", "85"))
NOTIF_PREFIX = os.getenv("NOTIF_R2_PREFIX", "notif").strip().strip("/") or "notif"
NOTIF_CLEANUP_DAYS = float(os.getenv("NOTIF_CLEANUP_DAYS", "7"))
FONT_CACHE_DIR = Path(os.getenv("NOTIF_FONT_DIR", "/tmp/updato_notif_fonts"))
REQUEST_TIMEOUT = 20

# Noto Bold TTFs (jsDelivr → notofonts). One family per script we ship digests in.
_FONT_URLS: dict[str, str] = {
    "en": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSans/NotoSans-Bold.ttf",
    "hi": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansDevanagari/NotoSansDevanagari-Bold.ttf",
    "ta": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansTamil/NotoSansTamil-Bold.ttf",
    "bn": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansBengali/NotoSansBengali-Bold.ttf",
    "kn": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansKannada/NotoSansKannada-Bold.ttf",
    "te": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansTelugu/NotoSansTelugu-Bold.ttf",
    "ml": "https://cdn.jsdelivr.net/gh/notofonts/noto-fonts@main/hinted/ttf/NotoSansMalayalam/NotoSansMalayalam-Bold.ttf",
}

_font_path_cache: dict[str, Path] = {}


def r2_configured() -> bool:
    return bool(
        os.getenv("R2_ACCESS_KEY_ID", "").strip()
        and os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
        and os.getenv("CF_ACCOUNT_ID", "").strip()
        and os.getenv("R2_PUBLIC_BASE", "").strip()
    )


def _public_base() -> str:
    return os.getenv("R2_PUBLIC_BASE", "").rstrip("/")


def _bucket() -> str:
    return os.getenv("R2_BUCKET", "").strip() or "updato-feeds"


def _ensure_font(lang: str) -> Path | None:
    """Download (once) the Noto Bold TTF for ``lang``; fall back to English."""
    key = lang if lang in _FONT_URLS else "en"
    if key in _font_path_cache and _font_path_cache[key].is_file():
        return _font_path_cache[key]

    url = _FONT_URLS[key]
    FONT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dest = FONT_CACHE_DIR / f"{key}.ttf"
    if not dest.is_file() or dest.stat().st_size < 10_000:
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
        except Exception as exc:
            print(f"[warn] font download failed for {key}: {exc}")
            if key != "en":
                return _ensure_font("en")
            return None

    _font_path_cache[key] = dest
    return dest


def _cover_crop(img, width: int, height: int):
    """Scale+center-crop like CSS object-fit: cover."""
    from PIL import Image

    src_w, src_h = img.size
    if src_w <= 0 or src_h <= 0:
        return Image.new("RGB", (width, height), (20, 20, 20))

    scale = max(width / src_w, height / src_h)
    new_w = max(1, int(round(src_w * scale)))
    new_h = max(1, int(round(src_h * scale)))
    resized = img.resize((new_w, new_h), Image.LANCZOS)
    left = max(0, (new_w - width) // 2)
    top = max(0, (new_h - height) // 2)
    return resized.crop((left, top, left + width, top + height))


def _draw_bottom_gradient(base, start_ratio: float = 0.48) -> None:
    """Paint a transparent→black vertical gradient over the bottom of ``base``."""
    from PIL import Image

    w, h = base.size
    start_y = int(h * start_ratio)
    span = max(1, h - start_y)
    # Build a 1×span alpha ramp, then stretch — much faster than per-pixel loops.
    ramp = Image.new("L", (1, span))
    ramp_px = ramp.load()
    for y in range(span):
        t = y / span
        ramp_px[0, y] = int(min(230, (t ** 1.35) * 240))
    alpha = Image.new("L", (w, h), 0)
    alpha.paste(ramp.resize((w, span), Image.BILINEAR), (0, start_y))
    black = Image.new("RGBA", (w, h), (0, 0, 0, 255))
    black.putalpha(alpha)
    composed = Image.alpha_composite(base.convert("RGBA"), black)
    base.paste(composed.convert("RGB"))


def _text_width(draw, text: str, font) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _ellipsize(draw, text: str, font, max_width: int) -> str:
    if _text_width(draw, text, font) <= max_width:
        return text
    ell = "…"
    while text:
        candidate = text.rstrip() + ell
        if _text_width(draw, candidate, font) <= max_width:
            return candidate
        text = text[:-1]
    return ell


def _wrap_lines(text: str, font, draw, max_width: int, max_lines: int = 3) -> list[str]:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return []

    words = text.split(" ")
    lines: list[str] = []
    current = ""
    i = 0
    while i < len(words):
        word = words[i]
        trial = word if not current else f"{current} {word}"
        if _text_width(draw, trial, font) <= max_width:
            current = trial
            i += 1
            continue
        if current:
            lines.append(current)
            current = ""
            if len(lines) >= max_lines:
                break
            continue
        lines.append(_ellipsize(draw, word, font, max_width))
        i += 1
        if len(lines) >= max_lines:
            break

    if current and len(lines) < max_lines:
        lines.append(current)
        i = len(words)

    if i < len(words) and lines:
        lines[-1] = _ellipsize(draw, lines[-1].rstrip("…"), font, max_width)

    return lines[:max_lines]


def _pick_font_size(lang: str, path: Path, draw, text: str, max_width: int):
    from PIL import ImageFont

    # Indic scripts need a touch more room per glyph.
    sizes = [52, 48, 44, 40, 36, 32] if lang == "en" else [48, 44, 40, 36, 32, 28]
    for size in sizes:
        font = ImageFont.truetype(str(path), size=size)
        lines = _wrap_lines(text, font, draw, max_width, max_lines=3)
        if len(lines) <= 3:
            # Prefer sizes that fit in ≤3 lines without ellipsis when possible.
            if not (lines and lines[-1].endswith("…")) or size <= 36:
                return font, lines
    font = ImageFont.truetype(str(path), size=sizes[-1])
    return font, _wrap_lines(text, font, draw, max_width, max_lines=3)


def bake_notification_image(
    image_url: str,
    title: str,
    lang: str = "en",
) -> bytes | None:
    """Download ``image_url``, overlay gradient + headline, return JPEG bytes."""
    from PIL import Image, ImageDraw, ImageFont

    if not image_url or not image_url.startswith("http"):
        return None

    try:
        resp = requests.get(
            image_url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": "UpdatoDigest/1.0"},
        )
        resp.raise_for_status()
        src = Image.open(io.BytesIO(resp.content)).convert("RGB")
    except Exception as exc:
        print(f"[warn] notif image download failed: {exc}")
        return None

    canvas = _cover_crop(src, NOTIF_WIDTH, NOTIF_HEIGHT)
    _draw_bottom_gradient(canvas)

    draw = ImageDraw.Draw(canvas)
    margin_x = 48
    margin_bottom = 44
    max_width = NOTIF_WIDTH - margin_x * 2

    font_path = _ensure_font(lang)
    if font_path is None:
        font = ImageFont.load_default()
        lines = _wrap_lines(title, font, draw, max_width, max_lines=3)
    else:
        font, lines = _pick_font_size(lang, font_path, draw, title, max_width)

    # Stack lines upward from the bottom margin.
    line_heights = []
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        line_heights.append(bbox[3] - bbox[1])
    gap = max(8, int((line_heights[0] if line_heights else 40) * 0.22))
    block_h = sum(line_heights) + gap * max(0, len(lines) - 1)
    y = NOTIF_HEIGHT - margin_bottom - block_h

    for line, lh in zip(lines, line_heights):
        # Soft shadow for legibility on bright gradient edges.
        draw.text((margin_x + 1, y + 2), line, font=font, fill=(0, 0, 0, 160))
        draw.text((margin_x, y), line, font=font, fill=(255, 255, 255))
        y += lh + gap

    out = io.BytesIO()
    canvas.save(out, format="JPEG", quality=NOTIF_JPEG_QUALITY, optimize=True)
    return out.getvalue()


def upload_notif_jpeg(data: bytes, lang: str, slot: str) -> str | None:
    """Upload JPEG bytes to R2 under ``notif/`` and return the public URL."""
    if not r2_configured():
        print("[warn] R2 not configured — skipping notif image upload")
        return None

    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("[warn] boto3 missing — pip install boto3")
        return None

    account = os.getenv("CF_ACCOUNT_ID", "").strip()
    key_id = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    bucket = _bucket()
    stamp = time.strftime("%Y%m%d_%H%M%S")
    key = f"{NOTIF_PREFIX}/{lang}_{slot}_{stamp}.jpg"

    try:
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            region_name="auto",
            config=Config(signature_version="s3v4"),
        )
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="image/jpeg",
            CacheControl="public, max-age=86400",
        )
    except Exception as exc:
        print(f"[warn] R2 upload failed: {exc}")
        return None

    return f"{_public_base()}/{key}"


def bake_and_upload(
    image_url: str | None,
    title: str,
    lang: str,
    slot: str,
    *,
    dry_run: bool = False,
    preview_dir: Path | None = None,
) -> str | None:
    """Bake + upload (or write a local preview in dry-run). Returns image URL."""
    if not image_url:
        return None

    jpeg = bake_notification_image(image_url, title, lang)
    if not jpeg:
        # Fall back to the original article image if bake fails.
        return image_url if image_url.startswith("http") else None

    if dry_run:
        dest_dir = preview_dir or Path("notif_previews")
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / f"{lang}_{slot}.jpg"
        path.write_bytes(jpeg)
        print(f"[dry] baked preview -> {path} ({len(jpeg)} bytes)")
        # Still upload in dry-run only if explicitly asked.
        if os.getenv("NOTIF_DRY_UPLOAD", "").strip() in ("1", "true", "yes"):
            url = upload_notif_jpeg(jpeg, lang, slot)
            print(f"[dry] uploaded preview url={url}")
            return url
        return f"file://{path.resolve()}"

    url = upload_notif_jpeg(jpeg, lang, slot)
    if url:
        print(f"[ok] baked notif image -> {url}")
        return url
    # Upload failed — keep original so the notification still has a picture.
    return image_url


def cleanup_old_notif_images() -> None:
    """Delete R2 objects under ``notif/`` older than NOTIF_CLEANUP_DAYS."""
    if NOTIF_CLEANUP_DAYS <= 0 or not r2_configured():
        return
    try:
        import boto3
        from botocore.config import Config
        from datetime import datetime, timezone, timedelta
    except ImportError:
        return

    account = os.getenv("CF_ACCOUNT_ID", "").strip()
    client = boto3.client(
        "s3",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID", "").strip(),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY", "").strip(),
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )
    cutoff = datetime.now(timezone.utc) - timedelta(days=NOTIF_CLEANUP_DAYS)
    bucket = _bucket()
    deleted = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{NOTIF_PREFIX}/"):
            for obj in page.get("Contents", []):
                last_mod = obj.get("LastModified")
                if last_mod and last_mod < cutoff:
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    deleted += 1
    except Exception as exc:
        print(f"[warn] notif cleanup failed: {exc}")
        return
    if deleted:
        print(f"[ok] cleaned {deleted} old notif image(s) older than {NOTIF_CLEANUP_DAYS}d")
