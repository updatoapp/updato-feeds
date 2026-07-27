#!/usr/bin/env python3
"""Bake Inshorts-style notification images and upload them to Cloudflare R2.

Produces a JPEG with a heavy bottom scrim and white headline so FCM's
standard big-picture notification shows text *inside* the image.

Aspect is ~2:1 — Android's BigPicture view crops taller (1:1) images and
clips the bottom text. Inshorts can go taller because it uses a custom
notification layout; with stock FCM image we must fit the shade viewport.
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
# 2:1 fits Android BigPicture without bottom crop (1:1 was getting clipped).
NOTIF_WIDTH = int(os.getenv("NOTIF_IMG_WIDTH", "1200"))
NOTIF_HEIGHT = int(os.getenv("NOTIF_IMG_HEIGHT", "600"))
NOTIF_JPEG_QUALITY = int(os.getenv("NOTIF_JPEG_QUALITY", "85"))
# Keep text above this fraction of the canvas — survives minor OEM crop.
NOTIF_SAFE_BOTTOM = float(os.getenv("NOTIF_SAFE_BOTTOM", "0.10"))
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


def _clean_headline(title: str) -> str:
    """Drop the kicker before the first colon so the overlay stays short.

    ``Indian Idol 16 Winner: ओडिशा की बेटी…`` → ``ओडिशा की बेटी…``
    Skips pure time prefixes like ``3:45 PM kickoff``.
    """
    text = re.sub(r"\s+", " ", (title or "").strip())
    if not text:
        return text
    for sep in (":", "\uff1a"):  # ASCII + fullwidth colon
        if sep not in text:
            continue
        before, after = text.split(sep, 1)
        before, after = before.strip(), after.strip()
        if not after:
            continue
        # Don't treat clock times as kickers.
        if re.fullmatch(r"\d{1,2}", before):
            continue
        return after
    return text


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


def _draw_bottom_gradient(base, start_ratio: float = 0.38) -> None:
    """Heavy transparent→black scrim over the lower half (Inshorts-like)."""
    from PIL import Image

    w, h = base.size
    start_y = int(h * start_ratio)
    span = max(1, h - start_y)
    ramp = Image.new("L", (1, span))
    ramp_px = ramp.load()
    for y in range(span):
        t = y / span
        # Smooth ease-in, then lock near-full black across the text band.
        if t < 0.45:
            alpha = int((t / 0.45) ** 1.1 * 160)
        else:
            # 160 → 250 over the remaining 55%
            u = (t - 0.45) / 0.55
            alpha = int(160 + u ** 0.85 * 90)
        ramp_px[0, y] = min(255, alpha)
    alpha_img = Image.new("L", (w, h), 0)
    alpha_img.paste(ramp.resize((w, span), Image.BILINEAR), (0, start_y))
    black = Image.new("RGBA", (w, h), (0, 0, 0, 255))
    black.putalpha(alpha_img)
    composed = Image.alpha_composite(base.convert("RGBA"), black)
    base.paste(composed.convert("RGB"))


def _coverage(path: Path) -> frozenset[int]:
    """Codepoints a TTF can actually render (script-only Noto builds omit Latin)."""
    if path in _coverage_cache:
        return _coverage_cache[path]
    points: set[int] = set()
    try:
        from fontTools.ttLib import TTFont

        with TTFont(str(path), fontNumber=0, lazy=True) as tt:
            for table in tt["cmap"].tables:
                points.update(table.cmap.keys())
    except Exception as exc:
        print(f"[warn] cmap read failed for {path.name}: {exc}")
    result = frozenset(points)
    _coverage_cache[path] = result
    return result


class FontSet:
    """A script font plus a Latin fallback, so mixed headlines never show tofu.

    Noto's per-script builds (e.g. NotoSansDevanagari) carry no Latin letters,
    so an English word inside a Hindi headline renders as boxes unless each run
    is drawn with a font that covers it.
    """

    def __init__(self, primary, primary_cp, latin, latin_cp):
        self.primary = primary
        self.primary_cp = primary_cp
        self.latin = latin
        self.latin_cp = latin_cp

    def _choose(self, ch: str, prefer_latin: bool):
        cp = ord(ch)
        if prefer_latin and self.latin is not None and cp in self.latin_cp:
            return self.latin
        if cp in self.primary_cp:
            return self.primary
        if self.latin is not None and cp in self.latin_cp:
            return self.latin
        return self.primary

    def runs(self, text: str) -> list[tuple[str, object]]:
        """Split ``text`` into (substring, font) runs, keeping scripts contiguous."""
        out: list[list] = []
        for token in re.split(r"(\s+)", text):
            if not token:
                continue
            # Keep whole Latin tokens (ICC, T20, BJP-2024) in one font.
            prefer_latin = bool(re.search(r"[A-Za-z]", token))
            for ch in token:
                font = self._choose(ch, prefer_latin)
                if out and out[-1][1] is font:
                    out[-1][0] += ch
                else:
                    out.append([ch, font])
        return [(s, f) for s, f in out]

    def metrics(self, text: str) -> tuple[int, int]:
        """Max (ascent, descent) across the fonts used by ``text``."""
        fonts = {f for _, f in self.runs(text)} or {self.primary}
        ascent = descent = 0
        for font in fonts:
            a, d = font.getmetrics()
            ascent = max(ascent, a)
            descent = max(descent, d)
        return ascent, descent

    def ink_extent(self, draw, text: str) -> tuple[int, int]:
        """Real ink ascent/descent from textbbox (catches Indic descenders)."""
        ascent, descent = self.metrics(text)
        x = 0.0
        top = 0
        bottom = 0
        for run, font in self.runs(text):
            bbox = draw.textbbox((x, 0), run, font=font, anchor="ls")
            top = min(top, bbox[1])
            bottom = max(bottom, bbox[3])
            x += draw.textlength(run, font=font)
        # Prefer ink box when it's taller than font metrics (common for Devanagari).
        return max(ascent, -top), max(descent, bottom)


_coverage_cache: dict[Path, frozenset[int]] = {}
_shaping_checked = False


def _warn_if_no_shaping() -> None:
    """Indic matras only reorder correctly when Pillow ships Raqm/HarfBuzz."""
    global _shaping_checked
    if _shaping_checked:
        return
    _shaping_checked = True
    try:
        from PIL import features

        if not features.check("raqm"):
            print(
                "[warn] Pillow has no Raqm/HarfBuzz — Indic text shaping will be "
                "wrong (misplaced matras). Install a Pillow wheel with Raqm."
            )
    except Exception:
        pass


def _text_width(draw, text: str, fs: FontSet) -> float:
    return sum(draw.textlength(run, font=font) for run, font in fs.runs(text))


def _draw_runs(draw, x: float, baseline: float, text: str, fs: FontSet, fill) -> None:
    for run, font in fs.runs(text):
        draw.text((x, baseline), run, font=font, fill=fill, anchor="ls")
        x += draw.textlength(run, font=font)


def _ellipsize(draw, text: str, fs: FontSet, max_width: float) -> str:
    if _text_width(draw, text, fs) <= max_width:
        return text
    while text:
        candidate = text.rstrip() + "…"
        if _text_width(draw, candidate, fs) <= max_width:
            return candidate
        text = text[:-1]
    return "…"


def _wrap_lines(text: str, fs: FontSet, draw, max_width: float, max_lines: int = 3) -> list[str]:
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
        if _text_width(draw, trial, fs) <= max_width:
            current = trial
            i += 1
            continue
        if current:
            lines.append(current)
            current = ""
            if len(lines) >= max_lines:
                break
            continue
        lines.append(_ellipsize(draw, word, fs, max_width))
        i += 1
        if len(lines) >= max_lines:
            break

    if current and len(lines) < max_lines:
        lines.append(current)
        i = len(words)

    if i < len(words) and lines:
        lines[-1] = _ellipsize(draw, lines[-1].rstrip("…"), fs, max_width)

    return lines[:max_lines]


def _build_font_set(lang: str, size: int) -> FontSet | None:
    from PIL import ImageFont

    if lang != "en":
        _warn_if_no_shaping()
    primary_path = _ensure_font(lang)
    if primary_path is None:
        return None
    latin_path = primary_path if lang == "en" else _ensure_font("en")

    primary = ImageFont.truetype(str(primary_path), size=size)
    primary_cp = _coverage(primary_path)
    if latin_path is None or latin_path == primary_path:
        return FontSet(primary, primary_cp, None, frozenset())

    latin = ImageFont.truetype(str(latin_path), size=size)
    return FontSet(primary, primary_cp, latin, _coverage(latin_path))


def _pick_font_size(lang: str, draw, text: str, max_width: float):
    # Indic: prefer 2 lines so the block fits the BigPicture safe band.
    max_lines = 2 if lang != "en" else 3
    sizes = [48, 44, 40, 36, 32, 28] if lang == "en" else [44, 40, 36, 32, 28, 26]
    fallback = None
    for size in sizes:
        fs = _build_font_set(lang, size)
        if fs is None:
            return None, []
        lines = _wrap_lines(text, fs, draw, max_width, max_lines=max_lines)
        fallback = (fs, lines)
        if lines and not lines[-1].endswith("…"):
            return fs, lines
    return fallback if fallback else (None, [])


def bake_notification_image(
    image_url: str,
    title: str,
    lang: str = "en",
) -> bytes | None:
    """Download ``image_url``, overlay gradient + headline, return JPEG bytes."""
    from PIL import Image, ImageDraw

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
    # Bottom safe zone — Android BigPicture often clips a few % at the edge.
    margin_bottom = max(72, int(NOTIF_HEIGHT * NOTIF_SAFE_BOTTOM))
    max_width = NOTIF_WIDTH - margin_x * 2
    headline = _clean_headline(title)

    font_set, lines = _pick_font_size(lang, draw, headline, max_width)
    if font_set is None or not lines:
        print(f"[warn] no usable font for lang={lang}; skipping text overlay")
    else:
        # Ink extents catch Indic descenders that font.getmetrics() under-reports.
        extents = [font_set.ink_extent(draw, line) for line in lines]
        line_heights = [a + d for a, d in extents]
        gap = max(10, int(line_heights[0] * 0.22))
        # Extra pad under the last line so matras/descenders aren't clipped.
        descender_pad = max(12, extents[-1][1] // 2)
        block_h = sum(line_heights) + gap * (len(lines) - 1) + descender_pad
        top = NOTIF_HEIGHT - margin_bottom - block_h

        for line, (ascent, descent) in zip(lines, extents):
            baseline = top + ascent
            _draw_runs(draw, margin_x + 1, baseline + 2, line, font_set, (0, 0, 0))
            _draw_runs(draw, margin_x, baseline, line, font_set, (255, 255, 255))
            top = baseline + descent + gap

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
