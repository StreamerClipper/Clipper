"""
agents/soap_clipper.py — Soap Opera Shorts: Clipper

Reads from output/soap_pending.jsonl (written by soap_scout.py).
For each job:
  1. Downloads each hotspot segment with yt-dlp --download-sections
  2. Crops to 9:16 (centre crop — soap operas are full-frame, no webcam)
  3. Burns auto-generated Turkish/English subtitles with ffmpeg
  4. Sends each clip to Discord for approval (same ✅/❌ pattern as clipper.py)

Designed to run inside GitHub Actions (triggered by soap_pending.jsonl commit),
or locally:
    python -m agents.soap_clipper
"""
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from config.settings import settings

log = logging.getLogger("soap_clipper")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

SOAP_PENDING_FILE   = Path("output/soap_pending.jsonl")
SOAP_PROCESSED_FILE = Path("output/soap_processed.jsonl")
SOAP_DISCORD_FILE   = Path("output/soap_discord_pending.jsonl")
CLIPS_DIR           = Path("output/clips")
TMP_DIR             = Path("/tmp/soap_clipper")

CLIP_DURATION       = 30     # seconds per Short
OUT_W, OUT_H        = 608, 1080

DISCORD_BOT_TOKEN   = settings.DISCORD_BOT_TOKEN
DISCORD_CHANNEL_ID  = settings.DISCORD_CHANNEL_ID


# =============================================================================
# Helpers
# =============================================================================

def discord_log(message: str):
    if not DISCORD_BOT_TOKEN:
        return
    try:
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
            headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
            json={"content": message},
            timeout=5,
        )
    except Exception:
        pass


def ts(seconds: float) -> str:
    """Seconds → HH:MM:SS.mmm for yt-dlp / ffmpeg."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


def ts_label(seconds: float) -> str:
    """Seconds → MM:SS for display."""
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


# =============================================================================
# Queue management (mirrors clipper.py's load_next_moment / mark_processed)
# =============================================================================

def load_next_job():
    if not SOAP_PENDING_FILE.exists():
        return None, None
    lines = [l for l in SOAP_PENDING_FILE.read_text().strip().splitlines() if l.strip()]
    if not lines:
        return None, None
    return json.loads(lines[-1]), lines


def mark_processed(job: dict, lines: list[str]):
    remaining = lines[:-1]
    SOAP_PENDING_FILE.write_text("\n".join(remaining) + ("\n" if remaining else ""))
    job["processed_at"] = datetime.now(timezone.utc).isoformat()
    with open(SOAP_PROCESSED_FILE, "a") as f:
        f.write(json.dumps(job) + "\n")


# =============================================================================
# Step 1 — Download hotspot segment
# =============================================================================

def download_segment(url: str, start: float, duration: int, out: Path) -> bool:
    """
    Download only the hotspot window using yt-dlp --download-sections.
    Much faster than downloading the full episode.
    """
    section = f"*{ts(start)}-{ts(start + duration)}"
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--no-warnings",
        "--cookies", "/home/StreamerClipper/clipbot/cookies.txt",
        "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "--download-sections", section,
        "--force-keyframes-at-cuts",
        "-o", str(out),
        url,
    ]
    log.info(f"Downloading segment {ts_label(start)}–{ts_label(start+duration)}...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if result.returncode != 0 or not out.exists():
        log.error(f"yt-dlp download failed: {result.stderr[:400]}")
        return False
    log.info(f"Downloaded: {out.stat().st_size/1024/1024:.1f}MB")
    return True


# =============================================================================
# Step 2 — Fetch subtitles
# =============================================================================

def fetch_subtitles(url: str, stem: Path) -> Path | None:
    """
    Try to fetch auto-generated subtitles in Turkish then English.
    Returns path to .vtt file if found, else None.
    """
    for lang in ("tr", "en", "en-US"):
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--skip-download",
            "--no-warnings",
        "--cookies", "/home/StreamerClipper/clipbot/cookies.txt",
            "--write-auto-sub",
            "--sub-lang", lang,
            "--sub-format", "vtt",
            "-o", str(stem),
            url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        # yt-dlp writes stem.lang.vtt
        candidate = stem.parent / f"{stem.name}.{lang}.vtt"
        if candidate.exists():
            log.info(f"Subtitles fetched: {lang}")
            return candidate

    log.warning("No subtitles available")
    return None


# =============================================================================
# Step 3 — Crop to 9:16 (centre crop, no webcam)
# =============================================================================

def get_video_dimensions(path: Path) -> tuple[int, int]:
    """Reuses same approach as clipper.py."""
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(path)],
        capture_output=True, text=True,
    )
    streams = json.loads(probe.stdout).get("streams", [])
    video = next((s for s in streams if s["codec_type"] == "video"), None)
    if not video:
        return 1920, 1080
    return int(video["width"]), int(video["height"])


def crop_to_vertical(input_path: Path, output_path: Path) -> bool:
    """
    Soap operas are full-frame 16:9. Centre-crop to 9:16 (608×1080).
    Matches the fullscreen path in clipper.py's crop_to_vertical().
    """
    w, h = get_video_dimensions(input_path)
    target_w = int(h * 9 / 16)
    crop_x   = (w - target_w) // 2

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", f"crop={target_w}:{h}:{crop_x}:0,scale={OUT_W}:{OUT_H}",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        log.error(f"ffmpeg crop failed: {result.stderr[-400:]}")
        return False
    log.info(f"Cropped to 9:16: {output_path}")
    return True


# =============================================================================
# Step 4 — Burn subtitles
# =============================================================================

def burn_subtitles(input_path: Path, sub_path: Path, output_path: Path, sub_offset: float) -> bool:
    """
    Burn the VTT subtitles into the video.
    Uses the same font style as clipper.py's add_captions().
    sub_offset shifts subtitle timestamps to align with the clip window.
    """
    safe_sub = str(sub_path).replace("\\", "/")
    vf = (
        f"subtitles='{safe_sub}':si=0"
        f":force_style='FontName=LuckiestGuy-Regular,"
        f"FontSize=20,Bold=1,"
        f"PrimaryColour=&H00FFFFFF,"
        f"OutlineColour=&H00000000,"
        f"Outline=2,Shadow=1,MarginV=160'"
    )
    cmd = [
        "ffmpeg", "-y",
        "-itsoffset", str(-sub_offset),
        "-i", str(input_path),
        "-vf", vf,
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        log.warning(f"Subtitle burn failed — skipping: {result.stderr[-300:]}")
        shutil.copy(input_path, output_path)
    return True


# =============================================================================
# Step 5 — Post to Discord for approval (same pattern as publisher.py)
# =============================================================================

def send_clip_to_discord(clip_path: Path, job: dict, hotspot: dict, clip_index: int) -> str | None:
    """
    Upload the clip to Discord for ✅/❌ approval.
    Returns the Discord message ID or None on failure.
    Saves to soap_discord_pending.jsonl so discord_bot.py (extended) can pick it up.
    """
    if not DISCORD_BOT_TOKEN:
        log.error("DISCORD_BOT_TOKEN not set")
        return None

    intensity_pct = int(hotspot["intensity"] * 100)
    content = (
        f"📺 **[Soap Shorts]** Clip {clip_index+1}/3 ready for approval\n\n"
        f"**Episode:** {job['title']}\n"
        f"**Timestamp:** `{ts_label(hotspot['start_sec'])}` — `{ts_label(hotspot['end_sec'])}`\n"
        f"**Most Replayed intensity:** `{intensity_pct}%`\n\n"
        f"React with ✅ to upload to YouTube Shorts, or ❌ to discard."
    )

    try:
        url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
        headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}

        with open(clip_path, "rb") as f:
            resp = requests.post(
                url,
                headers=headers,
                files={"file": (clip_path.name, f, "video/mp4")},
                data={"content": content},
                timeout=120,
            )

        if resp.status_code not in (200, 201):
            log.error(f"Discord upload failed ({resp.status_code}): {resp.text[:300]}")
            return None

        message_id = resp.json()["id"]
        log.info(f"Clip {clip_index+1} sent to Discord — message {message_id}")

        # Add reactions (mirrors publisher.py)
        import time
        import urllib.parse
        for emoji in ["✅", "❌"]:
            encoded = urllib.parse.quote(emoji)
            react_url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages/{message_id}/reactions/{encoded}/@me"
            requests.put(react_url, headers=headers, timeout=5)
            time.sleep(0.5)

        # Persist for discord_bot.py to pick up on reaction
        record = {
            "message_id":  message_id,
            "clip_path":   str(clip_path),
            "job":         job,
            "hotspot":     hotspot,
            "clip_index":  clip_index,
            "type":        "soap_short",   # lets discord_bot.py distinguish from Kick clips
        }
        SOAP_DISCORD_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SOAP_DISCORD_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

        return message_id

    except Exception as e:
        log.error(f"Discord send failed: {e}")
        return None


# =============================================================================
# Process one hotspot → clip
# =============================================================================

def process_hotspot(job: dict, hotspot: dict, clip_index: int) -> Path | None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)

    vid   = job["video_id"]
    slug  = f"soap_{vid}_clip{clip_index}_{datetime.now(timezone.utc).strftime('%H%M%S')}"
    url   = job["url"]
    start = hotspot["start_sec"]

    raw      = TMP_DIR / f"{slug}_raw.mp4"
    cropped  = TMP_DIR / f"{slug}_cropped.mp4"
    subbed   = TMP_DIR / f"{slug}_subbed.mp4"
    final    = CLIPS_DIR / f"{slug}_final.mp4"

    # 1. Download segment
    if not download_segment(url, start, CLIP_DURATION, raw):
        return None

    # 2. Fetch subtitles (do once per job, reuse across clips)
    sub_stem = TMP_DIR / f"soap_{vid}_subs"
    sub_path = sub_stem.parent / f"{sub_stem.name}.vtt"
    if not sub_path.exists():
        fetched = fetch_subtitles(url, sub_stem)
        if fetched:
            fetched.rename(sub_path)

    # 3. Crop 16:9 → 9:16
    if not crop_to_vertical(raw, cropped):
        return None
    raw.unlink(missing_ok=True)

    # 4. Burn subtitles if available
    if sub_path.exists():
        burn_subtitles(cropped, sub_path, subbed, start)
        cropped.unlink(missing_ok=True)
    else:
        subbed = cropped   # skip subtitle step cleanly

    shutil.move(str(subbed), str(final))
    log.info(f"Clip ready: {final}")
    return final


# =============================================================================
# Main
# =============================================================================

def main():
    job, lines = load_next_job()
    if job is None:
        log.info("No pending soap jobs — nothing to do")
        sys.exit(0)

    log.info(f"Processing: {job['title']} ({len(job['hotspots'])} hotspots)")
    discord_log(
        f"⚙️ **[Soap Shorts]** Processing *{job['title']}*\n"
        f"Generating {len(job['hotspots'])} clips..."
    )

    clips_sent = 0
    for i, hotspot in enumerate(job["hotspots"]):
        log.info(f"--- Hotspot {i+1}/{len(job['hotspots'])} @ {ts_label(hotspot['start_sec'])} ---")
        clip_path = process_hotspot(job, hotspot, i)
        if clip_path:
            msg_id = send_clip_to_discord(clip_path, job, hotspot, i)
            if msg_id:
                clips_sent += 1
        else:
            log.warning(f"Hotspot {i+1} failed — skipping")

    mark_processed(job, lines)
    discord_log(
        f"✅ **[Soap Shorts]** Done — {clips_sent}/{len(job['hotspots'])} clips sent for approval."
    )
    log.info(f"Done — {clips_sent} clips sent to Discord")


if __name__ == "__main__":
    main()
