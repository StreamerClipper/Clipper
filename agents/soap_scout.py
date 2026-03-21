"""
agents/soap_scout.py — Soap Opera Shorts: Scout

Polls a YouTube playlist (or channel uploads) for new episodes.
When a new episode is found, writes it to output/soap_pending.jsonl
so soap_clipper.py can process it.

Also handles the Discord !soap commands (mirrors !hype commands in discord_bot.py).

Run on PythonAnywhere always-on task:
    cd /home/StreamerClipper/clipbot && python -m agents.soap_scout

Or trigger manually with a URL:
    python -m agents.soap_scout --url https://www.youtube.com/watch?v=XXXXX
"""
import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from config.settings import settings

log = logging.getLogger("soap_scout")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

POLL_INTERVAL = int(os.getenv("SOAP_POLL_INTERVAL", 3600))   # 1 hour default
SOAP_PENDING_FILE = Path("output/soap_pending.jsonl")
SOAP_SEEN_FILE    = Path("output/soap_seen.json")            # tracks already-processed video IDs
DISCORD_CHANNEL_ID = settings.DISCORD_CHANNEL_ID


# =============================================================================
# Discord logging (mirrors discord_log() in scout.py)
# =============================================================================

def discord_log(message: str, channel_id: str = None):
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        return
    cid = channel_id or DISCORD_CHANNEL_ID
    try:
        requests.post(
            f"https://discord.com/api/v10/channels/{cid}/messages",
            headers={"Authorization": f"Bot {token}"},
            json={"content": message},
            timeout=5,
        )
    except Exception:
        pass


# =============================================================================
# Seen-video tracking
# =============================================================================

def load_seen() -> set[str]:
    if SOAP_SEEN_FILE.exists():
        return set(json.loads(SOAP_SEEN_FILE.read_text()))
    return set()


def mark_seen(video_id: str):
    seen = load_seen()
    seen.add(video_id)
    SOAP_SEEN_FILE.write_text(json.dumps(list(seen)))


# =============================================================================
# yt-dlp helpers
# =============================================================================

def fetch_playlist_entries(playlist_url: str, max_entries: int = 5) -> list[dict]:
    """
    Return the most recent N entries from a YouTube playlist/channel.
    Each entry: {id, title, url, upload_date, duration}
    """
    cmd = [
        "yt-dlp",
        "--dump-json",
        "--flat-playlist",
        "--playlist-end", str(max_entries),
        "--no-warnings",
        "--cookies", "/home/StreamerClipper/clipbot/cookies.txt",
        playlist_url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        log.error(f"yt-dlp playlist fetch failed: {result.stderr[:300]}")
        return []

    entries = []
    for line in result.stdout.strip().splitlines():
        try:
            entry = json.loads(line)
            entries.append({
                "id":          entry.get("id", ""),
                "title":       entry.get("title", ""),
                "url":         entry.get("url") or f"https://www.youtube.com/watch?v={entry['id']}",
                "upload_date": entry.get("upload_date", ""),
                "duration":    entry.get("duration", 0),
            })
        except Exception:
            continue
    return entries


def fetch_video_metadata(url: str) -> dict | None:
    """
    Fetch full metadata + heatmap for a single video URL.
    Returns dict with id, title, duration, heatmap list.
    """
    cmd = [
        "yt-dlp",
        "--dump-json",
        "--no-playlist",
        "--no-warnings",
        "--cookies", "/home/StreamerClipper/clipbot/cookies.txt",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        log.error(f"yt-dlp metadata fetch failed: {result.stderr[:300]}")
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        log.error("yt-dlp returned invalid JSON")
        return None

    raw_heatmap = data.get("heatmap") or []
    heatmap = [
        {
            "start":     p.get("start_time", 0),
            "end":       p.get("end_time", 0),
            "intensity": p.get("value", 0.0),
        }
        for p in raw_heatmap
    ]

    return {
        "video_id":    data["id"],
        "title":       data.get("title", ""),
        "url":         f"https://www.youtube.com/watch?v={data['id']}",
        "duration":    data.get("duration", 0),
        "heatmap":     heatmap,
        "upload_date": data.get("upload_date", ""),
    }


# =============================================================================
# Hotspot detection  (top 3 non-overlapping 30s windows)
# =============================================================================

CLIP_DURATION = 30
TOP_N = 3


def find_hotspots(heatmap: list[dict]) -> list[dict]:
    """
    Find top N non-overlapping peaks in the Most Replayed heatmap.
    Returns list of {start_sec, peak_sec, end_sec, intensity} dicts,
    in chronological order.
    """
    if not heatmap:
        return []

    half = CLIP_DURATION / 2
    ranked = sorted(heatmap, key=lambda p: p["intensity"], reverse=True)
    chosen = []
    windows = []   # list of (win_start, win_end) already claimed

    for point in ranked:
        if len(chosen) >= TOP_N:
            break

        peak_sec  = (point["start"] + point["end"]) / 2
        win_start = max(0.0, peak_sec - half)
        win_end   = win_start + CLIP_DURATION

        # Skip if overlaps with any already-chosen window
        if any(not (win_end <= ws or win_start >= we) for ws, we in windows):
            continue

        chosen.append({
            "start_sec": win_start,
            "peak_sec":  peak_sec,
            "end_sec":   win_end,
            "intensity": point["intensity"],
        })
        windows.append((win_start, win_end))

    # Return in chronological order — easier for the editor to review
    return sorted(chosen, key=lambda h: h["start_sec"])


# =============================================================================
# Queue a job for soap_clipper.py
# =============================================================================

def enqueue_job(video_meta: dict, hotspots: list[dict]):
    SOAP_PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    job = {
        "video_id":    video_meta["video_id"],
        "title":       video_meta["title"],
        "url":         video_meta["url"],
        "duration":    video_meta["duration"],
        "hotspots":    hotspots,
        "queued_at":   datetime.now(timezone.utc).isoformat(),
    }
    with open(SOAP_PENDING_FILE, "a") as f:
        f.write(json.dumps(job) + "\n")
    log.info(f"Queued job: {video_meta['title']} ({len(hotspots)} hotspots)")


# =============================================================================
# Process a single URL (used both by manual --url and playlist polling)
# =============================================================================

def process_url(url: str) -> bool:
    log.info(f"Fetching metadata for: {url}")
    meta = fetch_video_metadata(url)
    if not meta:
        discord_log(f"❌ **[Soap]** Could not fetch metadata for `{url}`")
        return False

    if not meta["heatmap"]:
        discord_log(
            f"⚠️ **[Soap]** No Most Replayed data for *{meta['title']}*\n"
            f"The video may be too new or have too few views. Try again later."
        )
        return False

    hotspots = find_hotspots(meta["heatmap"])
    if not hotspots:
        discord_log(f"⚠️ **[Soap]** No usable hotspots found in *{meta['title']}*")
        return False

    enqueue_job(meta, hotspots)

    hs_summary = "\n".join(
        f"  • Clip {i+1}: `{int(h['start_sec']//60):02d}:{int(h['start_sec']%60):02d}` — `{h['intensity']:.0%}` intensity"
        for i, h in enumerate(hotspots)
    )
    discord_log(
        f"📺 **[Soap]** New episode queued for clipping!\n"
        f"**{meta['title']}**\n"
        f"Hotspots found:\n{hs_summary}\n"
        f"⏳ Clips will appear for approval shortly..."
    )
    return True


# =============================================================================
# Playlist polling loop
# =============================================================================

def poll_playlist(playlist_url: str):
    log.info(f"Polling playlist: {playlist_url} every {POLL_INTERVAL}s")
    discord_log(f"📡 **[Soap Scout]** Started — polling playlist every {POLL_INTERVAL//3600}h")

    while True:
        seen = load_seen()
        entries = fetch_playlist_entries(playlist_url, max_entries=5)

        for entry in entries:
            vid = entry["id"]
            if vid in seen:
                continue
            log.info(f"New episode: {entry['title']}")
            ok = process_url(entry["url"])
            if ok:
                mark_seen(vid)

        time.sleep(POLL_INTERVAL)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Soap Opera Shorts — Scout")
    parser.add_argument("--url",      help="Process a single YouTube URL immediately")
    parser.add_argument("--playlist", help="Poll a playlist URL (overrides SOAP_PLAYLIST_URL in .env)")
    parser.add_argument("--debug",    action="store_true")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.url:
        # Manual single-video trigger (used by the Discord !soap clip command)
        ok = process_url(args.url)
        sys.exit(0 if ok else 1)

    playlist_url = args.playlist or os.getenv("SOAP_PLAYLIST_URL")
    if not playlist_url:
        log.error("No --url or --playlist provided, and SOAP_PLAYLIST_URL not set in .env")
        sys.exit(1)

    poll_playlist(playlist_url)


if __name__ == "__main__":
    main()
