"""
agents/soap_uploader.py — Soap Opera Shorts: YouTube Uploader

Called by discord_bot.py when the owner reacts ✅ to a soap clip.
Uploads the approved clip to YouTube Shorts using the existing YouTube
credentials already configured in settings.py.

Can also be run standalone:
    python -m agents.soap_uploader --record '{"clip_path": "...", "job": {...}, "hotspot": {...}}'
"""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

from config.settings import settings

log = logging.getLogger("soap_uploader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


# =============================================================================
# Build video metadata
# =============================================================================

def ts_label(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


def build_title(job: dict, hotspot: dict) -> str:
    """Punchy title matching publisher.py's style."""
    show = job.get("title", "Turkish Drama")
    # Strip episode number suffix if present (e.g. "Kızılcık Şerbeti 45. Bölüm")
    # Keep it short and punchy for Shorts
    if len(show) > 40:
        show = show[:37] + "..."
    label = ts_label(hotspot["start_sec"])
    return f"{show} | {label} 🔥 #Shorts"


def build_description(job: dict, hotspot: dict) -> str:
    intensity_pct = int(hotspot["intensity"] * 100)
    return (
        f"Most replayed moment from: {job['title']}\n"
        f"⏱ Timestamp: {ts_label(hotspot['start_sec'])}\n"
        f"📊 Most Replayed intensity: {intensity_pct}%\n"
        f"📺 Full episode: {job['url']}\n\n"
        "#Shorts #TurkishDrama #Dizi #TurkishSeries #DiziFan"
    )


def build_tags(job: dict) -> list[str]:
    base = ["Shorts", "Turkish drama", "Turkish series", "dizi", "turkish dizi"]
    words = [w for w in job.get("title", "").split() if len(w) > 2][:5]
    return base + words


# =============================================================================
# Upload — reuses same credentials as existing youtube_upload.py
# =============================================================================

def upload_to_youtube(clip_path: Path, title: str, description: str, tags: list[str]) -> str | None:
    """
    Reuses the same upload path as agents/youtube_upload.py.
    Imports it directly to avoid duplicating OAuth logic.
    """
    try:
        # Use existing youtube_upload agent if it exists
        from agents.youtube_upload import upload_to_youtube as _upload
        video_id = _upload(clip_path, title, description, tags)
        return video_id
    except ImportError:
        pass

    # Fallback: inline upload using googleapiclient
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        log.error("google-api-python-client not installed. Run: pip install google-api-python-client")
        return None

    client_id     = settings.YOUTUBE_CLIENT_ID
    client_secret = settings.YOUTUBE_CLIENT_SECRET
    refresh_token = settings.YOUTUBE_REFRESH_TOKEN

    if not all([client_id, client_secret, refresh_token]):
        log.error("Missing YouTube credentials in settings")
        return None

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/youtube.upload"],
    )
    creds.refresh(Request())

    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)

    body = {
        "snippet": {
            "title":           title,
            "description":     description,
            "tags":            tags,
            "categoryId":      "24",   # Entertainment
            "defaultLanguage": "tr",
        },
        "status": {
            "privacyStatus":            os.getenv("YT_UPLOAD_PRIVACY", "public"),
            "selfDeclaredMadeForKids":  False,
        },
    }

    media = MediaFileUpload(
        str(clip_path),
        mimetype="video/mp4",
        resumable=True,
        chunksize=10 * 1024 * 1024,
    )

    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            log.info(f"Upload {int(status.progress() * 100)}%...")

    video_id = response["id"]
    log.info(f"Uploaded: https://youtube.com/shorts/{video_id}")
    return video_id


# =============================================================================
# Main entry point — called by discord_bot.py on ✅ reaction
# =============================================================================

def handle_approval(record: dict) -> str | None:
    """
    Called from discord_bot.py's on_raw_reaction_add when type == 'soap_short'.
    record keys: clip_path, job, hotspot, clip_index
    Returns YouTube Shorts URL or None.
    """
    clip_path = Path(record["clip_path"])
    job       = record["job"]
    hotspot   = record["hotspot"]

    if not clip_path.exists():
        log.error(f"Clip file not found: {clip_path}")
        return None

    title       = build_title(job, hotspot)
    description = build_description(job, hotspot)
    tags        = build_tags(job)

    log.info(f"Uploading: {title}")
    video_id = upload_to_youtube(clip_path, title, description, tags)

    if video_id:
        clip_path.unlink(missing_ok=True)   # clean up after successful upload
        return f"https://youtube.com/shorts/{video_id}"
    return None


# =============================================================================
# CLI entry point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Soap Opera Shorts — YouTube Uploader")
    parser.add_argument("--record", required=True, help="JSON record string from soap_discord_pending.jsonl")
    args = parser.parse_args()

    record = json.loads(args.record)
    url = handle_approval(record)
    if url:
        log.info(f"Uploaded: {url}")
        sys.exit(0)
    else:
        log.error("Upload failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
