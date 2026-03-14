"""
agents/publisher.py — Agent 3: Publisher

Reads output/latest_clip.txt (written by Clipper), generates a title
and hashtags using Claude, then posts to YouTube Shorts / TikTok / Instagram.

Runs as a GitHub Actions step immediately after the Clipper.
Platform API integrations are stubbed — fill in as you connect each platform.
"""
import json
import logging
import os
from pathlib import Path

log = logging.getLogger("publisher")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


# =============================================================================
# Title + hashtag generation via Claude
# =============================================================================

def generate_metadata(channel: str, trigger_messages: list[str]) -> dict:
    """Use Claude to write a scroll-stopping title and hashtags."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("No ANTHROPIC_API_KEY — using fallback title")
        return {
            "title": f"{channel} goes crazy on Kick",
            "hashtags": ["#kick", "#clips", "#gaming", f"#{channel}"],
            "description": f"Wild moment from {channel}'s stream on Kick.",
        }

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        sample_chat = "\n".join(trigger_messages[-5:]) if trigger_messages else "(no sample)"

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""You write short-form video titles for Kick stream clips posted to TikTok, YouTube Shorts and Instagram Reels.

Channel: {channel}
Chat reaction sample:
{sample_chat}

Write a JSON object with these fields:
- title: punchy, under 60 chars, no clickbait. Describe what happened based on the chat.
- hashtags: array of 5 relevant hashtags (include #{channel} and #kick)
- description: one sentence description for YouTube, under 100 chars

Respond with only valid JSON, no markdown."""
            }]
        )

        text = message.content[0].text.strip()
        return json.loads(text)

    except Exception as e:
        log.error(f"Claude metadata generation failed: {e}")
        return {
            "title": f"{channel} hype moment on Kick",
            "hashtags": ["#kick", "#clips", f"#{channel}"],
            "description": f"Clip from {channel} on Kick.",
        }


# =============================================================================
# Platform posting stubs
# (Replace each stub with real API calls as you set up each platform)
# =============================================================================

def post_to_youtube(clip_path: Path, title: str, description: str) -> bool:
    """
    Upload to YouTube Shorts via YouTube Data API v3.
    Requires: YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET in env (OAuth2).
    Full implementation: https://developers.google.com/youtube/v3/guides/uploading_a_video
    """
    if not os.getenv("YOUTUBE_CLIENT_ID"):
        log.info("YouTube not configured — skipping")
        return False

    log.info(f"[YouTube] Would post: {title}")
    # TODO: implement OAuth2 flow + resumable upload
    # from googleapiclient.discovery import build
    # from googleapiclient.http import MediaFileUpload
    return False


def post_to_tiktok(clip_path: Path, title: str, hashtags: list[str]) -> bool:
    """
    Upload to TikTok via Content Posting API.
    Requires: TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET in env.
    Full implementation: https://developers.tiktok.com/doc/content-posting-api-get-started
    """
    if not os.getenv("TIKTOK_CLIENT_KEY"):
        log.info("TikTok not configured — skipping")
        return False

    log.info(f"[TikTok] Would post: {title}")
    # TODO: implement TikTok Content Posting API
    return False


def post_to_instagram(clip_path: Path, caption: str) -> bool:
    """
    Upload Reel via Instagram Graph API.
    Requires: INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_ACCOUNT_ID in env.
    Full implementation: https://developers.facebook.com/docs/instagram-api/guides/reels
    """
    if not os.getenv("INSTAGRAM_ACCESS_TOKEN"):
        log.info("Instagram not configured — skipping")
        return False

    log.info(f"[Instagram] Would post reel")
    # TODO: implement Instagram Graph API reel upload
    return False


# =============================================================================
# Main
# =============================================================================

def main():
    clip_ref = Path("output/latest_clip.txt")
    if not clip_ref.exists():
        log.warning("No latest_clip.txt found — nothing to publish")
        return

    clip_path = Path(clip_ref.read_text().strip())
    if not clip_path.exists():
        log.error(f"Clip file not found: {clip_path}")
        return

    # Load the most recent processed moment for context
    processed = Path("output/processed_moments.jsonl")
    moment_data = {}
    if processed.exists():
        lines = processed.read_text().strip().splitlines()
        if lines:
            moment_data = json.loads(lines[-1])

    channel = moment_data.get("channel", "streamer")
    trigger_messages = moment_data.get("trigger_messages", [])

    log.info(f"Generating metadata for clip from #{channel}...")
    meta = generate_metadata(channel, trigger_messages)

    log.info(f"Title     : {meta['title']}")
    log.info(f"Hashtags  : {' '.join(meta['hashtags'])}")
    log.info(f"Description: {meta['description']}")

    caption = f"{meta['title']}\n\n{' '.join(meta['hashtags'])}"

    results = {
        "youtube": post_to_youtube(clip_path, meta["title"], meta["description"]),
        "tiktok": post_to_tiktok(clip_path, meta["title"], meta["hashtags"]),
        "instagram": post_to_instagram(clip_path, caption),
    }

    posted = [p for p, ok in results.items() if ok]
    skipped = [p for p, ok in results.items() if not ok]

    if posted:
        log.info(f"Posted to: {', '.join(posted)}")
    if skipped:
        log.info(f"Skipped (not configured): {', '.join(skipped)}")

    # Save metadata alongside the clip
    meta_path = clip_path.with_suffix(".json")
    meta_path.write_text(json.dumps({**meta, "clip": str(clip_path), **results}, indent=2))
    log.info(f"Metadata saved: {meta_path}")


if __name__ == "__main__":
    main()
