"""
agents/publisher.py — Agent 3: Publisher

Runs inside GitHub Actions immediately after the Clipper.
1. Generates title + hashtags via Claude
2. Uploads the clip to Discord for approval
3. The Discord bot (running on PythonAnywhere) handles platform posting
   after you react with ✅ or ❌
"""
import json
import logging
import os
import sys
from pathlib import Path
import urllib.request
import urllib.parse

log = logging.getLogger("publisher")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "1482642033481875536")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# =============================================================================
# Title + hashtag generation via Claude
# =============================================================================

def generate_metadata(channel: str, trigger_messages: list[str]) -> dict:
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY — using fallback title")
        return {
            "title": f"{channel} goes crazy on Kick",
            "hashtags": ["#kick", "#clips", "#gaming", f"#{channel}"],
            "description": f"Wild moment from {channel}'s stream on Kick.",
        }

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        sample = "\n".join(trigger_messages[-5:]) if trigger_messages else "(no sample)"

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""You write short-form video titles for Kick stream clips posted to TikTok, YouTube Shorts and Instagram Reels.

Channel: {channel}
Chat reaction sample:
{sample}

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
# Discord upload — sends clip for approval
# =============================================================================

def send_to_discord(clip_path: Path, meta: dict, moment_data: dict) -> bool:
    """
    Upload the clip to Discord approval channel.
    Uses multipart form upload via urllib (no extra deps needed in Actions).
    """
    if not DISCORD_BOT_TOKEN:
        log.error("DISCORD_BOT_TOKEN not set — cannot send to Discord")
        return False

    if not clip_path.exists():
        log.error(f"Clip file not found: {clip_path}")
        return False

    channel = moment_data.get("channel", "unknown")
    rate = moment_data.get("message_rate", 0)
    sample = moment_data.get("trigger_messages", [])
    sample_text = sample[-1] if sample else ""

    # Build the approval message
    content = (
        f"🎬 **New clip ready for approval**\n\n"
        f"**Channel:** #{channel}\n"
        f"**Title:** {meta['title']}\n"
        f"**Hashtags:** {' '.join(meta['hashtags'])}\n"
        f"**Hype rate:** {rate:.0f} msgs/10s\n"
        f"**Sample:** `{sample_text}`\n\n"
        f"React with ✅ to post or ❌ to discard."
    )

    log.info(f"Uploading clip to Discord channel {DISCORD_CHANNEL_ID}...")

    try:
        import requests
        url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
        headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}

        with open(clip_path, "rb") as f:
            files = {"file": (clip_path.name, f, "video/mp4")}
            data = {
                "content": content,
                "payload_json": json.dumps({"content": content})
            }
            resp = requests.post(url, headers=headers, files=files, data={"content": content})

        if resp.status_code not in (200, 201):
            log.error(f"Discord upload failed ({resp.status_code}): {resp.text[:300]}")
            return False

        message_id = resp.json()["id"]
        log.info(f"Clip sent to Discord — message ID: {message_id}")

        # Add approval reactions
        for emoji in ["✅", "❌"]:
            encoded = urllib.parse.quote(emoji)
            reaction_url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages/{message_id}/reactions/{encoded}/@me"
            react_resp = requests.put(reaction_url, headers=headers)
            if react_resp.status_code == 204:
                log.info(f"Added {emoji} reaction")
            else:
                log.warning(f"Failed to add {emoji}: {react_resp.status_code}")

        # Save message ID so the Discord bot can look it up
        pending = {
            "message_id": message_id,
            "clip_path": str(clip_path),
            "meta": meta,
            "moment": moment_data,
        }
        pending_path = Path("output/discord_pending.jsonl")
        with open(pending_path, "a") as f:
            f.write(json.dumps(pending) + "\n")

        log.info("Pending approval saved — Discord bot will post when you react ✅")
        return True

    except Exception as e:
        log.error(f"Discord send failed: {e}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    clip_ref = Path("output/latest_clip.txt")
    if not clip_ref.exists():
        log.warning("No latest_clip.txt — nothing to publish")
        return

    clip_path = Path(clip_ref.read_text().strip())
    if not clip_path.exists():
        log.error(f"Clip file not found: {clip_path}")
        return

    # Load moment data for context
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

    log.info(f"Title: {meta['title']}")
    log.info(f"Tags:  {' '.join(meta['hashtags'])}")

    # Send to Discord for approval
    success = send_to_discord(clip_path, meta, moment_data)
    if success:
        log.info("Clip sent to Discord — waiting for your ✅ or ❌")
    else:
        log.error("Failed to send to Discord")
        sys.exit(1)


if __name__ == "__main__":
    main()
