"""
agents/discord_bot.py — Discord approval bot

Flow:
1. Clip posted to Discord with ✅/❌ reactions
2. Owner reacts ✅ → bot replies with 3 title suggestions
3. Owner replies with 1/2/3 or custom text within 5 minutes
4. Bot posts to platforms with chosen title
5. No reply in 5 minutes → uses suggestion 1 automatically

Run:
    cd ~/clipbot && python -m agents.discord_bot
"""
import asyncio
import json
import logging
import os
from pathlib import Path

import discord

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discord_bot")

APPROVE = "✅"
REJECT  = "❌"
TITLE_TIMEOUT = 300  # 5 minutes

# Tracks pending clips: message_id -> metadata dict
PENDING: dict[int, dict] = {}

# Tracks clips waiting for title: message_id -> metadata + suggestions
AWAITING_TITLE: dict[int, dict] = {}


# =============================================================================
# Title generation via Claude
# =============================================================================

def generate_title_suggestions(channel: str, trigger_messages: list[str]) -> list[str]:
    """Generate 3 title suggestions via Claude."""
    api_key = settings.ANTHROPIC_API_KEY
    if not api_key:
        return [
            f"{channel} goes crazy on Kick",
            f"Insane moment on {channel}'s stream",
            f"You won't believe what {channel} just did",
        ]

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        sample = "\n".join(trigger_messages[-5:]) if trigger_messages else "(no sample)"

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""Generate exactly 3 short punchy YouTube Shorts/TikTok titles for a clip from {channel}'s Kick stream.

Chat reaction sample:
{sample}

Rules:
- Under 60 characters each
- No clickbait, no ALL CAPS
- Describe what actually happened based on the chat
- Each title should take a different angle

Respond with ONLY a JSON array of 3 strings, no markdown:
["title 1", "title 2", "title 3"]"""
            }]
        )

        text = message.content[0].text.strip().replace("```json", "").replace("```", "")
        suggestions = json.loads(text)
        if isinstance(suggestions, list) and len(suggestions) == 3:
            return suggestions

    except Exception as e:
        log.warning(f"Claude title generation failed: {e}")

    return [
        f"{channel} goes crazy on Kick",
        f"Insane moment on {channel}'s stream",
        f"You won't believe what {channel} just did",
    ]


# =============================================================================
# Platform posting
# =============================================================================

async def post_to_platforms(meta: dict, title: str):
    """Download clip from GitHub and post to platforms."""
    import requests as req
    from pathlib import Path

    log.info(f"Posting to platforms: {title}")

    # Download clip from GitHub repo
    clip_filename = meta.get("clip_path", "").split("/")[-1]
    if not clip_filename:
        log.error("No clip filename in metadata")
        return

    clip_url = f"https://raw.githubusercontent.com/{settings.GITHUB_REPO}/main/output/clips/{clip_filename}"
    local_clip = Path(f"/tmp/{clip_filename}")

    log.info(f"Downloading clip from GitHub: {clip_filename}")
    r = req.get(clip_url, timeout=60)
    if r.status_code != 200:
        log.error(f"Failed to download clip: {r.status_code}")
        return

    local_clip.write_bytes(r.content)
    log.info(f"Clip downloaded: {local_clip} ({local_clip.stat().st_size/1024/1024:.1f}MB)")

    # YouTube upload
    if os.getenv("YOUTUBE_CLIENT_ID"):
        try:
            from agents.youtube_upload import upload_to_youtube
            hashtags = meta.get("hashtags", ["#kick", "#clips", "#gaming"])
            description = meta.get("description", "")
            video_id = upload_to_youtube(local_clip, title, description, hashtags)
            if video_id:
                log.info(f"[YouTube] Uploaded: https://youtube.com/shorts/{video_id}")
            else:
                log.warning("[YouTube] Upload returned no video ID")
        except Exception as e:
            log.error(f"[YouTube] Failed: {e}")

    if os.getenv("TIKTOK_CLIENT_KEY"):
        log.info("[TikTok] Posting... (not yet implemented)")

    if os.getenv("INSTAGRAM_ACCESS_TOKEN"):
        log.info("[Instagram] Posting... (not yet implemented)")

    local_clip.unlink(missing_ok=True)
    log.info("Done posting to platforms.")

# =============================================================================
# Title selection flow
# =============================================================================

async def wait_for_title(bot: discord.Client, channel: discord.TextChannel,
                         thread_message: discord.Message, suggestions: list[str],
                         meta: dict):
    """
    Wait up to 5 minutes for the owner to reply with a title choice.
    Falls back to suggestion 1 on timeout.
    """
    def check(msg: discord.Message):
        return (
            msg.channel.id == channel.id and
            msg.author.id != bot.user.id and
            msg.reference and
            msg.reference.message_id == thread_message.id
        )

    try:
        reply = await bot.wait_for("message", check=check, timeout=TITLE_TIMEOUT)
        content = reply.content.strip()

        if content in ("1", "2", "3"):
            title = suggestions[int(content) - 1]
            await reply.reply(f"Got it — using: **{title}**")
        else:
            title = content
            await reply.reply(f"Got it — using your title: **{title}**")

    except asyncio.TimeoutError:
        title = suggestions[0]
        await thread_message.reply(
            f"⏱️ No reply in 5 minutes — using suggestion 1:\n**{title}**"
        )

    log.info(f"Title selected: {title}")
    await post_to_platforms(meta, title)
    await channel.send(f"🎉 Posted! **{title}**")


# =============================================================================
# Discord bot
# =============================================================================

class ApprovalBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.reactions = True
        intents.guilds = True
        super().__init__(intents=intents)
        self.channel_id = int(settings.DISCORD_CHANNEL_ID)
        self.owner_id: int | None = None

    async def on_ready(self):
        log.info(f"Discord bot ready — logged in as {self.user}")

        channel = self.get_channel(self.channel_id)
        if channel:
            guild = channel.guild
            self.owner_id = guild.owner_id
            log.info(f"Watching #{channel.name} for reactions from owner {self.owner_id}")

            # Restore pending clips from disk
            pending_path = Path("output/discord_pending.jsonl")
            if pending_path.exists():
                for line in pending_path.read_text().strip().splitlines():
                    if line.strip():
                        try:
                            item = json.loads(line)
                            mid = int(item["message_id"])
                            PENDING[mid] = item.get("meta", {})
                            log.info(f"Restored pending clip: message {mid}")
                        except Exception as e:
                            log.warning(f"Could not restore pending: {e}")
        else:
            log.error(f"Channel {self.channel_id} not found — check DISCORD_CHANNEL_ID")

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        log.debug(f"Reaction: {payload.emoji} from {payload.user_id} on {payload.message_id}")

        if payload.channel_id != self.channel_id:
            return

        # Ignore bot's own reactions
        if payload.user_id == self.user.id:
            return

        # Only owner
        if self.owner_id and payload.user_id != self.owner_id:
            return

        emoji = str(payload.emoji)
        if emoji not in (APPROVE, REJECT):
            return

        message_id = payload.message_id
        log.info(f"Owner reacted {emoji} on message {message_id}")

        meta = PENDING.pop(message_id, {"title": "unknown clip"})
        channel = self.get_channel(self.channel_id)
        message = await channel.fetch_message(message_id)

        if emoji == REJECT:
            log.info(f"REJECTED clip")
            await message.reply("❌ Rejected — clip discarded.")
            await asyncio.sleep(3)
            await message.delete()
            return

        if emoji == APPROVE:
            log.info("Clip approved — generating title suggestions...")

            channel_name = meta.get("channel", "streamer")
            trigger_messages = meta.get("trigger_messages", [])
            suggestions = generate_title_suggestions(channel_name, trigger_messages)

            # Post title suggestions
            suggestion_text = (
                "✅ **Approved!** Choose a title:\n\n"
                f"**1.** {suggestions[0]}\n"
                f"**2.** {suggestions[1]}\n"
                f"**3.** {suggestions[2]}\n\n"
                "Reply to this message with **1**, **2**, **3** or type your own title.\n"
                "*(Auto-selects option 1 in 5 minutes)*"
            )
            suggestion_msg = await message.reply(suggestion_text)

            # Wait for title selection in background
            asyncio.create_task(
                wait_for_title(self, channel, suggestion_msg, suggestions, meta)
            )

        # Clean up pending file
        pending_path = Path("output/discord_pending.jsonl")
        if pending_path.exists():
            lines = [
                l for l in pending_path.read_text().strip().splitlines()
                if l.strip() and str(message_id) not in l
            ]
            pending_path.write_text("\n".join(lines) + "\n" if lines else "")


def main():
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        log.error("DISCORD_BOT_TOKEN not set in .env")
        return

    log.info("Starting Discord approval bot...")
    bot = ApprovalBot()
    bot.run(token, log_handler=None)


if __name__ == "__main__":
    main()
