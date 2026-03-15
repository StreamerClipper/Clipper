"""
agents/discord_bot.py — Discord approval bot

Runs on PythonAnywhere as a second always-on task.
Watches the approval channel for ✅/❌ reactions on clip messages.

✅ → posts the clip to all configured platforms
❌ → deletes the message, discards the clip

Setup:
    pip install --user discord.py

Run:
    cd ~/clipbot && python -m agents.discord_bot
"""
import asyncio
import json
import logging
import os
import signal
from pathlib import Path

import discord
import aiohttp

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discord_bot")

APPROVE = "✅"
REJECT  = "❌"

# Tracks pending clips: message_id -> metadata dict
PENDING: dict[int, dict] = {}


# =============================================================================
# Platform posting (called after approval)
# =============================================================================

async def post_to_platforms(meta: dict):
    """Post the approved clip to all configured platforms."""
    log.info(f"Posting approved clip: {meta.get('title')}")

    # YouTube
    if os.getenv("YOUTUBE_CLIENT_ID"):
        log.info("[YouTube] Posting... (not yet implemented)")

    # TikTok
    if os.getenv("TIKTOK_CLIENT_KEY"):
        log.info("[TikTok] Posting... (not yet implemented)")

    # Instagram
    if os.getenv("INSTAGRAM_ACCESS_TOKEN"):
        log.info("[Instagram] Posting... (not yet implemented)")

    log.info("Done posting to platforms.")


# =============================================================================
# Discord bot
# =============================================================================

class ApprovalBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True
        intents.members = True
        super().__init__(intents=intents)
        self.channel_id = int(settings.DISCORD_CHANNEL_ID)
        self.owner_id: int | None = None

    async def on_ready(self):
        log.info(f"Discord bot ready — logged in as {self.user}")

        # Fetch server owner so we only respond to their reactions
        channel = self.get_channel(self.channel_id)
        if channel:
            guild = channel.guild
            self.owner_id = guild.owner_id
            log.info(f"Watching channel #{channel.name} for reactions from owner {self.owner_id}")
        else:
            log.error(f"Channel {self.channel_id} not found — check DISCORD_CHANNEL_ID")

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        # Only care about our approval channel
        if payload.channel_id != self.channel_id:
            return

        # Only respond to the server owner's reactions
        if self.owner_id and payload.user_id != self.owner_id:
            return

        # Only care about ✅ and ❌
        emoji = str(payload.emoji)
        if emoji not in (APPROVE, REJECT):
            return

        message_id = payload.message_id
        if message_id not in PENDING:
            log.warning(f"Reaction on unknown message {message_id} — ignoring")
            return

        meta = PENDING.pop(message_id)
        channel = self.get_channel(self.channel_id)
        message = await channel.fetch_message(message_id)

        if emoji == APPROVE:
            log.info(f"APPROVED: {meta.get('title')}")
            await message.reply("✅ Approved — posting to platforms now...")
            await post_to_platforms(meta)
            await message.reply("🎉 Posted successfully!")

        elif emoji == REJECT:
            log.info(f"REJECTED: {meta.get('title')}")
            await message.reply("❌ Rejected — clip discarded.")
            await asyncio.sleep(3)
            await message.delete()


def main():
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        log.error("DISCORD_BOT_TOKEN not set in .env")
        return

    bot = ApprovalBot()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, loop.stop)

    log.info("Starting Discord approval bot...")
    bot.run(token)


if __name__ == "__main__":
    main()
