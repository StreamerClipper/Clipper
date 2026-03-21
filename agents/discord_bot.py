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
import sys

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

SOAP_INPUT_CHANNEL_ID = 1484842601617293394   # you paste URLs here
SOAP_LOG_CHANNEL_ID   = 1484834748181385256   # pipeline status / progress
SOAP_CLIPS_CHANNEL_ID = 1484834736257106020   # clip approvals (✅ / ❌)

TITLE_TIMEOUT = 86400  # 24h

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
    """Post the approved clip to all configured platforms with the chosen title."""
    log.info(f"Posting to platforms: {title}")

    clip_path_str = meta.get("clip_path", "")
    hashtags = meta.get("hashtags", ["#kick", "#clips", "#gaming"])
    description = meta.get("description", "")

    # YouTube Shorts
    if os.getenv("YOUTUBE_CLIENT_ID"):
        try:
            from agents.youtube_upload import upload_to_youtube
            from pathlib import Path
            video_id = upload_to_youtube(
                Path(clip_path_str),
                title,
                description,
                hashtags,
            )
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
        await thread_message.reply(
            "⏱️ No reply received — clip is saved but not posted. React ✅ again to retry."
        )
        return

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
                            PENDING[mid] = item  # store full record including clip_path
                            log.info(f"Restored pending clip: message {mid}")
                        except Exception as e:
                            log.warning(f"Could not restore pending: {e}")
        else:
            log.error(f"Channel {self.channel_id} not found — check DISCORD_CHANNEL_ID")

    async def on_message(self, message: discord.Message):
        """Handle !hype and !soap commands."""
        if message.author.bot:
            return

        content = message.content.strip()

        # ── Soap commands (separate input channel) ────────────────────────────
        if message.channel.id == SOAP_INPUT_CHANNEL_ID:
            if content.startswith("!soap clip "):
                url = content[len("!soap clip "):].strip()
                if not url.startswith("http"):
                    await message.channel.send("❌ Usage: `!soap clip https://www.youtube.com/watch?v=...`")
                    return
                await message.channel.send(
                    f"🔍 Analysing Most Replayed data for:\n`{url}`\n⏳ This takes ~30 seconds..."
                )
                import subprocess
                subprocess.Popen(
                    [sys.executable, "-m", "agents.soap_scout", "--url", url],
                    cwd=Path(".").resolve(),
                )
                await message.channel.send(
                    "📋 Episode queued — clips will appear in <#1484834736257106020> for approval.\n"
                    "*(Processing takes 2–5 minutes in GitHub Actions)*"
                )
                return

            if content == "ret":
                pending_file = Path("output/soap_pending.jsonl")
                processed_file = Path("output/soap_processed.jsonl")

                # Find the last URL — check pending first, then processed
                last_url = None

                if processed_file.exists():
                    lines = [l for l in processed_file.read_text().strip().splitlines() if l.strip()]
                    if lines:
                        last_url = json.loads(lines[-1]).get("url")

                if not last_url and pending_file.exists():
                    lines = [l for l in pending_file.read_text().strip().splitlines() if l.strip()]
                    if lines:
                        last_url = json.loads(lines[-1]).get("url")

                if not last_url:
                    await message.channel.send("❌ No previous URL found to retry.")
                    return

                await message.channel.send(f"🔄 Retrying last URL:\n`{last_url}`\n⏳ Queuing...")
                import subprocess
                subprocess.Popen(
                    [sys.executable, "-m", "agents.soap_scout", "--url", last_url],
                    cwd=Path(".").resolve(),
                )
                await message.channel.send(
                    "📋 Re-queued — clips will appear in <#1484834736257106020> once processed."
                )
                return

            if content == "!soap status":
                pending_file = Path("output/soap_pending.jsonl")
                disc_file    = Path("output/soap_discord_pending.jsonl")
                pending_n  = len([l for l in pending_file.read_text().splitlines() if l.strip()]) if pending_file.exists() else 0
                approval_n = len([l for l in disc_file.read_text().splitlines() if l.strip()]) if disc_file.exists() else 0
                await message.channel.send(
                    f"📺 **Soap Shorts Status**\n"
                    f"`{pending_n}` episode(s) queued for clipping\n"
                    f"`{approval_n}` clip(s) awaiting approval in <#1484834736257106020>"
                )
                return

            if content in ("!soap", "!soap help"):
                await message.channel.send(
                    "**Soap Shorts Commands:**\n"
                    "`!soap clip <youtube-url>` — queue an episode for clipping\n"
                    "`!soap status` — show pending jobs\n\n"
                    "Clips appear in <#1484834736257106020> with ✅/❌ buttons.\n"
                    "✅ uploads to YouTube Shorts · ❌ discards"
                )
                return

            return  # ignore anything else in the soap input channel
        # ── End soap commands ─────────────────────────────────────────────────

        # Existing Kick bot channel guard
        if message.channel.id != self.channel_id:
            return

        # !hype status — show current settings
        if content == "!hype status":
            env_path = Path(".env")
            settings_map = {}
            for line in env_path.read_text().splitlines():
                for key in ["HYPE_WINDOW_SECONDS", "HYPE_THRESHOLD", "HYPE_COOLDOWN_SECONDS"]:
                    if line.startswith(f"{key}="):
                        settings_map[key] = line.split("=", 1)[1].strip()
            await message.channel.send(
                f"⚙️ **Hype Settings:**\n"
                f"`HYPE_WINDOW_SECONDS` = {settings_map.get('HYPE_WINDOW_SECONDS', '?')}\n"
                f"`HYPE_THRESHOLD` = {settings_map.get('HYPE_THRESHOLD', '?')}\n"
                f"`HYPE_COOLDOWN_SECONDS` = {settings_map.get('HYPE_COOLDOWN_SECONDS', '?')}"
            )
            return

        # !hype set KEY VALUE
        if content.startswith("!hype set "):
            parts = content.split()
            if len(parts) != 4:
                await message.channel.send("Usage: `!hype set HYPE_THRESHOLD 45`")
                return

            key = parts[2].upper()
            value = parts[3]

            if key not in ["HYPE_WINDOW_SECONDS", "HYPE_THRESHOLD", "HYPE_COOLDOWN_SECONDS"]:
                await message.channel.send(
                    f"❌ Unknown key `{key}`\n"
                    f"Valid keys: `HYPE_WINDOW_SECONDS`, `HYPE_THRESHOLD`, `HYPE_COOLDOWN_SECONDS`"
                )
                return

            if not value.isdigit():
                await message.channel.send("❌ Value must be a number")
                return

            env_path = Path(".env")
            lines = env_path.read_text().splitlines()
            updated = False
            new_lines = []
            for line in lines:
                if line.startswith(f"{key}="):
                    new_lines.append(f"{key}={value}")
                    updated = True
                else:
                    new_lines.append(line)
            if not updated:
                new_lines.append(f"{key}={value}")
            env_path.write_text("\n".join(new_lines) + "\n")

            await message.channel.send(
                f"✅ Updated `{key}` = `{value}`\n"
                f"⚠️ Restart the Scout task for changes to take effect."
            )
            return

        # !hype help
        if content == "!hype":
            await message.channel.send(
                "**Hype Commands:**\n"
                "`!hype status` — show current settings\n"
                "`!hype set HYPE_THRESHOLD 45` — change threshold\n"
                "`!hype set HYPE_WINDOW_SECONDS 10` — change window\n"
                "`!hype set HYPE_COOLDOWN_SECONDS 120` — change cooldown"
            )

        # !restart — restart the discord bot process
        if content == "!restart":
            await message.channel.send("🔄 Restarting bot...")
            import os
            os.execv(sys.executable, [sys.executable, "-m", "agents.discord_bot"])
            return

        # !restart scout — write flag file to restart scout
        if content == "!restart scout":
            Path("/tmp/restart_scout.flag").touch()
            await message.channel.send(
                "⚠️ Scout restart flag set — but you need to manually restart "
                "the Scout always-on task on PythonAnywhere.\n"
                "Go to **Tasks** and click **Restart**."
            )
            return

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        log.debug(f"Reaction: {payload.emoji} from {payload.user_id} on {payload.message_id}")

        # ── Soap clip approval (separate clips channel) ───────────────────────
        if payload.channel_id == SOAP_CLIPS_CHANNEL_ID:
            if payload.user_id == self.user.id:
                return
            if self.owner_id and payload.user_id != self.owner_id:
                return

            emoji = str(payload.emoji)
            if emoji not in (APPROVE, REJECT):
                return

            message_id  = payload.message_id
            soap_record = None
            soap_path   = Path("output/soap_discord_pending.jsonl")

            if soap_path.exists():
                for line in soap_path.read_text().strip().splitlines():
                    try:
                        item = json.loads(line)
                        if str(item.get("message_id")) == str(message_id):
                            soap_record = item
                            break
                    except Exception:
                        pass

            if not soap_record:
                return

            clips_channel = self.get_channel(SOAP_CLIPS_CHANNEL_ID)
            log_channel   = self.get_channel(SOAP_LOG_CHANNEL_ID)
            message_obj   = await clips_channel.fetch_message(message_id)

            if emoji == REJECT:
                Path(soap_record.get("clip_path", "")).unlink(missing_ok=True)
                await message_obj.reply("❌ Soap clip discarded.")
                await asyncio.sleep(3)
                await message_obj.delete()
                if log_channel:
                    await log_channel.send(f"🗑️ Soap clip {soap_record.get('clip_index', 0)+1} discarded.")

            elif emoji == APPROVE:
                await message_obj.reply("⏳ Uploading to YouTube Shorts...")
                try:
                    from agents.soap_uploader import handle_approval
                    yt_url = await asyncio.get_event_loop().run_in_executor(
                        None, handle_approval, soap_record
                    )
                    if yt_url:
                        await clips_channel.send(f"🎬 **Soap Short uploaded!** {yt_url}")
                        if log_channel:
                            await log_channel.send(f"✅ Clip {soap_record.get('clip_index', 0)+1} uploaded: {yt_url}")
                    else:
                        await clips_channel.send("❌ YouTube upload failed — check logs.")
                except Exception as e:
                    await clips_channel.send(f"❌ Upload error: `{e}`")

            # Remove from soap pending file
            remaining = [
                l for l in soap_path.read_text().strip().splitlines()
                if l.strip() and str(message_id) not in l
            ]
            soap_path.write_text("\n".join(remaining) + "\n" if remaining else "")
            return  # do NOT fall through to Kick clip handling
        # ── End soap reaction block ───────────────────────────────────────────

        # Existing Kick clip handling below — unchanged
        if payload.channel_id != self.channel_id:
            return
        if payload.user_id == self.user.id:
            return
        if self.owner_id and payload.user_id != self.owner_id:
            return

        emoji = str(payload.emoji)
        if emoji not in (APPROVE, REJECT):
            return

        message_id = payload.message_id
        log.info(f"Owner reacted {emoji} on message {message_id}")

        record = PENDING.pop(message_id, None)
        log.info(f"DEBUG record keys: {list(record.keys()) if record else 'NONE'}")

        if not record:
            pending_path = Path("output/discord_pending.jsonl")
            if pending_path.exists():
                for line in pending_path.read_text().strip().splitlines():
                    if line.strip():
                        try:
                            item = json.loads(line)
                            if str(item.get("message_id")) == str(message_id):
                                record = item
                                log.info(f"Found record in pending file: {item.get('clip_path')}")
                                break
                        except Exception:
                            pass

        record = record or {}
        log.info(f"DEBUG clip_path: {record.get('clip_path', 'MISSING')}")

        meta = record.get("meta", {})
        meta["clip_path"] = record.get("clip_path", "")
        meta["hashtags"] = meta.get("hashtags", ["#kick", "#clips", "#gaming"])
        meta["description"] = meta.get("description", "")
        moment_data = record.get("moment", {})
        meta["channel"] = meta.get("channel") or moment_data.get("channel", "streamer")
        meta["trigger_messages"] = meta.get("trigger_messages") or moment_data.get("trigger_messages", [])

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
            suggestion_text = (
                "✅ **Approved!** Choose a title:\n\n"
                f"**1.** {suggestions[0]}\n"
                f"**2.** {suggestions[1]}\n"
                f"**3.** {suggestions[2]}\n\n"
                "Reply to this message with **1**, **2**, **3** or type your own title.\n"
                "*(Auto-selects option 1 in 5 minutes)*"
            )
            suggestion_msg = await message.reply(suggestion_text)
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
    # Prevent multiple instances
    pid_file = Path("/tmp/discord_bot.pid")
    if pid_file.exists():
        old_pid = pid_file.read_text().strip()
        try:
            os.kill(int(old_pid), 0)  # check if process is still running
            log.error(f"Bot already running (PID {old_pid}) — exiting")
            return
        except (OSError, ValueError):
            pass  # process is dead, continue
    pid_file.write_text(str(os.getpid()))

    token = settings.DISCORD_BOT_TOKEN
    if not token:
        log.error("DISCORD_BOT_TOKEN not set in .env")
        return
    log.info("Starting Discord approval bot...")
    try:
        bot = ApprovalBot()
        bot.run(token, log_handler=None)
    finally:
        pid_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
