"""
agents/scout.py — Agent 1: Scout + Rolling Buffer (Multi-Streamer)

Monitors multiple Kick streamers simultaneously.
Each streamer gets its own:
  - WebSocket chat connection
  - Rolling HLS buffer (30s)
  - Per-streamer hype threshold and cooldown
  - Per-streamer webcam coordinates

Configure in .env:
    KICK_CHANNELS=odablock,greg,xqc

    WEBCAM_odablock=0.7708,0.0611,0.2036,0.2778
    THRESHOLD_odablock=60
    COOLDOWN_odablock=120

    WEBCAM_greg=0.0,0.0,0.25,0.30
    THRESHOLD_greg=10
    COOLDOWN_greg=60

Run:
    cd /home/StreamerClipper/clipbot && python -m agents.scout
"""
import asyncio
import json
import logging
import time
import argparse
import base64
import signal
import subprocess
import shutil
from collections import deque, Counter
from datetime import datetime, timezone
from pathlib import Path
import websockets
import aiohttp
import cloudscraper
import requests

from core.models import ChatMessage, HypeMoment
from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scout")

KICK_WS_URL = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0-rc2&flash=false"
KICK_API_BASE = "https://kick.com/api/v2"
GITHUB_API = "https://api.github.com"
DISCORD_LOG_CHANNEL = "1482831221347057826"
DISCORD_SCOUT_LOG_CHANNEL = "1484635471891140618"

# Buffer settings
SEGMENT_DURATION = 10
MAX_SEGMENTS = 3
CLIP_AFTER_PEAK = 10
BUFFER_DIR = Path("/tmp/clipbot_hls")
CLIPS_DIR = Path("output/clips")


# =============================================================================
# Discord logging
# =============================================================================

def discord_log(message: str):
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        return
    try:
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_LOG_CHANNEL}/messages",
            headers={"Authorization": f"Bot {token}"},
            json={"content": message},
            timeout=5
        )
    except Exception:
        pass

def scout_log(message: str):
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        return
    try:
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_SCOUT_LOG_CHANNEL}/messages",
            headers={"Authorization": f"Bot {token}"},
            json={"content": message},
            timeout=5
        )
    except Exception:
        pass

# =============================================================================
# Kick API
# =============================================================================

def get_chatroom_id(channel_slug: str) -> tuple[int, str | None]:
    scraper = cloudscraper.create_scraper()
    url = f"{KICK_API_BASE}/channels/{channel_slug}"
    resp = scraper.get(url)
    if resp.status_code != 200:
        raise ValueError(f"Channel '{channel_slug}' not found (HTTP {resp.status_code})")
    info = resp.json()
    chatroom_id = info["chatroom"]["id"]
    stream_id = str(info["livestream"]["id"]) if info.get("livestream") else None
    return chatroom_id, stream_id


def get_hls_url(channel_slug: str) -> str | None:
    try:
        result = subprocess.run(
            ["streamlink", "--stream-url", f"https://kick.com/{channel_slug}", "best"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            url = result.stdout.strip()
            if url.startswith("http"):
                return url
        return None
    except Exception as e:
        log.warning(f"Failed to get HLS URL for {channel_slug}: {e}")
        return None


# =============================================================================
# GitHub helper
# =============================================================================

async def push_moment_to_github(moment: dict, session: aiohttp.ClientSession):
    if not settings.GITHUB_TOKEN or not settings.GITHUB_REPO:
        log.warning("GITHUB_TOKEN or GITHUB_REPO not set — skipping")
        return

    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    base = f"{GITHUB_API}/repos/{settings.GITHUB_REPO}/contents"

    # Upload clip file to GitHub first
    local_clip = moment.get("local_clip_path", "")
    if local_clip and Path(local_clip).exists():
        clip_path = Path(local_clip)
        log.info(f"Uploading clip: {clip_path.name} ({clip_path.stat().st_size/1024/1024:.1f}MB)")

        with open(clip_path, "rb") as f:
            clip_encoded = base64.b64encode(f.read()).decode("utf-8")

        clip_url = f"{base}/{local_clip}"
        clip_payload = {
            "message": f"[scout] clip: {clip_path.name}",
            "content": clip_encoded,
        }

        async with session.get(clip_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                clip_payload["sha"] = data["sha"]

        async with session.put(clip_url, headers=headers, json=clip_payload) as resp:
            if resp.status in (200, 201):
                log.info(f"Clip uploaded to GitHub")
                clip_path.unlink(missing_ok=True)
            else:
                body = await resp.text()
                log.error(f"Clip upload failed ({resp.status}): {body[:200]}")

    # Push moment to pending_moments.jsonl
    file_path = "output/pending_moments.jsonl"
    url = f"{base}/{file_path}"

    existing_content = ""
    sha = None
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            sha = data["sha"]
            existing_content = base64.b64decode(data["content"]).decode("utf-8")
        elif resp.status != 404:
            log.error(f"GitHub fetch failed: {resp.status}")
            return

    new_line = json.dumps(moment) + "\n"
    updated = existing_content + new_line
    encoded = base64.b64encode(updated.encode()).decode()

    payload = {
        "message": f"[scout] hype: {moment['channel']} @ {moment.get('peak_offset', 0):.0f}s",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            log.info("Pushed to GitHub — Clipper triggered")
        else:
            body = await resp.text()
            log.error(f"GitHub push failed ({resp.status}): {body[:200]}")


# =============================================================================
# Rolling HLS buffer
# =============================================================================

class RollingBuffer:
    def __init__(self, channel: str):
        self.channel = channel
        self.buffer_dir = BUFFER_DIR / channel
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self._process = None
        self._running = False
        self._hls_url: str | None = None
        # Clear any stale segments from previous session
        for seg in self.buffer_dir.glob("seg_*.ts"):
            seg.unlink(missing_ok=True)
        log.debug(f"[{channel}] Buffer directory cleared on init")

    def _clean_old_segments(self):
        segments = sorted(self.buffer_dir.glob("seg_*.ts"))
        if len(segments) > MAX_SEGMENTS:
            for seg in segments[:len(segments) - MAX_SEGMENTS]:
                seg.unlink(missing_ok=True)

    def get_buffered_segments(self) -> list[Path]:
        now = time.time()
        segments = sorted(self.buffer_dir.glob("seg_*.ts"))
        # Keep only segments written in the last 60 seconds and over 1MB
        return [s for s in segments
                if s.stat().st_size > 1_000_000
                and (now - s.stat().st_mtime) < 60]

    def extract_clip(self, timestamp: str) -> Path | None:
        segments = self.get_buffered_segments()
        if not segments:
            log.warning(f"[{self.channel}] Buffer empty")
            return None

        log.info(f"[{self.channel}] Stitching {len(segments)} segments (~{len(segments)*SEGMENT_DURATION}s)")
        CLIPS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = CLIPS_DIR / f"{self.channel}_{timestamp}_raw.ts"
        concat_file = self.buffer_dir / "concat.txt"

        with open(concat_file, "w") as f:
            for seg in segments:
                f.write(f"file '{seg.absolute()}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(concat_file),
            "-c", "copy",
            str(output_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        concat_file.unlink(missing_ok=True)

        if result.returncode != 0 or not output_path.exists():
            log.error(f"[{self.channel}] Stitch failed: {result.stderr[-200:]}")
            return None

        size = output_path.stat().st_size
        if size < 50_000:
            output_path.unlink(missing_ok=True)
            return None

        log.info(f"[{self.channel}] Buffer stitched: {output_path.name} ({size/1024/1024:.1f}MB)")
        # Detailed clip info for scout-log
        total_duration = len(segments) * SEGMENT_DURATION
        now = time.time()
        seg_details = "\n".join(
            f"  • {s.name} | {s.stat().st_size/1024/1024:.1f}MB | "
            f"recorded ~{datetime.fromtimestamp(s.stat().st_mtime).strftime('%H:%M:%S')} UTC"
            for s in segments
        )
        scout_log(
            f"🎬 **#{self.channel}** clip stitched\n"
            f"📁 File: `{output_path.name}`\n"
            f"📏 Total length: `{total_duration}s`\n"
            f"💾 Total size: `{size/1024/1024:.1f}MB`\n"
            f"📦 Segments:\n{seg_details}"
        )
        return output_path

    async def start(self, hls_url: str):
        self._hls_url = hls_url
        self._running = True
        asyncio.create_task(self._download_loop())

    async def _download_loop(self):
        segment_pattern = str(self.buffer_dir / "seg_%06d.ts")
        cmd = [
            "ffmpeg", "-y",
            "-live_start_index", "-1",
            "-i", self._hls_url,
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(SEGMENT_DURATION),
            segment_pattern
        ]

        try:
            self._process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            log.info(f"[{self.channel}] Buffer started (PID {self._process.pid})")
            scout_log(f"✅ **#{self.channel}** buffer started")

        except Exception as e:
            log.error(f"[{self.channel}] Failed to start buffer: {e}")
            return

        while self._running:
            await asyncio.sleep(SEGMENT_DURATION)
            self._clean_old_segments()
            segs = self.get_buffered_segments()
            if segs:
                log.debug(f"[{self.channel}] Buffer: {len(segs)} segments")

            if self._process and self._process.returncode is not None:
                log.warning(f"[{self.channel}] Buffer process died — refreshing HLS URL")
                scout_log(f"⚠️ **#{self.channel}** buffer died — refreshing HLS URL...")
                new_url = await asyncio.get_running_loop().run_in_executor(
                    None, get_hls_url, self.channel
                )
                if new_url:
                    self._hls_url = new_url
                    self._process = await asyncio.create_subprocess_exec(
                        *["ffmpeg", "-y", "-live_start_index", "-1",
                          "-i", new_url, "-c", "copy",
                          "-f", "segment", "-segment_time", str(SEGMENT_DURATION),
                          segment_pattern],
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                else:
                    await asyncio.sleep(30)

    def stop(self):
        self._running = False
        if self._process:
            try:
                self._process.terminate()
            except Exception:
                pass
        if self.buffer_dir.exists():
            shutil.rmtree(self.buffer_dir)
        log.info(f"[{self.channel}] Buffer stopped")


# =============================================================================
# Per-streamer hype detector
# =============================================================================

class HypeDetector:
    SPAM_KEYWORDS = {
        "weeat", "weet", "!giveaway", "!enter", "!join", "giveaway",
        "!claim", "!free", "!drop",
    }
    SPAM_DOMINANCE_THRESHOLD = 0.6

    def __init__(self, channel: str):
        self.channel = channel
        self.window = settings.HYPE_WINDOW_SECONDS
        self.threshold = settings.get_threshold(channel)
        self.cooldown = settings.get_cooldown(channel)
        self._timestamps: deque[datetime] = deque()
        self._last_trigger: datetime | None = None
        self._recent_messages: deque[str] = deque(maxlen=10)
        log.info(f"[{channel}] Thresholds: {self.threshold} msgs/{self.window}s, cooldown={self.cooldown}s")

    def push(self, msg: ChatMessage) -> float:
        now = msg.timestamp
        self._timestamps.append(now)
        self._recent_messages.append(f"{msg.username}: {msg.content}")
        cutoff = now.timestamp() - self.window
        while self._timestamps and self._timestamps[0].timestamp() < cutoff:
            self._timestamps.popleft()
        return len(self._timestamps) / self.window

    def is_spam(self) -> bool:
        if not self._recent_messages:
            return False
        messages = list(self._recent_messages)
        total = len(messages)
        words = []
        for m in messages:
            content = m.split(": ", 1)[-1].strip().lower()
            first_word = content.split()[0] if content.split() else ""
            words.append(first_word)
        for word in words:
            if word in self.SPAM_KEYWORDS:
                count = words.count(word)
                if count / total >= self.SPAM_DOMINANCE_THRESHOLD:
                    return True
        top_word, top_count = Counter(words).most_common(1)[0]
        if top_word and top_count / total >= self.SPAM_DOMINANCE_THRESHOLD:
            return True
        return False

    def should_trigger(self, rate: float, now: datetime) -> bool:
        if rate < self.threshold / self.window:
            return False
        if self._last_trigger is None:
            return not self.is_spam()
        if (now - self._last_trigger).total_seconds() < self.cooldown:
            return False
        return not self.is_spam()

    def trigger(self, channel: str, stream_id: str, offset: float, rate: float) -> HypeMoment:
        now = datetime.now(timezone.utc)
        self._last_trigger = now
        return HypeMoment(
            channel=channel,
            stream_id=stream_id,
            peak_offset=offset,
            peak_time=now,
            message_rate=rate,
            trigger_messages=list(self._recent_messages),
        )


# =============================================================================
# Per-channel scout
# =============================================================================

class KickChatScout:
    def __init__(self, channel_slug: str):
        self.channel_slug = channel_slug
        self.detector = HypeDetector(channel_slug)
        self.buffer = RollingBuffer(channel_slug)
        self._stream_start: datetime | None = None
        self._stream_id: str | None = None
        self._moments: list[HypeMoment] = []
        self._building_alerted = False

        Path(settings.LOGS_DIR).mkdir(parents=True, exist_ok=True)
        self._local_log = Path(settings.LOGS_DIR) / f"{channel_slug}_moments.jsonl"

    def _offset(self, now: datetime) -> float:
        if self._stream_start is None:
            return 0.0
        return (now - self._stream_start).total_seconds()

    async def run(self):
        log.info(f"[{self.channel_slug}] Fetching channel info...")
        loop = asyncio.get_running_loop()
        try:
            chatroom_id, stream_id = await loop.run_in_executor(
                None, get_chatroom_id, self.channel_slug
            )
        except ValueError as e:
            log.error(str(e))
            return

        self._stream_id = stream_id or "unknown"
        self._stream_start = datetime.now(timezone.utc)

        if stream_id:
            log.info(f"[{self.channel_slug}] LIVE — stream ID: {stream_id}")
            hls_url = await loop.run_in_executor(None, get_hls_url, self.channel_slug)
            if hls_url:
                await self.buffer.start(hls_url)
                discord_log(f"🟢 **#{self.channel_slug}** connected — rolling buffer active")
            else:
                discord_log(f"🟡 **#{self.channel_slug}** connected — buffer unavailable")
        else:
            log.warning(f"[{self.channel_slug}] OFFLINE — waiting...")
            discord_log(f"⚪ **#{self.channel_slug}** is offline — waiting...")

        async with websockets.connect(KICK_WS_URL) as ws:
            await ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }))
            log.info(f"[{self.channel_slug}] Listening...")

            async with aiohttp.ClientSession() as session:
                async for raw in ws:
                    await self._handle(raw, session)

    async def _handle(self, raw: str, session: aiohttp.ClientSession):
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            return

        if envelope.get("event") != "App\\Events\\ChatMessageEvent":
            return

        try:
            data = json.loads(envelope.get("data", "{}"))
        except json.JSONDecodeError:
            return

        now = datetime.now(timezone.utc)
        msg = ChatMessage(
            channel=self.channel_slug,
            username=data.get("sender", {}).get("username", "unknown"),
            content=data.get("content", ""),
            timestamp=now,
            stream_offset=self._offset(now),
        )

        rate = self.detector.push(msg)
        msgs_in_window = int(rate * self.detector.window)
        build_threshold = int(self.detector.threshold * 0.67)

        if msgs_in_window >= build_threshold and not self._building_alerted:
            self._building_alerted = True
            buf_segments = len(self.buffer.get_buffered_segments())
            discord_log(
                f"⚡ **Hype building** on #{self.channel_slug} — "
                f"`{msgs_in_window}/{self.detector.threshold}` msgs "
                f"| Buffer: `{buf_segments * SEGMENT_DURATION}s` ready"
            )
        elif msgs_in_window < build_threshold // 2:
            self._building_alerted = False

        # Manual trigger keywords
        MANUAL_TRIGGERS = ["go for it mr streamer", "!clip", "clipbot", "lock in mr streamer"]
        content_lower = msg.content.lower()
        manual_triggered = any(kw in content_lower for kw in MANUAL_TRIGGERS)

        if manual_triggered or self.detector.should_trigger(rate, now):
            # Lock cooldown IMMEDIATELY before any async sleep
            self.detector._last_trigger = datetime.now(timezone.utc)
            offset = self._offset(now)
            moment = self.detector.trigger(
                channel=self.channel_slug,
                stream_id=self._stream_id or "unknown",
                offset=offset,
                rate=rate * self.detector.window,
            )
            self._moments.append(moment)
            self._building_alerted = False
            trigger_time = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            top_msgs = moment.trigger_messages[-3:]
            top_msgs_str = "\n".join(f"    → {m}" for m in top_msgs)
            discord_msgs = "\n".join(f"> `{trigger_time}` {m}" for m in top_msgs)
            log.info(
                f"\n{'='*50}\n"
                f"  [{self.channel_slug}] HYPE TRIGGERED\n"
                f"  Time  : {trigger_time}\n"
                f"  Rate  : {moment.message_rate:.0f} msgs/{self.detector.window}s\n"
                f"  Offset: {offset:.1f}s\n"
                f"  Msgs  :\n{top_msgs_str}\n"
                f"{'='*50}"
            )
            buf_segments = len(self.buffer.get_buffered_segments())
            discord_log(
                f"🔥 **HYPE TRIGGERED** on #{self.channel_slug}\n"
                f"🕐 `{trigger_time}` | Rate: `{moment.message_rate:.0f}` msgs\n"
                f"Buffer: `{buf_segments * SEGMENT_DURATION}s` captured\n"
                f"💬 Last 3:\n{discord_msgs}"
            )

            await asyncio.sleep(CLIP_AFTER_PEAK)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            clip_path = self.buffer.extract_clip(timestamp)

            moment_dict = moment.to_dict()
            if clip_path:
                moment_dict["local_clip_path"] = str(clip_path)
                discord_log(f"✅ Clip ready from buffer — processing started!")
            else:
                discord_log(f"⚠️ Buffer empty — falling back to live recording")
                scout_log(f"⚠️ **#{self.channel_slug}** buffer empty on trigger — live recording fallback")
            with open(self._local_log, "a") as f:
                f.write(json.dumps(moment_dict) + "\n")

            await push_moment_to_github(moment_dict, session)

    def cleanup(self):
        self.buffer.stop()


# =============================================================================
# Main — run all channels concurrently
# =============================================================================

async def run_channel(channel: str):
    """Run scout for a single channel with auto-reconnect."""
    while True:
        scout = KickChatScout(channel)
        try:
            await scout.run()
        except asyncio.CancelledError:
            scout.cleanup()
            break
        except Exception as e:
            log.warning(f"[{channel}] Scout crashed: {e} — reconnecting in 30s...")
            discord_log(f"⚠️ **#{channel} scout crashed** — reconnecting in 30s...\n`{e}`")
            scout_log(f"🟢 **#{self.channel}** connected — buffer active")
            scout.cleanup()

        await asyncio.sleep(30)


async def main(channels: list[str], debug: bool = False):
    if debug:
        logging.getLogger("scout").setLevel(logging.DEBUG)

    if not channels:
        log.error("No channels configured! Set KICK_CHANNELS in .env")
        return

    log.info(f"Starting scouts for: {', '.join(channels)}")

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, loop.stop)

    # Run all channels concurrently
    await asyncio.gather(*[run_channel(ch) for ch in channels])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-streamer Kick scout + rolling buffer")
    parser.add_argument("--channel", help="Single channel (overrides KICK_CHANNELS in .env)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    channels = [args.channel] if args.channel else settings.KICK_CHANNELS
    asyncio.run(main(channels, args.debug))
