"""
agents/scout.py — Agent 1: Scout + Recorder (combined)

Runs as a single always-on task on PythonAnywhere.
Two async tasks run concurrently:
  1. Scout — monitors Kick chat, detects hype spikes
  2. Recorder — continuously records stream in 10s segments (rolling 5min buffer)

When hype is detected, the exact timestamp is used to extract a clip
from the local buffer — no spin-up delay, no missed moments.

Run:
    cd /home/StreamerClipper/clipbot && python -m agents.scout --channel odablock
"""
import asyncio
import json
import logging
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

# Recorder settings
BUFFER_MINUTES = 5
SEGMENT_SECONDS = 10
MAX_SEGMENTS = (BUFFER_MINUTES * 60) // SEGMENT_SECONDS  # 30 segments
CLIP_DURATION = 30
CLIP_PADDING_BEFORE = 20


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


# =============================================================================
# GitHub helper
# =============================================================================

async def push_moment_to_github(moment: HypeMoment, session: aiohttp.ClientSession):
    if not settings.GITHUB_TOKEN or not settings.GITHUB_REPO:
        log.warning("GITHUB_TOKEN or GITHUB_REPO not set — skipping GitHub push")
        return

    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    file_path = "output/pending_moments.jsonl"
    url = f"{GITHUB_API}/repos/{settings.GITHUB_REPO}/contents/{file_path}"

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

    new_line = json.dumps(moment.to_dict()) + "\n"
    updated_content = existing_content + new_line
    encoded = base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"[scout] hype moment: {moment.channel} @ {moment.peak_offset:.0f}s",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            log.info("Pushed moment to GitHub -> triggers Clipper workflow")
        else:
            body = await resp.text()
            log.error(f"GitHub push failed ({resp.status}): {body}")


# =============================================================================
# Rolling buffer recorder
# =============================================================================

class StreamRecorder:
    def __init__(self, channel: str):
        self.channel = channel
        self.buffer_dir = Path(f"/tmp/clipbot_buffer/{channel}")
        self.clips_dir = Path("output/clips")
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self.clips_dir.mkdir(parents=True, exist_ok=True)
        self._segment_index = 0
        self._running = False
        self._stream_start: datetime | None = None

    def _clean_buffer(self):
        segments = sorted(self.buffer_dir.glob("*.ts"))
        if len(segments) > MAX_SEGMENTS:
            for f in segments[:len(segments) - MAX_SEGMENTS]:
                f.unlink(missing_ok=True)

    def extract_clip(self, peak_offset: float, timestamp: str) -> Path | None:
        """Extract a clip from the buffer around the hype peak offset."""
        start_offset = max(0, peak_offset - CLIP_PADDING_BEFORE)
        start_seg = int(start_offset // SEGMENT_SECONDS)
        end_seg = int((peak_offset + (CLIP_DURATION - CLIP_PADDING_BEFORE)) // SEGMENT_SECONDS) + 1

        segments = sorted(self.buffer_dir.glob("*.ts"))
        if not segments:
            log.warning("Buffer empty — no segments to clip from")
            return None

        # Match by segment index
        target = []
        for seg in segments:
            try:
                idx = int(seg.stem)
                if start_seg <= idx <= end_seg:
                    target.append(seg)
            except ValueError:
                continue

        if not target:
            log.warning(f"No segments for offset {peak_offset:.0f}s — using latest segments")
            target = segments[-3:]  # fallback: last 30s

        output_path = self.clips_dir / f"{self.channel}_{timestamp}_raw.mp4"
        concat_file = Path(f"/tmp/concat_{timestamp}.txt")

        with open(concat_file, "w") as f:
            for seg in sorted(target):
                f.write(f"file '{seg}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(concat_file),
            "-t", str(CLIP_DURATION),
            "-c:v", "libx264", "-c:a", "aac", "-preset", "fast",
            str(output_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        concat_file.unlink(missing_ok=True)

        if result.returncode != 0 or not output_path.exists():
            log.error(f"Clip extraction failed: {result.stderr[-300:]}")
            return None

        size = output_path.stat().st_size
        if size < 50_000:
            output_path.unlink(missing_ok=True)
            log.error(f"Extracted clip too small ({size} bytes)")
            return None

        log.info(f"Clip extracted from buffer: {output_path} ({size/1024/1024:.1f}MB)")
        return output_path

    def cleanup(self):
        """Delete buffer on shutdown."""
        if self.buffer_dir.exists():
            shutil.rmtree(self.buffer_dir)
            log.info("Buffer cleaned up")

    async def run(self):
        """Continuously record stream in segments."""
        self._running = True
        self._stream_start = datetime.now(timezone.utc)
        log.info(f"Recorder started for #{self.channel}")

        while self._running:
            seg_path = self.buffer_dir / f"{self._segment_index:04d}.ts"
            cmd = [
                "streamlink",
                "--stdout",
                "--hls-duration", str(SEGMENT_SECONDS),
                "--stream-timeout", str(SEGMENT_SECONDS + 10),
                f"https://kick.com/{self.channel}",
                "best",
            ]
            try:
                with open(seg_path, "wb") as f:
                    subprocess.run(
                        cmd, stdout=f, stderr=subprocess.DEVNULL,
                        timeout=SEGMENT_SECONDS + 15
                    )
            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                log.warning(f"Recorder segment error: {e}")
                await asyncio.sleep(5)
                continue

            if seg_path.exists() and seg_path.stat().st_size > 10_000:
                self._clean_buffer()
                self._segment_index += 1
            else:
                seg_path.unlink(missing_ok=True)
                log.debug("Empty segment — stream offline?")
                await asyncio.sleep(10)

            await asyncio.sleep(0)  # yield to event loop


# =============================================================================
# Hype detector
# =============================================================================

class HypeDetector:
    SPAM_KEYWORDS = {
        "weeat", "weet", "!giveaway", "!enter", "!join", "giveaway",
        "!claim", "!free", "!drop",
    }
    SPAM_DOMINANCE_THRESHOLD = 0.6

    def __init__(self):
        self.window = settings.HYPE_WINDOW_SECONDS
        self.threshold = settings.HYPE_THRESHOLD
        self.cooldown = settings.HYPE_COOLDOWN_SECONDS
        self._timestamps: deque[datetime] = deque()
        self._last_trigger: datetime | None = None
        self._recent_messages: deque[str] = deque(maxlen=10)

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
                    log.info(f"Spam filter: '{word}' in {count}/{total} — skipping")
                    return True
        top_word, top_count = Counter(words).most_common(1)[0]
        if top_word and top_count / total >= self.SPAM_DOMINANCE_THRESHOLD:
            log.info(f"Spam filter: '{top_word}' dominates {top_count}/{total} — skipping")
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
# Combined Scout + Recorder
# =============================================================================

class KickChatScout:
    def __init__(self, channel_slug: str):
        self.channel_slug = channel_slug
        self.detector = HypeDetector()
        self.recorder = StreamRecorder(channel_slug)
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
        log.info(f"Fetching channel info for '{self.channel_slug}'...")
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
            log.info(f"Channel is LIVE - stream ID: {stream_id}")
            discord_log(f"🟢 **Scout+Recorder connected** to #{self.channel_slug}")
        else:
            log.warning("Channel is OFFLINE - listening, waiting for stream...")
            discord_log(f"⚪ **#{self.channel_slug} is offline** — waiting...")

        log.info(
            f"Config: window={settings.HYPE_WINDOW_SECONDS}s "
            f"threshold={settings.HYPE_THRESHOLD} msgs "
            f"cooldown={settings.HYPE_COOLDOWN_SECONDS}s"
        )

        # Run Scout (WebSocket) and Recorder concurrently
        await asyncio.gather(
            self._run_chat(chatroom_id),
            self.recorder.run(),
        )

    async def _run_chat(self, chatroom_id: int):
        async with websockets.connect(KICK_WS_URL) as ws:
            await ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }))
            log.info(f"Listening on #{self.channel_slug}...\n")

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
        log.debug(f"[{msg.username}] {msg.content}  ({rate:.1f} msg/s)")

        msgs_in_window = int(rate * self.detector.window)
        build_threshold = int(settings.HYPE_THRESHOLD * 0.67)

        if msgs_in_window >= build_threshold and not self._building_alerted:
            self._building_alerted = True
            discord_log(
                f"⚡ **Hype building** on #{self.channel_slug} — "
                f"`{msgs_in_window}/{settings.HYPE_THRESHOLD}` msgs in {settings.HYPE_WINDOW_SECONDS}s"
            )
        elif msgs_in_window < build_threshold:
            self._building_alerted = False

        if self.detector.should_trigger(rate, now):
            offset = self._offset(now)
            moment = self.detector.trigger(
                channel=self.channel_slug,
                stream_id=self._stream_id or "unknown",
                offset=offset,
                rate=rate * self.detector.window,
            )
            self._moments.append(moment)
            self._building_alerted = False

            log.info(
                f"\n{'='*50}\n"
                f"  HYPE on #{self.channel_slug}\n"
                f"  Rate  : {moment.message_rate:.0f} msgs / {settings.HYPE_WINDOW_SECONDS}s\n"
                f"  Offset: {offset:.0f}s into stream\n"
                f"  Sample: {moment.trigger_messages[-1]}\n"
                f"{'='*50}"
            )

            sample = moment.trigger_messages[-1] if moment.trigger_messages else ""
            discord_log(
                f"🔥 **HYPE TRIGGERED** on #{self.channel_slug}\n"
                f"Rate: `{moment.message_rate:.0f}` msgs/{settings.HYPE_WINDOW_SECONDS}s\n"
                f"Offset: `{offset:.0f}s` into stream\n"
                f"Sample: `{sample}`\n"
                f"⏳ Extracting clip from buffer..."
            )

            # Extract clip from local buffer immediately
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            clip_path = self.recorder.extract_clip(offset, timestamp)

            if clip_path:
                log.info(f"Clip extracted from buffer: {clip_path}")
                moment_dict = moment.to_dict()
                moment_dict["local_clip_path"] = str(clip_path)

                # Save locally
                with open(self._local_log, "a") as f:
                    f.write(json.dumps(moment_dict) + "\n")

                # Push to GitHub to trigger Clipper
                await push_moment_to_github_with_clip(moment_dict, session)
            else:
                log.warning("Buffer extraction failed — falling back to live recording")
                with open(self._local_log, "a") as f:
                    f.write(json.dumps(moment.to_dict()) + "\n")
                await push_moment_to_github(moment, session)


async def push_moment_to_github_with_clip(moment: dict, session: aiohttp.ClientSession):
    """Push moment including local clip path to GitHub."""
    if not settings.GITHUB_TOKEN or not settings.GITHUB_REPO:
        log.warning("GITHUB_TOKEN or GITHUB_REPO not set — skipping")
        return

    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    file_path = "output/pending_moments.jsonl"
    url = f"{GITHUB_API}/repos/{settings.GITHUB_REPO}/contents/{file_path}"

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
    updated_content = existing_content + new_line
    encoded = base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"[scout] clip ready: {moment['channel']} @ {moment['peak_offset']:.0f}s",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            log.info("Pushed to GitHub with clip path — Clipper workflow triggered")
        else:
            body = await resp.text()
            log.error(f"GitHub push failed ({resp.status}): {body}")


# =============================================================================
# Main
# =============================================================================

async def main(channel: str, debug: bool = False):
    if debug:
        logging.getLogger("scout").setLevel(logging.DEBUG)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, loop.stop)

    while True:
        scout = KickChatScout(channel)
        try:
            await scout.run()
        except asyncio.CancelledError:
            scout.recorder.cleanup()
            break
        except Exception as e:
            log.warning(f"Scout crashed: {e} — reconnecting in 30s...")
            discord_log(f"⚠️ **Scout crashed** — reconnecting in 30s...\n`{e}`")
            scout.recorder.cleanup()

        log.info("Waiting 30s before reconnecting...")
        await asyncio.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kick hype scout + recorder")
    parser.add_argument("--channel", required=True, help="Kick channel slug")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.channel, args.debug))
