"""
agents/scout.py — Agent 1: Scout + On-Demand Recorder

Runs as a single always-on task on PythonAnywhere.

Smart recording strategy:
  - At 67% of hype threshold → start recording immediately
  - At 100% threshold → mark peak, record 10s more, then clip
  - If hype dies within 30s without reaching 100% → abort, delete recording
  - This captures the exact hype moment with no delay

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
import sys
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

# Recording settings
CLIP_AFTER_PEAK = 10        # seconds to record after hype peak
RECORDING_TIMEOUT = 30      # seconds to wait for peak before aborting recording
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

async def push_moment_to_github(moment: dict, session: aiohttp.ClientSession):
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

    new_line = json.dumps(moment) + "\n"
    updated_content = existing_content + new_line
    encoded = base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"[scout] hype moment: {moment['channel']} @ {moment.get('peak_offset', 0):.0f}s",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            log.info("Pushed to GitHub — Clipper workflow triggered")
        else:
            body = await resp.text()
            log.error(f"GitHub push failed ({resp.status}): {body[:200]}")


# =============================================================================
# On-demand recorder
# =============================================================================

class OnDemandRecorder:
    """
    Records only when hype is building.
    Starts at 67% threshold, stops after peak + CLIP_AFTER_PEAK seconds.
    Aborts if peak never reached within RECORDING_TIMEOUT seconds.
    """

    def __init__(self, channel: str):
        self.channel = channel
        self._process: subprocess.Popen | None = None
        self._recording_path: Path | None = None
        self._recording = False
        self._start_time: datetime | None = None
        self._timeout_task: asyncio.Task | None = None
        CLIPS_DIR.mkdir(parents=True, exist_ok=True)

    @property
    def is_recording(self) -> bool:
        return self._recording

    def start(self) -> Path | None:
        """Start recording. Returns the output path."""
        if self._recording:
            log.debug("Already recording")
            return self._recording_path

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = CLIPS_DIR / f"{self.channel}_{timestamp}_raw.ts"
        self._recording_path = output_path
        self._start_time = datetime.now(timezone.utc)

        cmd = [
            "streamlink",
            "--stdout",
            "--stream-timeout", "120",
            "--stream-segment-timeout", "10",
            f"https://kick.com/{self.channel}",
            "best",
        ]

        try:
            with open(output_path, "wb") as f:
                self._process = subprocess.Popen(
                    cmd, stdout=f, stderr=subprocess.DEVNULL
                )
            self._recording = True
            log.info(f"🎬 Recording started: {output_path.name}")
            return output_path
        except Exception as e:
            log.error(f"Failed to start recording: {e}")
            return None

    def stop(self) -> Path | None:
        """Stop recording and return the file path if valid."""
        if not self._recording:
            return None

        self._recording = False

        if self._process:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
            self._process = None

        path = self._recording_path
        self._recording_path = None

        if path and path.exists():
            size = path.stat().st_size
            if size < 50_000:
                log.warning(f"Recording too small ({size} bytes) — discarding")
                path.unlink(missing_ok=True)
                return None
            log.info(f"Recording stopped: {path.name} ({size/1024/1024:.1f}MB)")
            return path

        return None

    def abort(self):
        """Abort recording and delete the file."""
        if not self._recording:
            return
        log.info("Recording aborted — hype died before peak")
        self._recording = False

        if self._process:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
            self._process = None

        if self._recording_path:
            self._recording_path.unlink(missing_ok=True)
            log.info(f"Deleted aborted recording")
            self._recording_path = None

    def recording_duration(self) -> float:
        """How long we've been recording in seconds."""
        if not self._recording or not self._start_time:
            return 0.0
        return (datetime.now(timezone.utc) - self._start_time).total_seconds()


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
# Combined Scout + On-Demand Recorder
# =============================================================================

class KickChatScout:
    def __init__(self, channel_slug: str):
        self.channel_slug = channel_slug
        self.detector = HypeDetector()
        self.recorder = OnDemandRecorder(channel_slug)
        self._stream_start: datetime | None = None
        self._stream_id: str | None = None
        self._moments: list[HypeMoment] = []
        self._building_alerted = False
        self._timeout_task: asyncio.Task | None = None

        Path(settings.LOGS_DIR).mkdir(parents=True, exist_ok=True)
        self._local_log = Path(settings.LOGS_DIR) / f"{channel_slug}_moments.jsonl"

    def _offset(self, now: datetime) -> float:
        if self._stream_start is None:
            return 0.0
        return (now - self._stream_start).total_seconds()

    async def _abort_timeout(self):
        """Wait RECORDING_TIMEOUT seconds, then abort if no peak reached."""
        await asyncio.sleep(RECORDING_TIMEOUT)
        if self.recorder.is_recording:
            log.info(f"No peak reached in {RECORDING_TIMEOUT}s — aborting recording")
            discord_log(
                f"⚡ Hype on #{self.channel_slug} died before peak — recording aborted"
            )
            self.recorder.abort()
            self._building_alerted = False

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
            discord_log(f"🟢 **Scout connected** to #{self.channel_slug} — on-demand recording ready")
        else:
            log.warning("Channel is OFFLINE - listening, waiting for stream...")
            discord_log(f"⚪ **#{self.channel_slug} is offline** — waiting...")

        log.info(
            f"Config: window={settings.HYPE_WINDOW_SECONDS}s "
            f"threshold={settings.HYPE_THRESHOLD} msgs "
            f"cooldown={settings.HYPE_COOLDOWN_SECONDS}s"
        )

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
        msgs_in_window = int(rate * self.detector.window)
        build_threshold = int(settings.HYPE_THRESHOLD * 0.67)

        # Start recording at 67% threshold
        if msgs_in_window >= build_threshold and not self._building_alerted:
            self._building_alerted = True
            discord_log(
                f"⚡ **Hype building** on #{self.channel_slug} — "
                f"`{msgs_in_window}/{settings.HYPE_THRESHOLD}` msgs — recording started!"
            )

            # Start on-demand recording
            if not self.recorder.is_recording:
                self.recorder.start()

                # Start abort timeout
                if self._timeout_task:
                    self._timeout_task.cancel()
                self._timeout_task = asyncio.create_task(self._abort_timeout())

        elif msgs_in_window < build_threshold // 2:
            # Hype dropped well below build threshold — reset alert
            self._building_alerted = False

        # Check for full hype trigger
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

            # Cancel abort timeout — peak reached!
            if self._timeout_task:
                self._timeout_task.cancel()
                self._timeout_task = None

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
                f"⏳ Recording {CLIP_AFTER_PEAK}s more then clipping..."
            )

            # Record for CLIP_AFTER_PEAK more seconds then stop
            await asyncio.sleep(CLIP_AFTER_PEAK)

            # Stop recording and get clip
            clip_path = self.recorder.stop()

            if clip_path:
                rec_duration = (datetime.now(timezone.utc) - self.recorder._start_time).total_seconds() if self.recorder._start_time else 0
                log.info(f"Clip captured: {clip_path.name} — pushing to GitHub")

                moment_dict = moment.to_dict()
                moment_dict["local_clip_path"] = str(clip_path)

                with open(self._local_log, "a") as f:
                    f.write(json.dumps(moment_dict) + "\n")

                await push_moment_to_github(moment_dict, session)
                discord_log(f"✅ Clip ready — processing started!")
            else:
                log.warning("Recording failed — falling back to live recording via GitHub")
                moment_dict = moment.to_dict()
                with open(self._local_log, "a") as f:
                    f.write(json.dumps(moment_dict) + "\n")
                await push_moment_to_github(moment_dict, session)


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
            scout.recorder.abort()
            break
        except Exception as e:
            log.warning(f"Scout crashed: {e} — reconnecting in 30s...")
            discord_log(f"⚠️ **Scout crashed** — reconnecting in 30s...\n`{e}`")
            scout.recorder.abort()

        log.info("Waiting 30s before reconnecting...")
        await asyncio.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kick hype scout + on-demand recorder")
    parser.add_argument("--channel", required=True, help="Kick channel slug")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.channel, args.debug))