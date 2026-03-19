"""
agents/scout_buffer.py — Scout + Rolling Buffer Recorder

Runs as always-on task on PythonAnywhere on the feature/rolling-buffer branch.

Rolling buffer strategy:
  - ffmpeg downloads raw HLS segments (no decoding — near-zero CPU)
  - Keeps last 3 segments (30s) on disk at all times
  - When hype fires → stitch last 3 segments + record 10s more
  - Total clip: ~30s before peak + 10s after = ~40s of perfect footage
  - No more missed PKs or deaths!

Run:
    cd /home/StreamerClipper/clipbot && python -m agents.scout_buffer --channel odablock
"""
import asyncio
import json
import logging
import argparse
import base64
import signal
import subprocess
import shutil
import os
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

# Buffer settings
SEGMENT_DURATION = 10       # seconds per segment
MAX_SEGMENTS = 3            # keep last 30s on disk
CLIP_AFTER_PEAK = 10        # seconds to record after hype peak
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
    """Get the raw HLS playlist URL using streamlink --stream-url."""
    try:
        result = subprocess.run(
            ["streamlink", "--stream-url", f"https://kick.com/{channel_slug}", "best"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            url = result.stdout.strip()
            if url.startswith("http"):
                log.info(f"HLS URL obtained: {url[:80]}...")
                return url
        log.warning(f"streamlink --stream-url failed: {result.stderr[-200:]}")
        return None
    except Exception as e:
        log.warning(f"Failed to get HLS URL: {e}")
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
        log.info(f"Uploading clip to GitHub: {clip_path.name} ({clip_path.stat().st_size/1024/1024:.1f}MB)")

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
                log.info(f"Clip uploaded to GitHub: {local_clip}")
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
    """
    Downloads Kick's HLS stream as raw segments using ffmpeg.
    No decoding/encoding — just copies .ts chunks from CDN.
    Keeps only the last MAX_SEGMENTS on disk.
    Near-zero CPU usage.
    """

    def __init__(self, channel: str):
        self.channel = channel
        self.buffer_dir = BUFFER_DIR / channel
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self._process: subprocess.Popen | None = None
        self._running = False
        self._segment_count = 0
        self._hls_url: str | None = None

    def _clean_old_segments(self):
        """Delete segments older than MAX_SEGMENTS."""
        segments = sorted(self.buffer_dir.glob("seg_*.ts"))
        if len(segments) > MAX_SEGMENTS:
            for seg in segments[:len(segments) - MAX_SEGMENTS]:
                seg.unlink(missing_ok=True)
                log.debug(f"Deleted old segment: {seg.name}")

    def get_buffered_segments(self) -> list[Path]:
        """Return current buffer segments sorted oldest to newest."""
        return sorted(self.buffer_dir.glob("seg_*.ts"))

    def extract_clip(self, timestamp: str) -> Path | None:
        """
        Stitch buffered segments into a clip file.
        Returns the clip path or None if buffer is empty.
        """
        segments = self.get_buffered_segments()
        if not segments:
            log.warning("Buffer is empty — no segments to stitch")
            return None

        log.info(f"Stitching {len(segments)} buffered segments (~{len(segments)*SEGMENT_DURATION}s)")

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
            log.error(f"Stitch failed: {result.stderr[-300:]}")
            return None

        size = output_path.stat().st_size
        if size < 50_000:
            output_path.unlink(missing_ok=True)
            log.error(f"Stitched clip too small ({size} bytes)")
            return None

        log.info(f"Buffer stitched: {output_path.name} ({size/1024/1024:.1f}MB)")
        return output_path

    async def start(self, hls_url: str):
        """Start one continuous ffmpeg process writing sequential segments."""
        self._hls_url = hls_url
        self._running = True
        log.info(f"Rolling buffer started for #{self.channel}")
        asyncio.create_task(self._download_loop())

    async def _download_loop(self):
        """
        Single continuous ffmpeg process using segment muxer.
        Writes seg_000000.ts, seg_000001.ts etc in sequence.
        Proper continuous footage — no gaps or restarts.
        """
        segment_pattern = str(self.buffer_dir / "seg_%06d.ts")

        cmd = [
        "ffmpeg", "-y",
        "-live_start_index", "-1",      # always start from latest segment
        "-i", self._hls_url,
        "-c", "copy",
        "-f", "segment",
        "-segment_time", str(SEGMENT_DURATION),
        "-reset_timestamps", "1",
        segment_pattern
        ]

        # Start ffmpeg as background process
        try:
            self._process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            log.info(f"ffmpeg segment process started (PID {self._process.pid})")
        except Exception as e:
            log.error(f"Failed to start ffmpeg: {e}")
            return

        # Watchdog — clean old segments and monitor process
        while self._running:
            await asyncio.sleep(SEGMENT_DURATION)
            self._clean_old_segments()

            segs = self.get_buffered_segments()
            if segs:
                log.debug(f"Buffer: {len(segs)} segments, latest: {segs[-1].name} ({segs[-1].stat().st_size/1024:.0f}KB)")

            # Check if process died
            if self._process and self._process.returncode is not None:
                log.warning("ffmpeg segment process died — refreshing HLS URL")
                new_url = await asyncio.get_running_loop().run_in_executor(
                    None, get_hls_url, self.channel
                )
                if new_url:
                    self._hls_url = new_url
                    self._process = await asyncio.create_subprocess_exec(
                        *["ffmpeg", "-y", "-i", new_url, "-c", "copy",
                          "-f", "segment", "-segment_time", str(SEGMENT_DURATION),
                          "-reset_timestamps", "1", segment_pattern],
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    log.info(f"ffmpeg restarted (PID {self._process.pid})")
                else:
                    await asyncio.sleep(30)

    def stop(self):
        """Stop the buffer — keep files for inspection."""
        self._running = False
        if self._process:
            try:
                self._process.terminate()
            except Exception:
                pass
        log.info(f"Buffer stopped — files kept at {self.buffer_dir}")




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
# Combined Scout + Rolling Buffer
# =============================================================================

class KickChatScout:
    def __init__(self, channel_slug: str):
        self.channel_slug = channel_slug
        self.detector = HypeDetector()
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

            # Get HLS URL and start rolling buffer
            hls_url = await loop.run_in_executor(None, get_hls_url, self.channel_slug)
            if hls_url:
                await self.buffer.start(hls_url)
                discord_log(
                    f"🟢 **Scout + Rolling Buffer** connected to #{self.channel_slug}\n"
                    f"Buffer: `{MAX_SEGMENTS * SEGMENT_DURATION}s` rolling window — PKs/deaths captured!"
                )
            else:
                discord_log(
                    f"🟡 **Scout connected** to #{self.channel_slug} — buffer unavailable, using on-demand recording"
                )
        else:
            log.warning("Channel OFFLINE — listening, waiting for stream...")
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

        if msgs_in_window >= build_threshold and not self._building_alerted:
            self._building_alerted = True
            buf_segments = len(self.buffer.get_buffered_segments())
            discord_log(
                f"⚡ **Hype building** on #{self.channel_slug} — "
                f"`{msgs_in_window}/{settings.HYPE_THRESHOLD}` msgs "
                f"| Buffer: `{buf_segments * SEGMENT_DURATION}s` ready"
            )
        elif msgs_in_window < build_threshold // 2:
            self._building_alerted = False

        # Manual trigger keywords — fire clip immediately
        MANUAL_TRIGGERS = ["go for it mr streamer", "!clip", "clipbot"]
        content_lower = msg.content.lower()
        manual_triggered = any(kw in content_lower for kw in MANUAL_TRIGGERS)

        if manual_triggered or self.detector.should_trigger(rate, now):
            offset = self._offset(now)
            moment = self.detector.trigger(
                channel=self.channel_slug,
                stream_id=self._stream_id or "unknown",
                offset=offset,
                rate=rate * self.detector.window,
            )
            self._moments.append(moment)
            self._building_alerted = False

            sample = moment.trigger_messages[-1] if moment.trigger_messages else ""
            buf_segments = len(self.buffer.get_buffered_segments())

            log.info(
                f"\n{'='*50}\n"
                f"  HYPE on #{self.channel_slug}\n"
                f"  Rate  : {moment.message_rate:.0f} msgs/{settings.HYPE_WINDOW_SECONDS}s\n"
                f"  Offset: {offset:.0f}s into stream\n"
                f"  Buffer: {buf_segments} segments ({buf_segments*SEGMENT_DURATION}s)\n"
                f"  Sample: {sample}\n"
                f"{'='*50}"
            )

            discord_log(
                f"🔥 **HYPE TRIGGERED** on #{self.channel_slug}\n"
                f"Rate: `{moment.message_rate:.0f}` msgs/{settings.HYPE_WINDOW_SECONDS}s\n"
                f"Buffer: `{buf_segments * SEGMENT_DURATION}s` captured\n"
                f"⏳ Recording {CLIP_AFTER_PEAK}s more then clipping..."
            )

            # Record CLIP_AFTER_PEAK more seconds into buffer
            await asyncio.sleep(CLIP_AFTER_PEAK)

            # Stitch buffer into clip
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            clip_path = self.buffer.extract_clip(timestamp)

            moment_dict = moment.to_dict()

            if clip_path:
                log.info(f"Clip from buffer: {clip_path.name}")
                moment_dict["local_clip_path"] = str(clip_path)
                discord_log(f"✅ Clip captured from buffer — processing started!")
            else:
                log.warning("Buffer extraction failed — Clipper will record live")
                discord_log(f"⚠️ Buffer empty — falling back to live recording")

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
            scout.buffer.stop()
            break
        except Exception as e:
            log.warning(f"Scout crashed: {e} — reconnecting in 30s...")
            discord_log(f"⚠️ **Scout crashed** — reconnecting in 30s...\n`{e}`")
            scout.buffer.stop()

        log.info("Waiting 30s before reconnecting...")
        await asyncio.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kick scout + rolling HLS buffer")
    parser.add_argument("--channel", required=True, help="Kick channel slug")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.channel, args.debug))