"""
agents/clipper.py — Agent 2: Clipper

Runs inside a GitHub Actions workflow (NOT on PythonAnywhere).
Records a live segment from a Kick stream using streamlink,
crops to 9:16, adds captions, and saves to output/clips/.

The workflow triggers when the Scout pushes a new hype moment.
Because we're clipping LIVE, the moment must still be within the
current stream when this runs (usually within 1-2 minutes of detection).
"""
import json
import logging
import subprocess
import sys
import os
import shutil
from datetime import datetime
from pathlib import Path

log = logging.getLogger("clipper")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

CLIPS_DIR = Path("output/clips")
MOMENTS_FILE = Path("output/pending_moments.jsonl")
PROCESSED_FILE = Path("output/processed_moments.jsonl")
DURATION = int(os.getenv("CLIP_PADDING_BEFORE", 20)) + int(os.getenv("CLIP_PADDING_AFTER", 10))


# =============================================================================
# Moment queue
# =============================================================================

def load_next_moment():
    if not MOMENTS_FILE.exists():
        log.info("No pending_moments.jsonl — nothing to process")
        return None, None

    lines = [l for l in MOMENTS_FILE.read_text().strip().splitlines() if l.strip()]
    if not lines:
        log.info("pending_moments.jsonl is empty — nothing to process")
        return None, None

    moment = json.loads(lines[0])
    log.info(f"Processing: #{moment['channel']} @ {moment['peak_offset']:.0f}s into stream")
    return moment, lines


def mark_processed(moment: dict, lines: list[str]):
    remaining = lines[1:]
    MOMENTS_FILE.write_text("\n".join(remaining) + ("\n" if remaining else ""))
    moment["processed_at"] = datetime.utcnow().isoformat()
    with open(PROCESSED_FILE, "a") as f:
        f.write(json.dumps(moment) + "\n")


# =============================================================================
# Step 1 — Record live segment with streamlink
# =============================================================================

def record_live_segment(channel: str, duration: int, output_path: Path) -> bool:
    """
    Record `duration` seconds from a live Kick stream using streamlink.
    streamlink's Kick plugin uses cloudscraper internally — bypasses Cloudflare.
    """
    log.info(f"Recording {duration}s from kick.com/{channel} via streamlink...")

    cmd = [
        "streamlink",
        "--output", str(output_path),
        "--stream-timeout", str(duration + 30),
        "--stream-segment-timeout", "10",
        "--hls-duration", str(duration),
        f"https://kick.com/{channel}",
        "best",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=duration + 60
        )
    except subprocess.TimeoutExpired:
        log.warning("streamlink timed out — checking if partial file is usable")

    if not output_path.exists():
        log.error(f"No output file created. streamlink stderr: {result.stderr[-600:]}")
        return False

    size = output_path.stat().st_size
    if size < 50_000:
        log.error(f"Output file too small ({size} bytes) — likely failed")
        return False

    log.info(f"Recorded {size/1024/1024:.1f}MB -> {output_path}")
    return True


# =============================================================================
# Step 2 — Crop to 9:16 vertical
# =============================================================================

def crop_to_vertical(input_path: Path, output_path: Path) -> bool:
    log.info("Cropping to 9:16 vertical...")

    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(input_path)],
        capture_output=True, text=True
    )
    streams = json.loads(probe.stdout).get("streams", [])
    video = next((s for s in streams if s["codec_type"] == "video"), None)
    if not video:
        log.error("Could not read video dimensions")
        return False

    w = int(video["width"])
    h = int(video["height"])
    target_w = int(h * 9 / 16)
    crop_x = (w - target_w) // 2

    log.info(f"Source: {w}x{h} -> cropped to {target_w}x{h}")

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", f"crop={target_w}:{h}:{crop_x}:0",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"Crop failed: {result.stderr[-500:]}")
        return False

    log.info(f"Cropped: {output_path}")
    return True


# =============================================================================
# Step 3 — Add captions with Whisper
# =============================================================================

def add_captions(input_path: Path, output_path: Path) -> bool:
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        log.warning("faster-whisper not installed — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    log.info("Transcribing with Whisper tiny model...")
    srt_path = input_path.with_suffix(".srt")

    try:
        model = WhisperModel("tiny", compute_type="int8")
        segments, _ = model.transcribe(str(input_path), beam_size=1)

        def fmt_time(t):
            h, r = divmod(t, 3600)
            m, s = divmod(r, 60)
            ms = int((s % 1) * 1000)
            return f"{int(h):02}:{int(m):02}:{int(s):02},{ms:03}"

        with open(srt_path, "w") as f:
            for i, seg in enumerate(segments, 1):
                f.write(f"{i}\n{fmt_time(seg.start)} --> {fmt_time(seg.end)}\n{seg.text.strip()}\n\n")

        log.info(f"Captions written: {srt_path}")
    except Exception as e:
        log.warning(f"Whisper failed: {e} — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", f"subtitles={srt_path}:force_style='FontSize=18,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,Outline=2'",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.warning(f"Caption burn failed — using uncaptioned: {result.stderr[-300:]}")
        shutil.copy(input_path, output_path)

    srt_path.unlink(missing_ok=True)
    log.info(f"Final clip: {output_path}")
    return True


# =============================================================================
# Main pipeline
# =============================================================================

def process_moment(moment: dict) -> Path | None:
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path("/tmp/clipbot")
    tmp.mkdir(exist_ok=True)

    channel = moment["channel"]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    slug = f"{channel}_{timestamp}"

    raw = tmp / f"{slug}_raw.ts"
    cropped = tmp / f"{slug}_cropped.mp4"
    final = CLIPS_DIR / f"{slug}_final.mp4"

    if not record_live_segment(channel, DURATION, raw):
        return None
    if not crop_to_vertical(raw, cropped):
        return None
    if not add_captions(cropped, final):
        return None

    raw.unlink(missing_ok=True)
    cropped.unlink(missing_ok=True)

    log.info(f"Clip ready: {final}")
    return final


def main():
    moment, lines = load_next_moment()
    if moment is None:
        sys.exit(0)

    clip_path = process_moment(moment)

    if clip_path:
        mark_processed(moment, lines)
        log.info(f"Done — clip saved to {clip_path}")
        Path("output/latest_clip.txt").write_text(str(clip_path))
    else:
        log.error("Clipping failed — moment left in pending queue for retry")
        sys.exit(1)


if __name__ == "__main__":
    main()
