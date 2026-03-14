"""
agents/clipper.py — Agent 2: Clipper

Runs inside a GitHub Actions workflow (NOT on PythonAnywhere).
Reads the latest unprocessed moment from output/pending_moments.jsonl,
downloads the VOD segment, crops to 9:16, adds captions, and saves
the finished clip to output/clips/.

GitHub Actions provides: Python, ffmpeg, yt-dlp, faster-whisper
"""
import json
import logging
import subprocess
import sys
import os
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
PADDING_BEFORE = int(os.getenv("CLIP_PADDING_BEFORE", 20))
PADDING_AFTER = int(os.getenv("CLIP_PADDING_AFTER", 10))


def load_next_moment() -> dict | None:
    """Load the oldest unprocessed moment."""
    if not MOMENTS_FILE.exists():
        log.info("No pending_moments.jsonl found — nothing to process")
        return None

    lines = MOMENTS_FILE.read_text().strip().splitlines()
    if not lines:
        log.info("pending_moments.jsonl is empty — nothing to process")
        return None

    # Take first unprocessed moment
    moment = json.loads(lines[0])
    log.info(f"Processing moment: {moment['channel']} @ {moment['peak_offset']:.0f}s")
    return moment, lines


def mark_processed(moment: dict, lines: list[str]):
    """Move the processed moment out of the pending file."""
    # Remove first line (the one we just processed)
    remaining = lines[1:]
    MOMENTS_FILE.write_text("\n".join(remaining) + ("\n" if remaining else ""))

    # Append to processed log
    moment["processed_at"] = datetime.utcnow().isoformat()
    with open(PROCESSED_FILE, "a") as f:
        f.write(json.dumps(moment) + "\n")


def get_kick_vod_url(stream_id: str) -> str:
    """
    Build the yt-dlp compatible URL for a Kick VOD.
    yt-dlp supports kick.com natively as of 2024.
    """
    return f"https://kick.com/video/{stream_id}"


def download_segment(vod_url: str, start: float, duration: float, output_path: Path) -> bool:
    """
    Download a specific time segment from a Kick VOD using yt-dlp + ffmpeg.
    Uses yt-dlp to get the direct stream URL, then ffmpeg to cut the segment.
    """
    log.info(f"Fetching VOD stream URL via yt-dlp...")

    # Get the direct URL without downloading
    result = subprocess.run(
        [
        "yt-dlp",
        "--get-url",
        "--format", "best[ext=mp4]/best",
        "--add-header", "User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--add-header", "Referer:https://kick.com",
        vod_url,
        ],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log.error(f"yt-dlp failed: {result.stderr}")
        return False

    stream_url = result.stdout.strip()
    if not stream_url:
        log.error("yt-dlp returned empty URL")
        return False

    log.info(f"Downloading segment: {start:.0f}s → {start + duration:.0f}s")

    # Use ffmpeg to cut the segment directly from the stream URL
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(max(0, start)),
        "-i", stream_url,
        "-headers", "Referer: https://kick.com\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n",
        "-t", str(duration),
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"ffmpeg segment download failed: {result.stderr[-500:]}")
        return False

    log.info(f"Segment downloaded: {output_path}")
    return True


def crop_to_vertical(input_path: Path, output_path: Path) -> bool:
    """
    Crop 16:9 video to 9:16 (vertical) for Shorts/Reels/TikTok.
    Crops centre of frame — takes the middle third horizontally.
    """
    log.info("Cropping to 9:16 vertical...")

    # Get video dimensions
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(input_path)],
        capture_output=True, text=True
    )
    import json as _json
    streams = _json.loads(probe.stdout).get("streams", [])
    video = next((s for s in streams if s["codec_type"] == "video"), None)
    if not video:
        log.error("Could not read video dimensions")
        return False

    w = int(video["width"])
    h = int(video["height"])

    # For 16:9 source: target width = h * 9/16, centred
    target_w = int(h * 9 / 16)
    crop_x = (w - target_w) // 2

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


def add_captions(input_path: Path, output_path: Path) -> bool:
    """
    Transcribe audio with faster-whisper and burn subtitles into the video.
    Falls back to no captions if whisper isn't available.
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        log.warning("faster-whisper not installed — skipping captions")
        import shutil
        shutil.copy(input_path, output_path)
        return True

    log.info("Transcribing audio with Whisper (tiny model)...")
    srt_path = input_path.with_suffix(".srt")

    try:
        model = WhisperModel("tiny", compute_type="int8")
        segments, _ = model.transcribe(str(input_path), beam_size=1)

        with open(srt_path, "w") as f:
            for i, seg in enumerate(segments, 1):
                def fmt_time(t):
                    h, r = divmod(t, 3600)
                    m, s = divmod(r, 60)
                    ms = int((s % 1) * 1000)
                    return f"{int(h):02}:{int(m):02}:{int(s):02},{ms:03}"
                f.write(f"{i}\n{fmt_time(seg.start)} --> {fmt_time(seg.end)}\n{seg.text.strip()}\n\n")

        log.info(f"Transcript written: {srt_path}")
    except Exception as e:
        log.warning(f"Whisper transcription failed: {e} — skipping captions")
        import shutil
        shutil.copy(input_path, output_path)
        return True

    # Burn subtitles
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
        log.warning(f"Caption burn failed — using uncaptioned clip: {result.stderr[-300:]}")
        import shutil
        shutil.copy(input_path, output_path)

    srt_path.unlink(missing_ok=True)
    log.info(f"Final clip: {output_path}")
    return True


def process_moment(moment: dict) -> Path | None:
    """Full pipeline for one moment: download → crop → captions → return path."""
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path("/tmp/clipbot")
    tmp.mkdir(exist_ok=True)

    channel = moment["channel"]
    stream_id = moment["stream_id"]
    offset = float(moment["peak_offset"])
    start = max(0, offset - PADDING_BEFORE)
    duration = PADDING_BEFORE + PADDING_AFTER

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    slug = f"{channel}_{timestamp}"

    raw = tmp / f"{slug}_raw.mp4"
    cropped = tmp / f"{slug}_cropped.mp4"
    final = CLIPS_DIR / f"{slug}_final.mp4"

    vod_url = get_kick_vod_url(stream_id)

    if not download_segment(vod_url, start, duration, raw):
        return None
    if not crop_to_vertical(raw, cropped):
        return None
    if not add_captions(cropped, final):
        return None

    # Clean up temp files
    raw.unlink(missing_ok=True)
    cropped.unlink(missing_ok=True)

    log.info(f"Clip ready: {final}")
    return final


def main():
    result = load_next_moment()
    if result is None:
        sys.exit(0)

    moment, lines = result
    clip_path = process_moment(moment)

    if clip_path:
        mark_processed(moment, lines)
        log.info(f"Done — clip saved to {clip_path}")
        # Write clip path to a file so the Publisher step can find it
        Path("output/latest_clip.txt").write_text(str(clip_path))
    else:
        log.error("Clipping failed — moment left in pending queue for retry")
        sys.exit(1)


if __name__ == "__main__":
    main()
