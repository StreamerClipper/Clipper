"""
agents/clipper.py — Agent 2: Clipper

Runs inside a GitHub Actions workflow (NOT on PythonAnywhere).
Records a live segment from a Kick stream using streamlink,
uses Claude vision to detect webcam position, then composites
a 9:16 vertical clip with cam on top (40%) and content on bottom (60%).
"""
import json
import logging
import subprocess
import sys
import os
import shutil
import base64
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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 60)
    except subprocess.TimeoutExpired:
        log.warning("streamlink timed out — checking if partial file is usable")
        result = subprocess.CompletedProcess(cmd, 1, "", "timeout")

    if not output_path.exists():
        log.error(f"No output file. stderr: {result.stderr[-600:]}")
        return False

    size = output_path.stat().st_size
    if size < 50_000:
        log.error(f"Output too small ({size} bytes)")
        return False

    log.info(f"Recorded {size/1024/1024:.1f}MB -> {output_path}")
    return True


# =============================================================================
# Step 2 — Detect webcam position with Claude vision
# =============================================================================

def extract_frame(video_path: Path, frame_path: Path) -> bool:
    """Extract the first frame of the video as a JPEG."""
    cmd = [
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-vframes", "1",
        "-q:v", "2",
        str(frame_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0 and frame_path.exists()

# Per-channel webcam defaults (used when Claude vision fails)
WEBCAM_DEFAULTS = {
    "odablock": lambda w, h: {"x": int(w * 0.75), "y": 0, "w": int(w * 0.25), "h": int(h * 0.35)},
}

def get_default_webcam(channel: str, video_w: int, video_h: int) -> dict | None:
    if channel in WEBCAM_DEFAULTS:
        cam = WEBCAM_DEFAULTS[channel](video_w, video_h)
        log.info(f"Using default webcam position for #{channel}: {cam}")
        return cam
    return None

def detect_webcam(frame_path: Path, video_w: int, video_h: int, channel: str = "") -> dict | None:
    """
    Send the first frame to Claude vision and ask it to identify
    the webcam/facecam bounding box.

    Returns a dict with keys: x, y, w, h (in pixels)
    or None if no webcam detected.
    """
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY — skipping webcam detection, using fullscreen crop")
        return None

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        with open(frame_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": image_data,
                        }
                    },
                    {
                        "type": "text",
                        "text": f"""This is a frame from a live stream. The full resolution is {video_w}x{video_h} pixels.

Identify the streamer's webcam/facecam rectangle if one exists.

Respond ONLY with a JSON object — no explanation, no markdown:
{{"has_webcam": true, "x": 0, "y": 0, "w": 320, "h": 240}}

Where x, y, w, h are pixel coordinates of the webcam in the full {video_w}x{video_h} frame.
If there is no visible webcam/facecam, respond with:
{{"has_webcam": false}}"""
                    }
                ]
            }]
        )

        text = response.content[0].text.strip()
        # Strip any accidental markdown
        text = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)

        if not result.get("has_webcam"):
            log.info("Claude: no webcam detected — will use fullscreen crop")
            return None

        cam = {
            "x": int(result["x"]),
            "y": int(result["y"]),
            "w": int(result["w"]),
            "h": int(result["h"]),
        }
        log.info(f"Claude detected webcam at x={cam['x']} y={cam['y']} {cam['w']}x{cam['h']}")
        return cam

    except Exception as e:
        log.warning(f"Webcam detection failed: {e} — trying channel default")
        return get_default_webcam(channel, video_w, video_h)


# =============================================================================
# Step 3 — Composite 9:16 layout
# =============================================================================

def get_video_dimensions(video_path: Path) -> tuple[int, int]:
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(video_path)],
        capture_output=True, text=True
    )
    streams = json.loads(probe.stdout).get("streams", [])
    video = next((s for s in streams if s["codec_type"] == "video"), None)
    if not video:
        return 1920, 1080
    return int(video["width"]), int(video["height"])


def crop_to_vertical(input_path: Path, output_path: Path, channel: str = "") -> bool:
    """
    Build a 9:16 vertical clip:
    - Top 40%: streamer webcam (detected by Claude vision)
    - Bottom 60%: game/content (centre crop of source)
    - Falls back to simple centre crop if no webcam found
    """
    w, h = get_video_dimensions(input_path)
    log.info(f"Source dimensions: {w}x{h}")

    # Output canvas: 9:16 at 1080p = 608x1080 (rounded to even)
    out_w = 608
    out_h = 1080
    cam_h = int(out_h * 0.40)    # 432px — top 40%
    content_h = out_h - cam_h    # 648px — bottom 60%

    # Extract first frame for Claude
    frame_path = input_path.with_suffix(".jpg")
    has_frame = extract_frame(input_path, frame_path)

    cam = None
    if has_frame:
        cam = detect_webcam(frame_path, w, h, channel)
        frame_path.unlink(missing_ok=True)

    if cam:
        # === Layout: cam top 40%, content bottom 60% ===
        log.info("Building 40/60 cam+content layout...")

        # Scale cam to fill top section (out_w wide, cam_h tall)
        # Scale content centre crop to fill bottom section
        content_crop_w = int(h * out_w / content_h)
        content_crop_x = max(0, (w - content_crop_w) // 2)

        vf = (
            # Scale source to two streams
            f"[0:v]split=2[cam_src][content_src];"

            # Top: crop webcam area, scale to out_w x cam_h
            f"[cam_src]crop={cam['w']}:{cam['h']}:{cam['x']}:{cam['y']},"
            f"scale={out_w}:{cam_h}:force_original_aspect_ratio=decrease,"
            f"pad={out_w}:{cam_h}:(ow-iw)/2:(oh-ih)/2[cam_out];"

            # Bottom: centre crop content, scale to out_w x content_h
            f"[content_src]crop={content_crop_w}:{h}:{content_crop_x}:0,"
            f"scale={out_w}:{content_h}[content_out];"

            # Stack vertically
            f"[cam_out][content_out]vstack=inputs=2[out]"
        )

        cmd = [
            "ffmpeg", "-y",
            "-i", str(input_path),
            "-filter_complex", vf,
            "-map", "[out]",
            "-map", "0:a",
            "-c:v", "libx264",
            "-c:a", "aac",
            "-preset", "fast",
            str(output_path)
        ]
    else:
        # === Fallback: simple centre crop to 9:16 ===
        log.info("No webcam — using fullscreen centre crop...")
        target_w = int(h * 9 / 16)
        crop_x = (w - target_w) // 2

        cmd = [
            "ffmpeg", "-y",
            "-i", str(input_path),
            "-vf", f"crop={target_w}:{h}:{crop_x}:0,scale={out_w}:{out_h}",
            "-c:v", "libx264",
            "-c:a", "aac",
            "-preset", "fast",
            str(output_path)
        ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"ffmpeg crop failed: {result.stderr[-500:]}")
        return False

    log.info(f"Cropped: {output_path}")
    return True


# =============================================================================
# Step 4 — Add captions with Whisper
# =============================================================================

def add_captions(input_path: Path, output_path: Path) -> bool:
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        log.warning("faster-whisper not installed — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    log.info("Transcribing with Whisper for word-level captions...")
    srt_path = input_path.with_suffix(".srt")

    try:
        model = WhisperModel("tiny", compute_type="int8")
        segments, _ = model.transcribe(
            str(input_path),
            beam_size=1,
            word_timestamps=True  # enables word-by-word timing
        )

        def fmt_time(t):
            h, r = divmod(t, 3600)
            m, s = divmod(r, 60)
            ms = int((s % 1) * 1000)
            return f"{int(h):02}:{int(m):02}:{int(s):02},{ms:03}"

        # Write one subtitle entry per word
        entries = []
        for seg in segments:
            if hasattr(seg, "words") and seg.words:
                for word in seg.words:
                    entries.append((word.start, word.end, word.word.strip().upper()))
            else:
                # Fallback to segment level if no word timestamps
                entries.append((seg.start, seg.end, seg.text.strip().upper()))

        with open(srt_path, "w") as f:
            for i, (start, end, text) in enumerate(entries, 1):
                # Add small buffer so words don't disappear too fast
                end = max(end, start + 0.15)
                f.write(f"{i}\n{fmt_time(start)} --> {fmt_time(end)}\n{text}\n\n")

        log.info(f"Word-level captions: {len(entries)} words")

    except Exception as e:
        log.warning(f"Whisper failed: {e} — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    # Burn with large centred style matching Odablock's YouTube Shorts
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", (
            f"subtitles={srt_path}:force_style='"
            "FontName=Arial,"
            "FontSize=22,"
            "Bold=1,"
            "PrimaryColour=&H00FFFFFF,"   # white text
            "OutlineColour=&H00000000,"   # black outline
            "BackColour=&H00000000,"
            "Outline=3,"                  # thick outline
            "Shadow=0,"
            "Alignment=2,"               # centre bottom
            "MarginV=80'"                # push up from bottom edge
        ),
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
    log.info(f"Final clip with word captions: {output_path}")
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
    if not crop_to_vertical(raw, cropped, channel):
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