"""
agents/clipper.py — Agent 2: Clipper

Runs inside a GitHub Actions workflow (NOT on PythonAnywhere).
Pipeline:
  1. Record live segment from Kick via streamlink
  2. Smart trim — remove dead sections, keep best 20-30s
  3. Crop to 9:16 vertical layout (35% cam top, 65% content bottom)
  4. Background music (OSRS vibe-matched)
  5. Sound effects (KO, death, FAAHHH)
  6. Word-pair captions via Whisper
  7. Save finished clip to output/clips/
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
from config.settings import settings

log = logging.getLogger("clipper")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

CLIPS_DIR = Path("output/clips")
MOMENTS_FILE = Path("output/pending_moments.jsonl")
PROCESSED_FILE = Path("output/processed_moments.jsonl")
DURATION = int(os.getenv("CLIP_PADDING_BEFORE", 50)) + int(os.getenv("CLIP_PADDING_AFTER", 10))
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
# Step 2 — Demucs disabled, use original audio
# =============================================================================

def separate_vocals(input_path: Path, output_path: Path) -> bool:
    """Demucs disabled — using original audio directly."""
    shutil.copy(input_path, output_path)
    return True


# =============================================================================
# Step 3 — Detect webcam position
# =============================================================================

def extract_frame(video_path: Path, frame_path: Path) -> bool:
    cmd = [
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-vframes", "1",
        "-q:v", "2",
        str(frame_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0 and frame_path.exists()


# NEW
def get_default_webcam(channel: str, video_w: int, video_h: int) -> dict | None:
    cam = settings.get_webcam(channel, video_w, video_h)
    if cam:
        log.info(f"Using .env webcam for #{channel}: {cam}")
    return cam


def detect_webcam(frame_path: Path, video_w: int, video_h: int, channel: str = "") -> dict | None:
    # For known channels — use verified coordinates BUT also check for fullscreen
    if settings.get_webcam(channel, video_w, video_h):
        # Send frame to Claude to check if fullscreen mode is active
        if ANTHROPIC_API_KEY:
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                with open(frame_path, "rb") as f:
                    image_data = base64.b64encode(f.read()).decode("utf-8")

                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=100,
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
                                "text": """Is the streamer's webcam/face taking up the FULL screen (or most of it),
with no game visible? Or is the game visible with webcam in a corner?
Reply with ONLY one word: FULLSCREEN or CORNER"""
                            }
                        ]
                    }]
                )

                answer = response.content[0].text.strip().upper()
                log.info(f"Claude layout detection: {answer}")

                if "FULLSCREEN" in answer:
                    log.info("Fullscreen webcam detected — will use full frame crop")
                    return {"fullscreen": True, "x": 0, "y": 0, "w": video_w, "h": video_h}

            except Exception as e:
                log.warning(f"Fullscreen detection failed: {e}")

        # Normal corner webcam — use verified coordinates
        return get_default_webcam(channel, video_w, video_h)

    # Unknown channel — try Claude vision
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY — no webcam detection")
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
                        "text": f"""This is a frame from a Kick.com live stream at {video_w}x{video_h} pixels.
Find the streamer's webcam showing their FACE — usually in a corner, 200-500px wide.
Reply ONLY with JSON:
{{"has_webcam": true, "x": 1400, "y": 0, "w": 480, "h": 380}}
or {{"has_webcam": false}}"""
                    }
                ]
            }]
        )

        text = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        log.info(f"Claude webcam response: {text}")
        result = json.loads(text)

        if not result.get("has_webcam"):
            return None

        cam = {k: int(result[k]) for k in ["x", "y", "w", "h"]}

        if cam["w"] < 100 or cam["h"] < 100:
            return None
        if cam["x"] + cam["w"] > video_w or cam["y"] + cam["h"] > video_h:
            return None

        log.info(f"Claude detected webcam: x={cam['x']} y={cam['y']} {cam['w']}x{cam['h']}")
        return cam

    except Exception as e:
        log.warning(f"Webcam detection failed: {e}")
        return None


# =============================================================================
# Step 4 — Crop to 9:16 vertical layout
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
    w, h = get_video_dimensions(input_path)
    log.info(f"Source dimensions: {w}x{h}")

    out_w = 608
    out_h = 1080
    cam_h = int(out_h * 0.35)    # 378px — top 35%
    content_h = out_h - cam_h    # 702px — bottom 65%

    frame_path = input_path.with_suffix(".jpg")
    has_frame = extract_frame(input_path, frame_path)

    cam = None
    if has_frame:
        cam = detect_webcam(frame_path, w, h, channel)
        frame_path.unlink(missing_ok=True)

    if cam:
        # Check if fullscreen mode detected
        if cam.get("fullscreen"):
            log.info("Fullscreen mode — cropping webcam to 9:16 centre...")
            # Centre crop the full frame to 9:16
            target_w = int(h * 9 / 16)
            crop_x = max(0, (w - target_w) // 2)
            cmd = [
                "ffmpeg", "-y",
                "-i", str(input_path),
                "-vf", f"crop={target_w}:{h}:{crop_x}:0,scale={out_w}:{out_h}",
                "-c:v", "libx264",
                "-c:a", "aac",
                "-preset", "fast",
                str(output_path)
            ]
        else:
            log.info(f"Building 35/65 cam+content layout...")
            log.info(f"Webcam: x={cam['x']} y={cam['y']} {cam['w']}x{cam['h']}")

            cc = settings.get_content_crop(channel, w, h)
            if cc:
                content_crop = f"crop={cc['w']}:{cc['h']}:{cc['x']}:{cc['y']}"
                log.info(f"Using .env content crop for #{channel}: {cc}")
            else:
                if cam["x"] > w * 0.5:
                    content_w = cam["x"]
                    content_x = 0
                else:
                    content_w = w - (cam["x"] + cam["w"])
                    content_x = cam["x"] + cam["w"]
                content_crop = f"crop={content_w}:{h}:{content_x}:0"
                log.info(f"Auto content crop: {content_crop}")

            vf = (
                f"[0:v]split=2[cam_src][content_src];"
                f"[cam_src]crop={cam['w']}:{cam['h']}:{cam['x']}:{cam['y']},"
                f"scale={out_w}:{cam_h}:force_original_aspect_ratio=increase,"
                f"crop={out_w}:{cam_h}[cam_out];"
                f"[content_src]{content_crop},"
                f"scale={out_w}:{content_h}[content_out];"
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
# Step 5 — Add word-pair captions with Whisper
# =============================================================================

def add_captions(input_path: Path, output_path: Path) -> bool:
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        log.warning("faster-whisper not installed — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    log.info("Transcribing with Whisper for word-pair captions...")
    srt_path = input_path.with_suffix(".srt")

    try:
        model = WhisperModel("tiny", compute_type="int8")
        segments, _ = model.transcribe(
            str(input_path),
            beam_size=1,
            word_timestamps=True
        )

        def fmt_time(t):
            h, r = divmod(t, 3600)
            m, s = divmod(r, 60)
            ms = int((s % 1) * 1000)
            return f"{int(h):02}:{int(m):02}:{int(s):02},{ms:03}"

        # Group into pairs of 2 words
        entries = []
        for seg in segments:
            if hasattr(seg, "words") and seg.words:
                words = seg.words
                for i in range(0, len(words), 2):
                    pair = words[i:i+2]
                    start = pair[0].start
                    end = pair[-1].end
                    text = " ".join(w.word.strip() for w in pair).upper()
                    entries.append((start, end, text))
            else:
                entries.append((seg.start, seg.end, seg.text.strip().upper()))

        with open(srt_path, "w") as f:
            for i, (start, end, text) in enumerate(entries, 1):
                end = max(end, start + 0.15)
                f.write(f"{i}\n{fmt_time(start)} --> {fmt_time(end)}\n{text}\n\n")

        log.info(f"Word-pair captions: {len(entries)} entries")

    except Exception as e:
        log.warning(f"Whisper failed: {e} — skipping captions")
        shutil.copy(input_path, output_path)
        return True

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", (
            f"subtitles={srt_path}:force_style='"
            "FontName=LuckiestGuy-Regular,"
            "FontSize=20,"
            "Bold=1,"
            "PrimaryColour=&H00FFFFFF,"
            "OutlineColour=&H00000000,"
            "BackColour=&H00000000,"
            "Outline=2,"
            "Shadow=1,"
            "MarginV=160'"
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
    log.info(f"Final clip with captions: {output_path}")
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

    raw     = tmp / f"{slug}_raw.ts"
    trimmed = tmp / f"{slug}_trimmed.ts"
    clean   = tmp / f"{slug}_clean.ts"
    cropped = tmp / f"{slug}_cropped.mp4"
    scored  = tmp / f"{slug}_scored.mp4"
    sfx_out = tmp / f"{slug}_sfx.mp4"
    final   = CLIPS_DIR / f"{slug}_final.mp4"

    # Use pre-extracted clip from buffer if available
    local_clip = moment.get("local_clip_path", "")
    if local_clip and Path(local_clip).exists():
        log.info(f"Using pre-extracted clip from buffer: {local_clip}")
        shutil.copy(local_clip, raw)
        Path(local_clip).unlink(missing_ok=True)
    elif not record_live_segment(channel, DURATION, raw):
        return None

    # Smart trim disabled
    shutil.copy(raw, trimmed)
    raw.unlink(missing_ok=True)

    # Vocals (disabled)
    if not separate_vocals(trimmed, clean):
        return None

    # Crop to 9:16
    if not crop_to_vertical(clean, cropped, channel):
        return None

    try:
        from agents.music import mix_music
        trigger_messages = moment.get("trigger_messages", [])
        mix_music(cropped, scored, trigger_messages)
    except Exception as e:
        log.warning(f"Music mix failed: {e} — skipping")
        shutil.copy(cropped, scored)

    # NEW — SFX disabled
    shutil.copy(scored, sfx_out)

    # Captions
    if not add_captions(sfx_out, final):
        return None

    # Cleanup
    for f in [trimmed, clean, cropped, scored, sfx_out]:
        f.unlink(missing_ok=True)

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
