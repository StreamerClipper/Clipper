"""
agents/trimmer.py — Smart clip trimmer

Analyzes audio energy in a clip to find and remove dead/silent sections,
keeping only the most active parts around the hype peak.

Target: 20-30 seconds of pure action from a 50-60 second raw clip.
"""
import json
import logging
import subprocess
import shutil
from pathlib import Path

log = logging.getLogger("trimmer")

TARGET_MIN = 20   # minimum clip length in seconds
TARGET_MAX = 30   # maximum clip length in seconds
SILENCE_THRESHOLD = -35   # dB — below this is considered silence
SILENCE_MIN_DURATION = 1.5  # seconds — minimum silence duration to remove


def detect_silence(input_path: Path) -> list[dict]:
    """
    Use ffmpeg silencedetect to find silent segments.
    Returns list of {start, end, duration} dicts.
    """
    result = subprocess.run([
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-af", f"silencedetect=noise={SILENCE_THRESHOLD}dB:d={SILENCE_MIN_DURATION}",
        "-f", "null", "-"
    ], capture_output=True, text=True)

    silent_segments = []
    current_start = None

    for line in result.stderr.splitlines():
        if "silence_start" in line:
            try:
                current_start = float(line.split("silence_start: ")[1])
            except (IndexError, ValueError):
                pass
        elif "silence_end" in line and current_start is not None:
            try:
                parts = line.split("silence_end: ")[1].split(" | ")
                end = float(parts[0])
                duration = float(parts[1].split("silence_duration: ")[1])
                silent_segments.append({
                    "start": current_start,
                    "end": end,
                    "duration": duration
                })
                current_start = None
            except (IndexError, ValueError):
                pass

    return silent_segments


def get_clip_duration(input_path: Path) -> float:
    """Get total clip duration in seconds."""
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", str(input_path)],
        capture_output=True, text=True
    )
    try:
        return float(json.loads(probe.stdout)["format"]["duration"])
    except Exception:
        return 30.0


def find_best_segment(total_duration: float,
                      silent_segments: list[dict],
                      peak_offset_in_clip: float) -> tuple[float, float]:
    """
    Find the best start/end time to keep TARGET_MIN-TARGET_MAX seconds
    of the most active content, centred around the peak moment.

    Returns (start_time, end_time) in seconds.
    """
    target = (TARGET_MIN + TARGET_MAX) / 2  # aim for 25s

    # Build list of active (non-silent) regions
    active_regions = []
    prev_end = 0.0

    for seg in sorted(silent_segments, key=lambda x: x["start"]):
        if seg["start"] > prev_end:
            active_regions.append({"start": prev_end, "end": seg["start"]})
        prev_end = seg["end"]

    if prev_end < total_duration:
        active_regions.append({"start": prev_end, "end": total_duration})

    if not active_regions:
        # No active regions found — use middle section
        mid = total_duration / 2
        start = max(0, mid - target / 2)
        end = min(total_duration, start + target)
        return start, end

    # Find which active region contains the peak
    peak_region = None
    for region in active_regions:
        if region["start"] <= peak_offset_in_clip <= region["end"]:
            peak_region = region
            break

    if not peak_region:
        # Peak is in a silent section — use closest active region
        peak_region = min(active_regions,
                         key=lambda r: abs(r["start"] - peak_offset_in_clip))

    # Centre window around peak within active region
    peak_in_region = max(peak_region["start"],
                        min(peak_offset_in_clip, peak_region["end"]))
    start = max(0, peak_in_region - target * 0.6)  # 60% before peak
    end = min(total_duration, start + target)

    # Adjust if we're too close to the end
    if end - start < TARGET_MIN:
        start = max(0, end - TARGET_MIN)

    # Clamp to target range
    if end - start > TARGET_MAX:
        end = start + TARGET_MAX

    log.info(f"Best segment: {start:.1f}s - {end:.1f}s ({end-start:.1f}s)")
    return start, end


def trim_clip(input_path: Path, output_path: Path,
              peak_offset: float = 20.0) -> bool:
    """
    Trim a clip to remove dead sections and keep the best 20-30 seconds.

    peak_offset: estimated position of the hype peak in the clip (seconds).
                 Default 20s assumes CLIP_PADDING_BEFORE=20.
    """
    total_duration = get_clip_duration(input_path)
    log.info(f"Trimming {total_duration:.1f}s clip (peak at ~{peak_offset:.1f}s)")

    # If already short enough, skip trimming
    if total_duration <= TARGET_MAX:
        log.info(f"Clip already {total_duration:.1f}s — no trimming needed")
        shutil.copy(input_path, output_path)
        return True

    # Detect silent sections
    silent_segments = detect_silence(input_path)
    log.info(f"Found {len(silent_segments)} silent segment(s)")
    for s in silent_segments:
        log.info(f"  Silence: {s['start']:.1f}s - {s['end']:.1f}s ({s['duration']:.1f}s)")

    # Find best segment to keep
    start, end = find_best_segment(total_duration, silent_segments, peak_offset)
    duration = end - start

    log.info(f"Trimming to {start:.1f}s - {end:.1f}s ({duration:.1f}s)")

    # Extract the segment
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-ss", str(start),
        "-t", str(duration),
        "-vf", "scale=1920:1080",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "fast",
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.warning(f"Trim failed: {result.stderr[-300:]} — using original")
        shutil.copy(input_path, output_path)
        return True

    final_duration = get_clip_duration(output_path)
    log.info(f"Trimmed: {total_duration:.1f}s → {final_duration:.1f}s")
    return True