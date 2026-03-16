"""
agents/chat_overlay.py — Scrolling chat overlay

Burns chat messages as a scrolling overlay on top of the game section
of the clip. Messages scroll upward like a real chat feed.

Uses ffmpeg drawtext filter with timed entries per message.
"""
import json
import logging
import subprocess
import shutil
import re
from pathlib import Path

log = logging.getLogger("chat_overlay")

# Chat overlay settings
CHAT_X = 10              # pixels from left edge of frame
CHAT_Y_START = 0.55      # start at 55% from top (middle of game section)
FONT_SIZE = 16
FONT_COLOR = "white"
OUTLINE_COLOR = "black"
BG_ALPHA = 0.45          # semi-transparent background behind text
MSG_DURATION = 3.0       # seconds each message stays visible
MSG_INTERVAL = 0.8       # seconds between each message appearing
MAX_MESSAGES = 12        # max messages to show


def clean_message(text: str) -> str:
    """
    Clean chat message for display:
    - Replace emote tags like [emote:12345:KEKW] with just the name
    - Remove special characters that break ffmpeg drawtext
    - Truncate long messages
    """
    # Replace emote tags with just the emote name
    text = re.sub(r'\[emote:\d+:(\w+)\]', r'[\1]', text)

    # Remove characters that break ffmpeg drawtext
    text = text.replace("'", "").replace('"', "").replace("\\", "")
    text = text.replace(":", " ").replace("%", "pct").replace("\n", " ")

    # Remove non-ASCII characters
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Truncate
    if len(text) > 40:
        text = text[:37] + "..."

    return text.strip()


def build_chat_overlay(input_path: Path, output_path: Path,
                       trigger_messages: list[str],
                       clip_duration: float) -> bool:
    """
    Add scrolling chat overlay to the clip.
    Messages appear one by one scrolling upward over the game section.
    """
    if not trigger_messages:
        log.info("No chat messages — skipping overlay")
        shutil.copy(input_path, output_path)
        return True

    # Clean and prepare messages
    cleaned = []
    for msg in trigger_messages:
        # Format: "username: content"
        if ": " in msg:
            username, content = msg.split(": ", 1)
            content = clean_message(content)
            username = clean_message(username)
            if content:
                cleaned.append(f"{username}- {content}")
        else:
            text = clean_message(msg)
            if text:
                cleaned.append(text)

    if not cleaned:
        log.info("No valid messages after cleaning — skipping overlay")
        shutil.copy(input_path, output_path)
        return True

    # Limit messages
    messages = cleaned[:MAX_MESSAGES]
    log.info(f"Adding {len(messages)} chat messages as overlay")

    # Get video dimensions
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_streams", str(input_path)],
        capture_output=True, text=True
    )
    streams = json.loads(probe.stdout).get("streams", [])
    video = next((s for s in streams if s["codec_type"] == "video"), None)
    if not video:
        shutil.copy(input_path, output_path)
        return True

    vid_w = int(video["width"])
    vid_h = int(video["height"])

    # Space messages evenly across the clip duration
    # Start appearing at 10% into clip, finish at 90%
    usable_duration = clip_duration * 0.8
    start_time = clip_duration * 0.1

    if len(messages) > 1:
        interval = min(MSG_INTERVAL, usable_duration / len(messages))
    else:
        interval = 0

    # Build drawtext filter for each message
    # Messages stack upward — each new message pushes older ones up
    drawtext_filters = []

    for i, msg in enumerate(messages):
        t_start = start_time + (i * interval)
        t_end = min(t_start + MSG_DURATION, clip_duration - 0.5)

        if t_start >= clip_duration:
            break

        # Y position — messages scroll up as more appear
        # Bottom message starts at CHAT_Y_START, each new one is above it
        base_y = int(vid_h * CHAT_Y_START)
        line_height = FONT_SIZE + 6
        y_pos = base_y - (i * line_height)

        if y_pos < int(vid_h * 0.4):  # don't go above game section
            break

        # Semi-transparent dark background box
        # drawbox for background
        box_filter = (
            f"drawbox="
            f"x={CHAT_X - 2}:"
            f"y={y_pos - FONT_SIZE}:"
            f"w=text_w+8:"
            f"h={FONT_SIZE + 4}:"
            f"color=black@{BG_ALPHA}:"
            f"t=fill:"
            f"enable='between(t,{t_start:.2f},{t_end:.2f})'"
        )

        # Text
        text_filter = (
            f"drawtext="
            f"text='{msg}':"
            f"fontsize={FONT_SIZE}:"
            f"fontcolor={FONT_COLOR}:"
            f"bordercolor={OUTLINE_COLOR}:"
            f"borderw=2:"
            f"x={CHAT_X}:"
            f"y={y_pos - FONT_SIZE + 2}:"
            f"enable='between(t,{t_start:.2f},{t_end:.2f})'"
        )

        drawtext_filters.append(text_filter)

    if not drawtext_filters:
        shutil.copy(input_path, output_path)
        return True

    # Chain all filters together
    vf = ",".join(drawtext_filters)

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-vf", vf,
        "-c:v", "libx264",
        "-c:a", "copy",
        "-preset", "fast",
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.warning(f"Chat overlay failed: {result.stderr[-400:]} — using original")
        shutil.copy(input_path, output_path)
        return True

    log.info(f"Chat overlay added: {len(drawtext_filters)} messages")
    return True