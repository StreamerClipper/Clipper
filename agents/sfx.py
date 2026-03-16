"""
agents/sfx.py — Sound effect detection and mixing

Analyzes clip content using chat messages and Claude vision to detect
key moments, then mixes in the appropriate sound effect at the right timestamp.

Trigger map:
  KO / kill        → sword slash
  Death / RIP      → OSRS death sound
  FAAHHH / shotgun → shotgun fah
  Hype / insane    → mentality sound effect
"""
import json
import logging
import os
import subprocess
import shutil
from pathlib import Path

log = logging.getLogger("sfx")

SFX_DIR = Path("music/sfx")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# =============================================================================
# Sound effect library — maps trigger name to filename
# =============================================================================

SFX_FILES = {
    "sword_slash":  "Sword Slash.mp3",
    "death_sound":  "OSRS death sound.m4a",
    "shotgun_fah":  "Shotgun Fah.m4a",
    "mentality":    "Mentality Sound Effect.mp3",
}

# Chat keyword triggers — maps keywords to sound effect name
CHAT_TRIGGERS = {
    "sword_slash":  ["ko", "clapped", "gg", "dead", "killed", "rekt", "destroyed"],
    "death_sound":  ["rip", "f ", " f\n", "died", "dead", "skull", "ded"],
    "shotgun_fah":  ["fah", "fahhh", "faahhh", "shotgun", "boom"],
    "mentality":    ["insane", "crazy", "no way", "actual", "mentality", "bro what"],
}

# Minimum confidence to trigger (fraction of recent messages containing keyword)
TRIGGER_THRESHOLD = 0.2  # 20% of messages contain the keyword


# =============================================================================
# Detection
# =============================================================================

def detect_sfx_from_chat(trigger_messages: list[str]) -> tuple[str | None, float]:
    """
    Scan chat messages for sound effect triggers.
    Returns (sfx_name, confidence) or (None, 0).
    """
    if not trigger_messages:
        return None, 0.0

    total = len(trigger_messages)
    combined = " ".join(trigger_messages).lower()

    scores = {}
    for sfx_name, keywords in CHAT_TRIGGERS.items():
        count = sum(1 for kw in keywords if kw in combined)
        if count > 0:
            scores[sfx_name] = count / total

    if not scores:
        return None, 0.0

    best = max(scores, key=scores.get)
    confidence = scores[best]

    if confidence >= TRIGGER_THRESHOLD:
        log.info(f"Chat trigger detected: {best} (confidence: {confidence:.0%})")
        return best, confidence

    return None, 0.0


def detect_sfx_from_vision(clip_path: Path) -> tuple[str | None, float]:
    """
    Use Claude vision to detect key moments in the clip.
    Extracts a frame from the middle of the clip and analyzes it.
    Returns (sfx_name, confidence) or (None, 0).
    """
    if not ANTHROPIC_API_KEY:
        return None, 0.0

    try:
        import base64
        import anthropic

        # Extract frame from middle of clip
        frame_path = Path(f"/tmp/sfx_frame_{clip_path.stem}.jpg")
        result = subprocess.run([
            "ffmpeg", "-y", "-i", str(clip_path),
            "-ss", "15",  # middle of 30s clip
            "-vframes", "1", "-q:v", "2",
            str(frame_path)
        ], capture_output=True, text=True)

        if result.returncode != 0 or not frame_path.exists():
            return None, 0.0

        with open(frame_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")
        frame_path.unlink(missing_ok=True)

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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
                        "text": """This is a frame from an OSRS (Old School RuneScape) stream clip.

Identify what is happening. Reply with ONLY one of these options:
- "ko" — a player character just died or got knocked out (skull icon, death animation, large damage number)
- "levelup" — level up interface is showing
- "loot" — valuable items dropped on the ground
- "normal" — nothing special happening

Reply with just the single word."""
                    }
                ]
            }]
        )

        detection = response.content[0].text.strip().lower()
        log.info(f"Vision detection: {detection}")

        mapping = {
            "ko": ("sword_slash", 0.8),
            "levelup": ("mentality", 0.7),
            "loot": ("mentality", 0.6),
            "normal": (None, 0.0),
        }
        return mapping.get(detection, (None, 0.0))

    except Exception as e:
        log.warning(f"Vision SFX detection failed: {e}")
        return None, 0.0


def detect_sfx(clip_path: Path, trigger_messages: list[str]) -> tuple[str | None, float]:
    """
    Combined detection — uses both chat and vision.
    Chat takes priority if confident, otherwise falls back to vision.
    """
    # Try chat first (faster, cheaper)
    sfx, confidence = detect_sfx_from_chat(trigger_messages)
    if sfx and confidence >= TRIGGER_THRESHOLD:
        return sfx, confidence

    # Fall back to vision
    sfx_vision, conf_vision = detect_sfx_from_vision(clip_path)
    if sfx_vision:
        return sfx_vision, conf_vision

    # Use chat result even if below threshold
    if sfx:
        return sfx, confidence

    return None, 0.0


# =============================================================================
# Mixing
# =============================================================================

def get_sfx_path(sfx_name: str) -> Path | None:
    """Get the full path to a sound effect file."""
    filename = SFX_FILES.get(sfx_name)
    if not filename:
        return None
    path = SFX_DIR / filename
    if not path.exists():
        log.warning(f"SFX file not found: {path}")
        return None
    return path


def mix_sfx(input_path: Path, output_path: Path,
            trigger_messages: list[str],
            sfx_volume: float = 0.8,
            sfx_offset: float = 18.0) -> bool:
    """
    Detect and mix a sound effect into the clip.

    sfx_volume: volume of the sound effect (0.0-1.0)
    sfx_offset: when in the clip to play the SFX (seconds from start)
                default 18s = roughly at the hype peak in a 30s clip

    Falls back gracefully — returns True even if no SFX detected.
    """
    sfx_name, confidence = detect_sfx(input_path, trigger_messages)

    if not sfx_name:
        log.info("No SFX trigger detected — using clip as-is")
        shutil.copy(input_path, output_path)
        return True

    sfx_path = get_sfx_path(sfx_name)
    if not sfx_path:
        log.warning(f"SFX file missing for {sfx_name} — skipping")
        shutil.copy(input_path, output_path)
        return True

    log.info(f"Mixing SFX: {sfx_name} at t={sfx_offset:.0f}s (volume: {sfx_volume:.0%})")

    # Get clip duration
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", str(input_path)],
        capture_output=True, text=True
    )
    try:
        clip_duration = float(json.loads(probe.stdout)["format"]["duration"])
    except Exception:
        clip_duration = 30.0

    # Mix SFX at the specified offset
    # adelay delays the SFX by sfx_offset seconds
    # amix blends it with the original audio
    delay_ms = int(sfx_offset * 1000)

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-i", str(sfx_path),
        "-filter_complex", (
            f"[1:a]volume={sfx_volume},adelay={delay_ms}|{delay_ms}[sfx];"
            f"[0:a][sfx]amix=inputs=2:duration=first:dropout_transition=0[aout]"
        ),
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac",
        "-t", str(clip_duration),
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.warning(f"SFX mix failed: {result.stderr[-300:]} — using original")
        shutil.copy(input_path, output_path)
        return True

    log.info(f"SFX mixed successfully: {sfx_name} → {output_path}")
    return True