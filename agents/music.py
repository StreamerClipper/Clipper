"""
agents/music.py — OSRS vibe-matched background music

Detects the emotional vibe of a clip using chat messages and transcript,
then selects and mixes in a matching OSRS track at low volume.

Tracks are stored in music/osrs/ in the repo.
Run setup_music.py once to download them.
"""
import json
import logging
import os
import random
import subprocess
import shutil
from pathlib import Path

log = logging.getLogger("music")

MUSIC_DIR = Path("music/osrs")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MUSIC_VOLUME = 0.15  # 15% volume — subtle background, not overpowering

# =============================================================================
# Curated track list by vibe
# Each entry: (filename, description)
# =============================================================================

TRACKS = {
    "hype":  [("Fanfare.mp3",    "Victory fanfare — big moments")],
    "funny": [("Flute_Salad.mp3","Whimsical, silly — chaos")],
    "chill": [("Flute_Salad.mp3","Light and breezy — calm moments")],
    "loot":  [("Fanfare.mp3",    "Victory — big drops")],
    "sad":   [("Sad_Meadow.mp3", "Sorrowful — bad luck")],
}

# Internet Archive direct download URLs for each track
# Verified URLs from Runescape-OST-Classics on Internet Archive
_BASE = "https://archive.org/download/Runescape-OST-Classics"

TRACK_URLS = {
    "Scape_Main.mp3":     f"{_BASE}/Scape%20Main.mp3",
    "Adventure.mp3":      f"{_BASE}/Adventure.mp3",
    "Fanfare.mp3":        f"{_BASE}/Fanfare.mp3",
    "Autumn_Voyage.mp3":  f"{_BASE}/Autumn%20Voyage.mp3",
    "Flute_Salad.mp3":    f"{_BASE}/Flute%20Salad.mp3",
    "Baroque.mp3":        f"{_BASE}/Baroque.mp3",
    "Harmony.mp3":        f"{_BASE}/Harmony.mp3",
    "Sea_Shanty_2.mp3":   f"{_BASE}/Sea%20Shanty%202.mp3",
    "Yesteryear.mp3":     f"{_BASE}/Yesteryear.mp3",
    "Newbie_Melody.mp3":  f"{_BASE}/Newbie%20Melody.mp3",
    "Nightfall.mp3":      f"{_BASE}/Nightfall.mp3",
    "Medieval.mp3":       f"{_BASE}/Medieval.mp3",
    "Garden.mp3":         f"{_BASE}/Garden.mp3",
    "Scape_Sad.mp3":      f"{_BASE}/Scape%20Sad.mp3",
    "Dark.mp3":           f"{_BASE}/Dark.mp3",
    "Waterfall.mp3":      f"{_BASE}/Waterfall.mp3",
}


# =============================================================================
# Vibe detection via Claude
# =============================================================================

def detect_vibe(trigger_messages: list[str], transcript: str = "") -> str:
    """
    Use Claude to classify the clip vibe from chat messages and transcript.
    Returns one of: hype, funny, chill, loot, sad
    Defaults to 'hype' if detection fails.
    """
    if not ANTHROPIC_API_KEY:
        log.warning("No API key — defaulting to hype vibe")
        return "hype"

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        chat_sample = "\n".join(trigger_messages[-10:]) if trigger_messages else "(none)"
        transcript_sample = transcript[:300] if transcript else "(none)"

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=50,
            messages=[{
                "role": "user",
                "content": f"""Classify the vibe of this Kick stream clip moment.

Chat messages during the hype spike:
{chat_sample}

Streamer transcript (if available):
{transcript_sample}

Choose exactly ONE vibe from this list:
- hype: exciting combat, big plays, intense moments, PogChamp spam
- funny: KEKW floods, chaotic moments, trolling, absurd situations
- chill: skilling, relaxed chat, low energy moments
- loot: rare drops, big wins, valuable items, gambling wins
- sad: deaths, losses, bad luck, RIP spam, F in chat

Reply with ONLY the single word vibe, nothing else."""
            }]
        )

        vibe = response.content[0].text.strip().lower()
        if vibe in TRACKS:
            log.info(f"Detected vibe: {vibe}")
            return vibe
        else:
            log.warning(f"Unknown vibe '{vibe}' — defaulting to hype")
            return "hype"

    except Exception as e:
        log.warning(f"Vibe detection failed: {e} — defaulting to hype")
        return "hype"


# =============================================================================
# Track selection — avoids repeating recent tracks
# =============================================================================

RECENTLY_PLAYED: list[str] = []
MAX_RECENT = 5


def pick_track(vibe: str) -> Path | None:
    """Pick a random track for the vibe, avoiding recently played ones."""
    available = TRACKS.get(vibe, TRACKS["hype"])
    available_names = [t[0] for t in available]

    # Filter out recently played
    candidates = [t for t in available_names if t not in RECENTLY_PLAYED]
    if not candidates:
        candidates = available_names  # all were recent, reset

    chosen = random.choice(candidates)

    # Track recently played
    RECENTLY_PLAYED.append(chosen)
    if len(RECENTLY_PLAYED) > MAX_RECENT:
        RECENTLY_PLAYED.pop(0)

    track_path = MUSIC_DIR / chosen
    if not track_path.exists():
        log.warning(f"Track not found: {track_path} — skipping music")
        return None

    log.info(f"Selected track: {chosen} (vibe: {vibe})")
    return track_path


# =============================================================================
# Download tracks (run once in GitHub Actions setup)
# =============================================================================

def download_tracks():
    """Download all curated OSRS tracks from Internet Archive."""
    MUSIC_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    failed = 0

    for filename, url in TRACK_URLS.items():
        dest = MUSIC_DIR / filename
        if dest.exists():
            log.info(f"Already exists: {filename}")
            continue

        log.info(f"Downloading: {filename}...")
        result = subprocess.run(
            ["curl", "-sSL", "--max-redirs", "5", "-o", str(dest), url],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0 and dest.exists() and dest.stat().st_size > 10_000:
            log.info(f"Downloaded: {filename} ({dest.stat().st_size/1024:.0f}KB)")
            downloaded += 1
        else:
            log.warning(f"Failed to download: {filename}")
            dest.unlink(missing_ok=True)
            failed += 1

    log.info(f"Download complete: {downloaded} tracks, {failed} failed")


# =============================================================================
# Mix music into clip
# =============================================================================

def mix_music(input_path: Path, output_path: Path,
              trigger_messages: list[str], transcript: str = "") -> bool:
    """
    Detect vibe, pick matching OSRS track, and mix it into the clip.
    The track loops if shorter than the clip.
    Falls back to no music if anything fails.
    """
    # Detect vibe
    vibe = detect_vibe(trigger_messages, transcript)

    # Pick track
    track_path = pick_track(vibe)
    if not track_path:
        log.warning("No track available — skipping music mix")
        shutil.copy(input_path, output_path)
        return True

    # Get clip duration
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", str(input_path)],
        capture_output=True, text=True
    )
    try:
        duration = float(json.loads(probe.stdout)["format"]["duration"])
    except Exception:
        duration = 30.0

    log.info(f"Mixing '{track_path.name}' at {MUSIC_VOLUME*100:.0f}% volume into {duration:.0f}s clip...")

    # Mix: original vocals audio + OSRS music at low volume
    # -stream_loop -1 loops the music track if it's shorter than the clip
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-stream_loop", "-1",
        "-i", str(track_path),
        "-filter_complex", (
            f"[0:a]volume=1.0[voice];"          # keep voice at full volume
            f"[1:a]volume={MUSIC_VOLUME}[music];" # music at 15%
            f"[voice][music]amix=inputs=2:duration=first[aout]"  # mix, stop at clip end
        ),
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac",
        "-t", str(duration),
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.warning(f"Music mix failed: {result.stderr[-300:]} — using unmixed audio")
        shutil.copy(input_path, output_path)
        return True

    log.info(f"Music mixed successfully: {output_path}")
    return True
