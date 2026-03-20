"""
config/settings.py — loads .env and exposes typed config values

Per-streamer config supported via .env:
    KICK_CHANNELS=odablock,greg,xqc

    # Per-streamer webcam (x%,y%,w%,h% as decimals from calibrator tool)
    WEBCAM_odablock=0.7708,0.0611,0.2036,0.2778
    WEBCAM_greg=0.0,0.0,0.25,0.30

    # Per-streamer content crop
    CONTENT_odablock=0.0,0.0,0.673,0.687

    # Per-streamer hype thresholds
    THRESHOLD_odablock=60
    THRESHOLD_greg=10

    # Per-streamer cooldown
    COOLDOWN_odablock=120
    COOLDOWN_greg=60
"""
import os
from dotenv import load_dotenv
import json

load_dotenv()


class Settings:
    # Anthropic
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    # GitHub
    GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
    GITHUB_REPO: str = os.getenv("GITHUB_REPO", "")

    # Scout — global defaults (per-streamer overrides via helper functions)
    KICK_CHANNELS: list[str] = [
        c.strip() for c in os.getenv("KICK_CHANNELS", "").split(",") if c.strip()
    ]
    HYPE_WINDOW_SECONDS: int = int(os.getenv("HYPE_WINDOW_SECONDS", 10))
    HYPE_THRESHOLD: int = int(os.getenv("HYPE_THRESHOLD", 30))
    HYPE_COOLDOWN_SECONDS: int = int(os.getenv("HYPE_COOLDOWN_SECONDS", 120))

    # Clipper
    CLIP_PADDING_BEFORE: int = int(os.getenv("CLIP_PADDING_BEFORE", 20))
    CLIP_PADDING_AFTER: int = int(os.getenv("CLIP_PADDING_AFTER", 10))

    # YouTube
    YOUTUBE_CLIENT_ID: str = os.getenv("YOUTUBE_CLIENT_ID", "")
    YOUTUBE_CLIENT_SECRET: str = os.getenv("YOUTUBE_CLIENT_SECRET", "")
    YOUTUBE_REFRESH_TOKEN: str = os.getenv("YOUTUBE_REFRESH_TOKEN", "")

    # Discord
    DISCORD_BOT_TOKEN: str = os.getenv("DISCORD_BOT_TOKEN", "")
    DISCORD_CHANNEL_ID: str = os.getenv("DISCORD_CHANNEL_ID", "1482642034203426848")

    # Output paths
    CLIPS_DIR: str = os.path.join(os.path.dirname(__file__), "..", "output", "clips")
    LOGS_DIR: str = os.path.join(os.path.dirname(__file__), "..", "output", "logs")
    MOMENTS_FILE: str = os.path.join(
        os.path.dirname(__file__), "..", "output", "pending_moments.jsonl"
    )

    # =========================================================================
    # Per-streamer helpers
    # =========================================================================

    def get_threshold(self, channel: str) -> int:
        """Get hype threshold for a specific channel."""
        return int(os.getenv(f"THRESHOLD_{channel.upper()}", self.HYPE_THRESHOLD))

    def get_cooldown(self, channel: str) -> int:
        """Get cooldown for a specific channel."""
        return int(os.getenv(f"COOLDOWN_{channel.upper()}", self.HYPE_COOLDOWN_SECONDS))

    def get_webcam(self, channel: str, video_w: int, video_h: int) -> dict | None:
        # Try STREAMER_CONFIG first (GitHub Actions)
        config_raw = os.getenv("STREAMER_CONFIG")
        if config_raw:
            try:
                val = json.loads(config_raw).get(channel, {}).get("webcam")
            except Exception:
                val = None
        else:
            # Fall back to individual .env vars (PythonAnywhere)
            val = os.getenv(f"WEBCAM_{channel.upper()}")

        if not val:
            return None
        try:
            x_pct, y_pct, w_pct, h_pct = map(float, val.split(","))
            return {
                "x": int(video_w * x_pct),
                "y": int(video_h * y_pct),
                "w": int(video_w * w_pct),
                "h": int(video_h * h_pct),
            }
        except Exception:
            return None

    def get_content_crop(self, channel: str, video_w: int, video_h: int) -> dict | None:
        # Try STREAMER_CONFIG first (GitHub Actions)
        config_raw = os.getenv("STREAMER_CONFIG")
        if config_raw:
            try:
                val = json.loads(config_raw).get(channel, {}).get("content")
            except Exception:
                val = None
        else:
            # Fall back to individual .env vars (PythonAnywhere)
            val = os.getenv(f"CONTENT_{channel.upper()}")

        if not val:
            return None
        try:
            x_pct, y_pct, w_pct, h_pct = map(float, val.split(","))
            return {
                "x": int(video_w * x_pct),
                "y": int(video_h * y_pct),
                "w": int(video_w * w_pct),
                "h": int(video_h * h_pct),
            }
        except Exception:
            return None

settings = Settings()
