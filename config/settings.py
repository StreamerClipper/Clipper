"""
config/settings.py — loads .env and exposes typed config values
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Anthropic (Publisher agent)
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    # GitHub (Scout pushes moments here to trigger Clipper)
    GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
    GITHUB_REPO: str = os.getenv("GITHUB_REPO", "")  # e.g. "username/clipbot"

    # Scout
    KICK_CHANNELS: list[str] = os.getenv("KICK_CHANNELS", "").split(",")
    HYPE_WINDOW_SECONDS: int = int(os.getenv("HYPE_WINDOW_SECONDS", 10))
    HYPE_THRESHOLD: int = int(os.getenv("HYPE_THRESHOLD", 15))
    HYPE_COOLDOWN_SECONDS: int = int(os.getenv("HYPE_COOLDOWN_SECONDS", 60))

    # Clipper (used inside GitHub Actions)
    CLIP_PADDING_BEFORE: int = int(os.getenv("CLIP_PADDING_BEFORE", 20))
    CLIP_PADDING_AFTER: int = int(os.getenv("CLIP_PADDING_AFTER", 10))

    # Output paths
    CLIPS_DIR: str = os.path.join(os.path.dirname(__file__), "..", "output", "clips")
    LOGS_DIR: str = os.path.join(os.path.dirname(__file__), "..", "output", "logs")
    MOMENTS_FILE: str = os.path.join(
        os.path.dirname(__file__), "..", "output", "pending_moments.jsonl"
    )


settings = Settings()
