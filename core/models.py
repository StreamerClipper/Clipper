"""
core/models.py — shared data structures used across all agents
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ChatMessage:
    channel: str
    username: str
    content: str
    timestamp: datetime
    stream_offset: float | None = None


@dataclass
class HypeMoment:
    channel: str
    stream_id: str
    peak_offset: float
    peak_time: datetime
    message_rate: float
    trigger_messages: list[str] = field(default_factory=list)
    clip_path: str | None = None
    posted: bool = False

    def to_dict(self) -> dict:
        return {
            "channel": self.channel,
            "stream_id": self.stream_id,
            "peak_offset": self.peak_offset,
            "peak_time": self.peak_time.isoformat(),
            "message_rate": round(self.message_rate, 2),
            "trigger_messages": self.trigger_messages,
        }

    def __str__(self):
        return (
            f"[{self.channel}] hype @ {self.peak_offset:.0f}s "
            f"({self.message_rate:.1f} msg/s)"
        )
