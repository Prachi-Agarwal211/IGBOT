from __future__ import annotations
from datetime import datetime


class EngagementAgent:
    """Stub engagement agent.

    Planned capabilities (future):
      - Fetch recent comments for recent posts since a timestamp.
      - Reply using safe templates with randomized delays.
      - Optional filters (keywords, toxicity checks).
    """

    def __init__(self) -> None:
        pass

    def run(self, since_utc_iso: str, max_replies: int = 10) -> int:
        """Placeholder no-op that validates the time string and returns 0.
        Returns number of replies made (0 for now).
        """
        # Validate format a bit to fail fast if malformed
        try:
            # allow trailing Z
            _ = datetime.fromisoformat(since_utc_iso.replace("Z", "+00:00"))
        except Exception as e:
            raise ValueError(f"Invalid --since (UTC ISO) '{since_utc_iso}': {e}")
        # No real actions yet
        return 0
