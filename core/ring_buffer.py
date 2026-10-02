from collections import deque
from typing import Dict, Optional


class TickRingBuffer:
    """Fixed-capacity rolling buffer maintaining exact digit frequencies in O(1) time."""

    def __init__(self, capacity: int = 1000):
        self.capacity: int = capacity
        self.ticks: deque[int] = deque(maxlen=capacity)
        # Direct frequency table for digits 0-9
        self.counts: Dict[int, int] = {digit: 0 for digit in range(10)}

    def append(self, digit: int) -> None:
        """Add a new last digit, evicting the oldest if at capacity."""
        if not (0 <= digit <= 9):
            raise ValueError(f"Digit must be between 0 and 9, got {digit}")

        # If full, evict the oldest tick from frequency counters
        if len(self.ticks) == self.capacity:
            oldest_digit = self.ticks[0]
            self.counts[oldest_digit] -= 1

        self.counts[digit] += 1
        self.ticks.append(digit)

    @property
    def is_ready(self) -> bool:
        """True only when the buffer has reached the full sample capacity."""
        return len(self.ticks) == self.capacity

    def get_percentages(self) -> Optional[Dict[int, float]]:
        """Returns percentage distribution {0: pct, ..., 9: pct} in O(1) time."""
        total = len(self.ticks)
        if total == 0:
            return None

        # Fixed 10-iteration loop over digits 0..9 (strictly O(1))
        return {digit: (count / total) * 100.0 for digit, count in self.counts.items()}
