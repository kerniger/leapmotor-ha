"""Conservative cloud timestamp validation for access-state entities."""
from datetime import datetime
import math
import time

MAX_STATUS_AGE = 15 * 60
MAX_FUTURE_SKEW = 60


def timestamp_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return None
            number = parsed.timestamp()
        except (ValueError, OverflowError, TypeError):
            return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number / 1000 if number >= 1e12 else number


def is_fresh(value, now=None):
    timestamp = timestamp_seconds(value)
    if timestamp is None:
        return False
    age = (time.time() if now is None else now) - timestamp
    return -MAX_FUTURE_SKEW <= age <= MAX_STATUS_AGE
