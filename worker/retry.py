import random
from datetime import datetime, timedelta, timezone


def compute_backoff_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    jitter: bool = True,
) -> float:
    """
    Computes exponential backoff delay with jitter.
    Formula: min(max_delay, base_delay * (2 ** (attempt - 1))) + jitter
    """
    if attempt < 1:
        attempt = 1

    # Exponential growth: 2^(attempt - 1)
    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))

    if jitter:
        # Full jitter between 0 and 0.5 * delay to avoid thundering herd problem
        delay += random.uniform(0, 0.5 * delay)

    return min(max_delay, delay)


def compute_next_retry_at(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    jitter: bool = True,
) -> datetime:
    """
    Returns the UTC timestamp when the next retry attempt is due.
    """
    delay_seconds = compute_backoff_delay(
        attempt=attempt,
        base_delay=base_delay,
        max_delay=max_delay,
        jitter=jitter,
    )
    return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
