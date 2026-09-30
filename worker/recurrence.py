"""
Recurrence rule parser and next-run calculator for recurring jobs.

Supported formats:
  - Interval syntax: "@every 30s", "@every 5m", "@every 2h", "@every 1d"
  - Cron syntax (5 fields): "*/5 * * * *", "0 9 * * 1", etc.

For cron parsing we use the `croniter` library if available, falling back
to interval-only mode gracefully.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Optional


_INTERVAL_RE = re.compile(
    r"^@every\s+(?:(?P<days>\d+)d)?(?:(?P<hours>\d+)h)?(?:(?P<minutes>\d+)m)?(?:(?P<seconds>\d+)s)?$",
    re.IGNORECASE,
)


def parse_interval(rule: str) -> Optional[timedelta]:
    """
    Parses an "@every" interval rule and returns the corresponding timedelta.
    Examples: "@every 30s", "@every 5m", "@every 2h", "@every 1d", "@every 1h30m"
    Returns None if the rule is not an interval rule.
    """
    m = _INTERVAL_RE.match(rule.strip())
    if not m:
        return None

    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)

    total = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if total.total_seconds() <= 0:
        raise ValueError(f"Invalid interval rule '{rule}': total duration must be positive")
    return total


def next_run_from_interval(rule: str, after: Optional[datetime] = None) -> datetime:
    """
    Returns the next UTC run time for an "@every" interval rule.
    `after` defaults to now (UTC) if not provided.
    """
    delta = parse_interval(rule)
    if delta is None:
        raise ValueError(f"Rule '{rule}' is not a valid interval rule")
    base = after or datetime.now(timezone.utc)
    return base + delta


def next_run_from_cron(rule: str, after: Optional[datetime] = None) -> datetime:
    """
    Returns the next UTC run time for a standard 5-field cron expression.
    Requires the `croniter` package.
    """
    try:
        from croniter import croniter  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "croniter is required for cron recurrence rules. "
            "Install it with: pip install croniter"
        ) from exc

    base = after or datetime.now(timezone.utc)
    # croniter expects a naive or tz-aware datetime; convert to UTC then back
    base_naive = base.replace(tzinfo=None)
    cron = croniter(rule, base_naive)
    next_naive = cron.get_next(datetime)
    return next_naive.replace(tzinfo=timezone.utc)


def compute_next_run(rule: str, after: Optional[datetime] = None) -> datetime:
    """
    Dispatches to the appropriate next-run calculator based on the rule format.
    - "@every ..." rules use interval arithmetic.
    - All other rules are treated as 5-field cron expressions.
    """
    if not rule or not rule.strip():
        raise ValueError("recurrence_rule must not be empty")

    stripped = rule.strip()
    if stripped.startswith("@every"):
        return next_run_from_interval(stripped, after=after)

    return next_run_from_cron(stripped, after=after)


def is_valid_recurrence_rule(rule: str) -> bool:
    """Returns True if the rule is parseable, False otherwise."""
    try:
        compute_next_run(rule)
        return True
    except Exception:
        return False
