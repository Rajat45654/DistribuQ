import asyncio
import logging
from typing import Any
from worker.handlers.registry import register_handler

logger = logging.getLogger("distribuq.worker.handlers")


@register_handler("math.add")
@register_handler("test_math")
async def handle_math_add(a: int = 10, b: int = 20, **kwargs: Any) -> dict:
    """Adds two integers."""
    res = int(a) + int(b)
    return {"sum": res, **kwargs}


@register_handler("math.multiply")
async def handle_math_multiply(a: int = 1, b: int = 1, **kwargs: Any) -> dict:
    """Multiplies two integers."""
    return {"product": int(a) * int(b), **kwargs}


@register_handler("echo")
@register_handler("test_echo")
async def handle_echo(message: str = "Hello from DistribuQ!", **kwargs: Any) -> dict:
    """Echoes a message."""
    return {"echo": message, **kwargs}


@register_handler("sleep")
@register_handler("test_slow")
async def handle_sleep(seconds: float = 3.0, **kwargs: Any) -> dict:
    """Sleeps for given seconds to simulate long-running work."""
    sec = float(kwargs.get("duration", seconds))
    logger.info("Executing slow job: sleeping for %.1f seconds...", sec)
    await asyncio.sleep(sec)
    return {"slept_seconds": sec, **kwargs}


@register_handler("fail")
@register_handler("test_fail")
async def handle_fail(reason: str = "Simulated job failure", **kwargs: Any) -> Any:
    """Intentionally raises an exception to test failure transitions."""
    raise RuntimeError(reason)


_FLAKY_ATTEMPTS: dict[str, int] = {}


@register_handler("flaky")
async def handle_flaky(task_key: str = "default", fail_until_attempt: int = 2, **kwargs: Any) -> dict:
    """Fails until attempt reaches fail_until_attempt, then succeeds."""
    count = _FLAKY_ATTEMPTS.get(task_key, 0) + 1
    _FLAKY_ATTEMPTS[task_key] = count
    if count < fail_until_attempt:
        raise RuntimeError(f"Simulated transient error on attempt {count}")
    return {"recovered_at_attempt": count, "task_key": task_key, **kwargs}
