import asyncio
import logging
from typing import Any
from worker.handlers.registry import register_handler

logger = logging.getLogger("distribuq.worker.handlers")


@register_handler("math.add")
async def handle_math_add(a: int = 0, b: int = 0) -> dict:
    """Adds two integers."""
    res = a + b
    return {"sum": res}


@register_handler("math.multiply")
async def handle_math_multiply(a: int = 1, b: int = 1) -> dict:
    """Multiplies two integers."""
    return {"product": a * b}


@register_handler("echo")
async def handle_echo(message: str = "") -> dict:
    """Echoes a message."""
    return {"echo": message}


@register_handler("sleep")
async def handle_sleep(seconds: float = 1.0) -> dict:
    """Sleeps for given seconds to simulate long-running work."""
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}


@register_handler("fail")
async def handle_fail(reason: str = "Simulated job failure") -> Any:
    """Intentionally raises an exception to test failure transitions."""
    raise RuntimeError(reason)
