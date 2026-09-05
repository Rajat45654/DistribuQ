import inspect
import logging
from typing import Any, Callable, Dict

logger = logging.getLogger("distribuq.worker.registry")

HandlerFunc = Callable[..., Any]
_REGISTRY: Dict[str, HandlerFunc] = {}


def register_handler(job_type: str):
    """Decorator to register a handler for a given job type."""
    def decorator(fn: HandlerFunc):
        if job_type in _REGISTRY:
            logger.warning("Overwriting existing handler for type: %s", job_type)
        _REGISTRY[job_type] = fn
        return fn
    return decorator


def get_handler(job_type: str) -> HandlerFunc:
    if job_type not in _REGISTRY:
        raise KeyError(f"No handler registered for job type: '{job_type}'")
    return _REGISTRY[job_type]


async def execute_handler(fn: HandlerFunc, payload: Dict[str, Any]) -> Any:
    """Executes a handler, supporting both async and sync implementations."""
    if inspect.iscoroutinefunction(fn):
        return await fn(**payload)
    return fn(**payload)
