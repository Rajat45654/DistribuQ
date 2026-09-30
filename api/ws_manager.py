"""
WebSocket connection manager for the DistribuQ live dashboard.

Maintains the set of active WebSocket connections and fans out events
received from the Redis pub/sub channel to all of them.
"""

import asyncio
import json
import logging
from typing import Set

from fastapi import WebSocket

logger = logging.getLogger("distribuq.ws")


class ConnectionManager:
    """Thread-safe set of active WebSocket connections with broadcast support."""

    def __init__(self):
        self._connections: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        logger.info("WS client connected. Total: %d", len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.info("WS client disconnected. Total: %d", len(self._connections))

    async def broadcast(self, message: str) -> None:
        """Send a raw JSON string to all connected clients. Dead connections are pruned."""
        if not self._connections:
            return
        dead: Set[WebSocket] = set()
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._connections.discard(ws)

    @property
    def count(self) -> int:
        return len(self._connections)


# Singleton used across the whole API process
manager = ConnectionManager()


async def redis_subscriber_loop(redis_url: str) -> None:
    """
    Long-running async task that subscribes to the Redis pub/sub events channel
    and fans every message out to all connected WebSocket clients.

    Should be started as a background task in the API lifespan.
    """
    import redis.asyncio as aioredis
    from api.events import EVENTS_CHANNEL

    client: aioredis.Redis | None = None
    pubsub = None

    while True:
        try:
            client = aioredis.from_url(redis_url, decode_responses=True)
            pubsub = client.pubsub()
            await pubsub.subscribe(EVENTS_CHANNEL)
            logger.info("Redis subscriber listening on channel '%s'", EVENTS_CHANNEL)

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message.get("data", "")
                if manager.count > 0:
                    await manager.broadcast(data)

        except asyncio.CancelledError:
            logger.info("Redis subscriber task cancelled - shutting down.")
            break
        except Exception as exc:
            logger.error("Redis subscriber error: %s - reconnecting in 2s", exc)
            await asyncio.sleep(2)
        finally:
            if pubsub:
                try:
                    await pubsub.unsubscribe(EVENTS_CHANNEL)
                    await pubsub.aclose()
                except Exception:
                    pass
            if client:
                try:
                    await client.aclose()
                except Exception:
                    pass
