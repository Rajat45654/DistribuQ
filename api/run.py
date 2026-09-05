import asyncio
import selectors
import sys
import uvicorn

from api.config import settings


def get_loop_factory():
    if sys.platform == "win32":
        return lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    return None


async def start_server():
    config = uvicorn.Config(
        "api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level=settings.LOG_LEVEL.lower(),
        lifespan="on",
    )
    server = uvicorn.Server(config)
    await server.serve()


def main():
    print(f"Starting DistribuQ API server on {settings.API_HOST}:{settings.API_PORT}...")
    loop_factory = get_loop_factory()
    if loop_factory:
        asyncio.run(start_server(), loop_factory=loop_factory)
    else:
        asyncio.run(start_server())


if __name__ == "__main__":
    main()
