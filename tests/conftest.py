import asyncio
import sys
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.main import app
from worker.db import init_worker_db_pool, close_worker_db_pool


@pytest_asyncio.fixture(loop_scope="function")
async def client():
    async with app.router.lifespan_context(app):
        await init_worker_db_pool()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
        await close_worker_db_pool()
