"""Fixtures de teste.

Os testes usam o Postgres real (a corrida depende da UNIQUE do Postgres), mas
num banco separado `<banco>_test`, criado sob demanda. O banco de desenvolvimento
nunca é tocado. O RabbitMQ é trocado por um coletor em memória.
"""

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

import database
import main
from database import Base


def _database_urls():
    base = make_url(database.DATABASE_URL)
    test_name = os.environ.get("TEST_DATABASE_NAME", f"{base.database}_test")
    # Trava de segurança: create_all/drop_all nunca podem rodar no banco real
    assert test_name.endswith("_test"), "o banco de testes deve terminar em _test"
    return base.set(database="postgres"), base.set(database=test_name)


@pytest_asyncio.fixture
async def engine():
    admin_url, test_url = _database_urls()

    admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    async with admin.connect() as conn:
        exists = await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": test_url.database},
        )
        if not exists:
            await conn.execute(text(f'CREATE DATABASE "{test_url.database}"'))
    await admin.dispose()

    # NullPool: cada conexão pertence ao event loop do teste que a abriu
    eng = create_async_engine(test_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest.fixture
def published(monkeypatch):
    """Lista dos eventos que a API tentou publicar (RabbitMQ substituído)."""
    events: list[dict] = []

    async def fake_publish(payload: dict) -> None:
        events.append(payload)

    monkeypatch.setattr(main.messaging, "publish_order_created", fake_publish)
    return events


@pytest_asyncio.fixture
async def client(engine, published):
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_session():
        async with sessions() as session:
            yield session

    main.app.dependency_overrides[main.get_session] = override_get_session
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http
    main.app.dependency_overrides.clear()


@pytest.fixture
def count_orders(engine):
    async def _count() -> int:
        async with engine.connect() as conn:
            return await conn.scalar(text("SELECT count(*) FROM orders"))

    return _count
