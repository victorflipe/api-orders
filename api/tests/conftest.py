"""Fixtures de teste.

Os testes usam o Postgres real (a corrida depende da UNIQUE do Postgres), mas
num banco separado `<banco>_test`, criado sob demanda. O banco de desenvolvimento
nunca é tocado. O esquema vem das migrations (`alembic upgrade head`), o mesmo
caminho de produção, e não de `create_all`. O RabbitMQ é trocado por um coletor
em memória.
"""

import asyncio
import os
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

import database
import main
from database import Base

ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"


def _database_urls():
    base = make_url(database.DATABASE_URL)
    test_name = os.environ.get("TEST_DATABASE_NAME", f"{base.database}_test")
    # Trava de segurança: o esquema do banco de testes é apagado a cada execução
    assert test_name.endswith("_test"), "o banco de testes deve terminar em _test"
    return base.set(database="postgres"), base.set(database=test_name)


async def _reset_database(admin_url, test_url) -> None:
    """Cria o banco de testes se faltar e zera o esquema `public`."""
    admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    async with admin.connect() as conn:
        exists = await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": test_url.database},
        )
        if not exists:
            await conn.execute(text(f'CREATE DATABASE "{test_url.database}"'))
    await admin.dispose()

    eng = create_async_engine(test_url, poolclass=NullPool)
    async with eng.begin() as conn:
        # Um comando por execução: o asyncpg não aceita vários em um só
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
    await eng.dispose()


@pytest.fixture(scope="session")
def migrated_database():
    """Aplica `alembic upgrade head` uma vez por execução, num banco zerado.

    É uma fixture síncrona de propósito: o env.py do Alembic chama
    asyncio.run(), que não pode rodar dentro de um event loop já ativo.
    """
    admin_url, test_url = _database_urls()
    asyncio.run(_reset_database(admin_url, test_url))

    config = Config(str(ALEMBIC_INI))
    config.attributes["url"] = test_url.render_as_string(hide_password=False)
    command.upgrade(config, "head")
    return test_url


def _truncate_statement(preparer) -> str | None:
    """TRUNCATE de todas as tabelas dos modelos, para não precisar listar uma a
    uma. A tabela de controle do Alembic nunca entra: apagá-la faria o próximo
    `upgrade` recriar tudo. Só enxerga tabelas registradas no `Base.metadata`.
    """
    names = [
        preparer.quote(table.name)
        for table in Base.metadata.sorted_tables
        if table.name != "alembic_version"
    ]
    if not names:
        return None
    return f"TRUNCATE {', '.join(names)} RESTART IDENTITY CASCADE"


@pytest_asyncio.fixture
async def engine(migrated_database):
    # NullPool: cada conexão pertence ao event loop do teste que a abriu
    eng = create_async_engine(migrated_database, poolclass=NullPool)
    statement = _truncate_statement(eng.dialect.identifier_preparer)
    if statement:
        async with eng.begin() as conn:
            await conn.execute(text(statement))
    yield eng
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
