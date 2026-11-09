import asyncio
import json

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

import worker

BODY = {"product": "camiseta", "value": "49.90"}


class FakeMessage:
    """Mensagem do aio-pika, registrando como o consumer a encerrou."""

    def __init__(self, message_id, body: bytes):
        self.message_id = message_id
        self.body = body
        self.calls: list[tuple] = []

    async def ack(self):
        self.calls.append(("ack",))

    async def nack(self, requeue=True):
        self.calls.append(("nack", requeue))

    async def reject(self, requeue=True):
        self.calls.append(("reject", requeue))


@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def deliver(session_factory):
    async def _deliver(message: FakeMessage) -> FakeMessage:
        await worker.on_message(message, session_factory)
        return message

    return _deliver


@pytest.fixture
def db(engine):
    async def _scalar(sql: str, **params):
        async with engine.connect() as conn:
            return await conn.scalar(text(sql), params)

    return _scalar


@pytest.fixture
def order_updates(engine):
    """Conta os UPDATE em `orders` que chegaram de fato ao banco."""
    statements: list[str] = []

    def before(conn, cursor, statement, *args):
        if statement.lstrip().upper().startswith("UPDATE ORDERS"):
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", before)
    yield statements
    event.remove(engine.sync_engine, "before_cursor_execute", before)


async def created_event(client, published) -> FakeMessage:
    """Cria um pedido pela API e monta a mensagem que ela publicaria."""
    response = await client.post("/orders", json=BODY)
    assert response.status_code == 201
    payload = published[-1]
    return FakeMessage(str(payload["id"]), json.dumps(payload).encode())


async def test_same_message_delivered_twice_updates_the_order_once(
    client, published, deliver, db, order_updates
):
    message = await created_event(client, published)
    order_id = int(message.message_id)

    first = await deliver(message)
    # Redelivery: mesma mensagem (mesmo message_id), objeto novo
    second = await deliver(FakeMessage(message.message_id, message.body))

    assert len(order_updates) == 1  # a segunda entrega nem tenta o UPDATE
    assert await db("SELECT status FROM orders WHERE id = :i", i=order_id) == "paid"
    assert await db("SELECT count(*) FROM processed_events") == 1
    # As duas entregas são confirmadas; nenhuma volta para a fila
    assert first.calls == [("ack",)]
    assert second.calls == [("ack",)]


async def test_concurrent_deliveries_of_the_same_message_update_once(
    client, published, deliver, db, order_updates
):
    message = await created_event(client, published)

    copies = [FakeMessage(message.message_id, message.body) for _ in range(5)]
    await asyncio.gather(*[deliver(m) for m in copies])

    assert len(order_updates) == 1
    assert await db("SELECT count(*) FROM processed_events") == 1
    assert all(m.calls == [("ack",)] for m in copies)


async def test_failure_rolls_back_and_requeues_without_ack(
    client, published, deliver, db, monkeypatch
):
    message = await created_event(client, published)
    order_id = int(message.message_id)

    async def boom(session, order_id):
        raise RuntimeError("banco caiu no meio")

    monkeypatch.setattr(worker, "mark_order_paid", boom)
    monkeypatch.setattr(worker, "RETRY_DELAY", 0)

    await deliver(message)

    assert message.calls == [("nack", True)]
    # O registro do evento é desfeito junto: a retentativa não será ignorada
    assert await db("SELECT count(*) FROM processed_events") == 0
    assert await db("SELECT status FROM orders WHERE id = :i", i=order_id) == "pending"


async def test_ack_only_happens_after_the_commit(
    client, published, session_factory, db
):
    message = await created_event(client, published)
    seen_at_ack = []

    async def ack():
        seen_at_ack.append(await db("SELECT count(*) FROM processed_events"))

    message.ack = ack
    await worker.on_message(message, session_factory)

    assert seen_at_ack == [1]  # no momento do ack, o commit já era visível


@pytest.mark.parametrize(
    "message_id, body",
    [
        (None, b'{"id": 1}'),
        ("1", b"isso nao e json"),
        ("1", b'{"sem_id": true}'),
    ],
)
async def test_invalid_message_is_rejected_without_requeue(deliver, db, message_id, body):
    message = await deliver(FakeMessage(message_id, body))

    assert message.calls == [("reject", False)]
    assert await db("SELECT count(*) FROM processed_events") == 0


async def test_only_pending_orders_become_paid(client, published, engine, deliver, db):
    message = await created_event(client, published)
    order_id = int(message.message_id)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE orders SET status = 'shipped' WHERE id = :i"), {"i": order_id})

    await deliver(message)

    # Um pedido que já avançou não volta para paid; o evento fica registrado
    assert await db("SELECT status FROM orders WHERE id = :i", i=order_id) == "shipped"
    assert await db("SELECT count(*) FROM processed_events") == 1
    assert message.calls == [("ack",)]
