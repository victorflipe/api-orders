import json
import logging
import os
from pathlib import Path

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from dotenv import load_dotenv

# Local: lê ../.env. No container vale a variável do compose.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "orders"
ORDER_CREATED_KEY = "order.created"

_connection: aio_pika.abc.AbstractRobustConnection | None = None
_exchange: aio_pika.abc.AbstractExchange | None = None


async def connect() -> None:
    global _connection, _exchange
    _connection = await aio_pika.connect_robust(
        os.environ["RABBITMQ_URL"], timeout=5
    )
    channel = await _connection.channel()
    _exchange = await channel.declare_exchange(
        EXCHANGE_NAME, ExchangeType.TOPIC, durable=True
    )


async def close() -> None:
    global _connection, _exchange
    if _connection is not None:
        await _connection.close()
    _connection = None
    _exchange = None


async def publish_order_created(payload: dict) -> None:
    """Publica order.created. Nunca propaga erro: o pedido já foi gravado."""
    try:
        if _exchange is None:
            await connect()
        message = Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(payload["id"]),
        )
        await _exchange.publish(message, routing_key=ORDER_CREATED_KEY, timeout=5)
    except Exception:
        logger.exception("Falha ao publicar %s (pedido %s)", ORDER_CREATED_KEY, payload.get("id"))
