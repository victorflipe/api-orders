"""Consumer de `order.created`: marca o pedido como pago.

Fluxo de cada mensagem (ack manual):
  1. Em UMA transação: registra o message_id em `processed_events` e muda o
     pedido de pending para paid.
  2. Só depois do commit, dá o ack.

Se o worker cair entre o commit e o ack, o RabbitMQ reentrega a mensagem; o
message_id já registrado faz a segunda entrega virar no-op (só ack). Isso dá
processamento "pelo menos uma vez" na entrega com efeito "uma vez" no banco.
"""

import asyncio
import json
import logging
import os
import signal

import aio_pika
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert

import messaging
from database import OrderModel, OrderStatus, ProcessedEvent, SessionLocal

logger = logging.getLogger("worker")

PREFETCH = 10
RETRY_DELAY = 1.0  # pausa antes de devolver à fila, para não girar em falso


class InvalidMessage(ValueError):
    """Mensagem que nunca vai dar certo, por mais que seja reentregue."""


def parse(message) -> tuple[str, int]:
    if not message.message_id:
        raise InvalidMessage("mensagem sem message_id")
    try:
        order_id = int(json.loads(message.body)["id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise InvalidMessage(f"payload inválido: {exc!r}") from exc
    return message.message_id, order_id


async def mark_order_paid(session, order_id: int) -> bool:
    """pending -> paid. Só atualiza pedidos ainda pendentes."""
    result = await session.execute(
        update(OrderModel)
        .where(OrderModel.id == order_id, OrderModel.status == OrderStatus.PENDING)
        .values(status=OrderStatus.PAID)
    )
    return result.rowcount == 1


PAID, SKIPPED, DUPLICATE = "paid", "skipped", "duplicate"


async def handle_event(session_factory, message_id: str, order_id: int) -> str:
    """Aplica o evento numa única transação e diz o que aconteceu:
    PAID (pedido atualizado), SKIPPED (pedido inexistente ou já adiante) ou
    DUPLICATE (mensagem já processada antes)."""
    
    async with session_factory() as session, session.begin():
        # ON CONFLICT DO NOTHING: duas entregas simultâneas da mesma mensagem
        # esperam uma pela outra na chave primária, e só uma insere.
        inserted = await session.scalar(
            pg_insert(ProcessedEvent)
            .values(message_id=message_id)
            .on_conflict_do_nothing()
            .returning(ProcessedEvent.message_id)
        )
        
        if inserted is None:
            return DUPLICATE
        
        paid = await mark_order_paid(session, order_id)
        return PAID if paid else SKIPPED  # o commit acontece ao sair do `begin()`


async def on_message(message, session_factory=SessionLocal) -> None:
    
    try:
        message_id, order_id = parse(message)
        
    except InvalidMessage:
        logger.exception("Mensagem descartada")
        await message.reject(requeue=False)  # reentregar não adianta
        return

    try:
        outcome = await handle_event(session_factory, message_id, order_id)
        
    except Exception:
        logger.exception("Falha ao processar %s; devolvendo à fila", message_id)
        await asyncio.sleep(RETRY_DELAY)
        await message.nack(requeue=True)  # nada foi gravado: a transação deu rollback
        return

    # O ack vem DEPOIS do commit
    await message.ack()
    
    if outcome == PAID:
        logger.info("Pedido %s marcado como pago (evento %s)", order_id, message_id)
    
    elif outcome == SKIPPED:
        logger.warning(
            "Pedido %s não existe ou não estava pendente; evento %s registrado sem alterar nada",
            order_id, message_id,
        )
        
    else:
        logger.info("Evento %s repetido; ignorado", message_id)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    stop = asyncio.Event()
    
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    
    except NotImplementedError:  # Windows
        pass

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH)
        _, queue = await messaging.declare_topology(channel)
        await queue.consume(on_message, no_ack=False)  # ack manual
        logger.info("Consumindo %s", messaging.QUEUE_NAME)
        await stop.wait()
        logger.info("Encerrando")


if __name__ == "__main__":
    asyncio.run(main())
