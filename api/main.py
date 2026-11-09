import hashlib
import json
import logging
from contextlib import asynccontextmanager

import messaging
from fastapi import APIRouter, FastAPI, Depends, Header, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field
from database import OrderModel, SessionLocal, OrderStatus
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)

class OrderBase(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    product: str = Field(min_length=2, max_length=255)
    value: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class OrderCreate(OrderBase):
    """Entrada: o cliente só informa product e value. O status é do sistema."""

    # Campos desconhecidos (ex.: "status") viram 422 em vez de serem ignorados
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")


class OrderOut(OrderBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: OrderStatus
    created_at: datetime


def request_hash(order: OrderCreate) -> str:
    """Hash estável do corpo: 10 e 10.00 geram o mesmo valor."""
    canonical = json.dumps(
        {"product": order.product, "value": f"{order.value:.2f}"}, sort_keys=True
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await messaging.connect()
    except Exception:
        logger.warning("RabbitMQ indisponível na inicialização; tentará reconectar ao publicar", exc_info=True)
    yield
    await messaging.close()


app = FastAPI(lifespan=lifespan)
router = APIRouter()

async def get_session():
    async with SessionLocal() as session:
        yield session


@router.post("/orders", status_code=201, response_model=OrderOut)
async def create_order(
    order: OrderCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=255),
    session: AsyncSession = Depends(get_session),
):
    body_hash = request_hash(order)

    async def find_by_key():
        return await session.scalar(
            select(OrderModel).where(OrderModel.idempotency_key == idempotency_key)
        )

    def replay(existing: OrderModel) -> OrderModel:
        # Mesma chave com outro corpo: erro, como o Stripe. Sem hash guardado
        # (pedido anterior ao recurso), não há como comparar e devolvemos o original.
        if existing.request_hash is not None and existing.request_hash != body_hash:
            raise HTTPException(
                status_code=422,
                detail="Idempotency-Key já foi usada com um corpo de requisição diferente",
            )
        response.status_code = 200
        return existing

    # Repetição: devolve o pedido original em vez de criar outro
    if idempotency_key:
        existing = await find_by_key()
        if existing:
            return replay(existing)

    db_order = OrderModel(
        product=order.product,
        value=order.value,
        idempotency_key=idempotency_key,
        request_hash=body_hash if idempotency_key else None,
    )
    session.add(db_order)
    try:
        await session.commit()
    except IntegrityError:
        # Requisições simultâneas com a mesma chave: a UNIQUE do banco decide
        await session.rollback()
        logger.info("Corrida de idempotência: UNIQUE rejeitou a chave %r", idempotency_key)
        if not idempotency_key:
            raise
        existing = await find_by_key()
        if not existing:
            raise
        return replay(existing)
    await session.refresh(db_order)  # carrega created_at, gerado pelo banco
    # Só no caminho de criação: repetições (200) não republicam o evento
    await messaging.publish_order_created(
        OrderOut.model_validate(db_order).model_dump(mode="json")
    )
    return db_order


@router.get("/orders", status_code=200, response_model=list[OrderOut])
async def all_orders(session: AsyncSession = Depends(get_session)):
    result = await session.scalars(select(OrderModel).order_by(OrderModel.id))
    return result.all()

app.include_router(router)
