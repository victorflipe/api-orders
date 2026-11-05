import logging
from contextlib import asynccontextmanager

import messaging
from fastapi import APIRouter, FastAPI, Depends, Header, Response
from pydantic import BaseModel, ConfigDict, Field
from database import OrderModel, SessionLocal, OrderStatus
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)

class Order(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    product: str = Field(min_length=2, max_length=255)
    value: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    status: OrderStatus = OrderStatus.PENDING


class OrderOut(Order):
    model_config = ConfigDict(from_attributes=True)

    id: int
    product: str
    created_at: datetime


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
    order: Order,
    response: Response,
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=255),
    session: AsyncSession = Depends(get_session),
):
    async def find_by_key():
        return await session.scalar(
            select(OrderModel).where(OrderModel.idempotency_key == idempotency_key)
        )

    # Repetição: devolve o pedido original em vez de criar outro
    if idempotency_key:
        existing = await find_by_key()
        if existing:
            response.status_code = 200
            return existing

    db_order = OrderModel(
        product=order.product,
        value=order.value,
        status=order.status,
        idempotency_key=idempotency_key,
    )
    session.add(db_order)
    try:
        await session.commit()
    except IntegrityError:
        # Requisições simultâneas com a mesma chave: a UNIQUE do banco decide
        await session.rollback()
        if not idempotency_key:
            raise
        existing = await find_by_key()
        if not existing:
            raise
        response.status_code = 200
        return existing
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
