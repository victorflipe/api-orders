import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Numeric
from decimal import Decimal
from datetime import datetime
import enum
from sqlalchemy import Enum, String, func

# Local: lê ../.env. No container o arquivo não existe e vale a variável do compose.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# a URL do compose é postgresql://..., o asyncpg precisa de postgresql+asyncpg://
DATABASE_URL = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    PAID = "paid"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"

class OrderModel(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    product: Mapped[str]
    value: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    status: Mapped[OrderStatus] = mapped_column(
        Enum(
            OrderStatus,
            name="order_status",
            values_callable=lambda e: [m.value for m in e],
        ),
        default=OrderStatus.PENDING,
        server_default=OrderStatus.PENDING.value,
    )
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    # NULL é permitido várias vezes sob UNIQUE no Postgres (pedidos sem chave)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    # SHA-256 do corpo normalizado; detecta reuso da chave com outro corpo
    request_hash: Mapped[str | None] = mapped_column(String(64))
