from fastapi import APIRouter, FastAPI, Depends
from pydantic import BaseModel
from database import OrderModel, SessionLocal
from sqlalchemy.ext.asyncio import AsyncSession

class Order(BaseModel):
    product: str
    value: float
    

app = FastAPI()
router = APIRouter()

async def get_session():
    async with SessionLocal() as session:
        yield session


@router.post("/orders", status_code=201)
async def create_order(order: Order, session: AsyncSession = Depends(get_session)):
    db_order = OrderModel(product=order.product, value=order.value)
    session.add(db_order)
    await session.commit()
    return {"id": db_order.id, **order.model_dump()}

app.include_router(router)
