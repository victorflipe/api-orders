import pytest

BODY = {"product": "camiseta", "value": "49.90"}


async def test_create_order_sets_status_pending_and_publishes_event(client, published):
    response = await client.post("/orders", json=BODY)

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "pending"
    assert data["value"] == "49.90"
    assert data["created_at"]
    assert published == [data]


async def test_client_cannot_choose_the_status(client, count_orders):
    response = await client.post("/orders", json={**BODY, "status": "paid"})

    assert response.status_code == 422
    assert await count_orders() == 0


@pytest.mark.parametrize(
    "body",
    [
        {"product": "", "value": "10"},
        {"product": "   ", "value": "10"},
        {"product": "camiseta", "value": "-5"},
        {"product": "camiseta", "value": "0"},
        {"product": "camiseta", "value": "10.999"},
        {"product": "camiseta", "value": "10000000000"},
    ],
)
async def test_invalid_body_is_rejected(client, count_orders, body):
    response = await client.post("/orders", json=body)

    assert response.status_code == 422
    assert await count_orders() == 0


async def test_list_orders(client):
    await client.post("/orders", json=BODY)
    await client.post("/orders", json={"product": "calça", "value": "120"})

    response = await client.get("/orders")

    assert response.status_code == 200
    assert [o["product"] for o in response.json()] == ["camiseta", "calça"]
