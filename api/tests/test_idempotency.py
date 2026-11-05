import asyncio
from collections import Counter

BODY = {"product": "camiseta", "value": "49.90"}


def key(value: str) -> dict:
    return {"Idempotency-Key": value}


async def test_concurrent_requests_with_same_key_create_one_order(
    client, published, count_orders
):
    """Cenário do README: 10 requisições simultâneas com a mesma chave
    resultam em um 201, nove 200 e uma única linha no banco."""
    responses = await asyncio.gather(
        *[client.post("/orders", json=BODY, headers=key("race-1")) for _ in range(10)]
    )

    assert Counter(r.status_code for r in responses) == {201: 1, 200: 9}
    assert len({r.json()["id"] for r in responses}) == 1
    assert await count_orders() == 1
    # O evento só é publicado na criação, nunca nas repetições
    assert len(published) == 1


async def test_replay_returns_original_order(client, published, count_orders):
    first = await client.post("/orders", json=BODY, headers=key("k-1"))
    second = await client.post("/orders", json=BODY, headers=key("k-1"))

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json() == first.json()
    assert await count_orders() == 1
    assert len(published) == 1


async def test_equivalent_bodies_are_the_same_request(client):
    """10, 10.00 e espaços nas pontas geram o mesmo hash."""
    first = await client.post(
        "/orders", json={"product": "camiseta", "value": 10}, headers=key("k-2")
    )
    second = await client.post(
        "/orders",
        json={"product": "  camiseta  ", "value": "10.00"},
        headers=key("k-2"),
    )

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


async def test_same_key_with_different_body_is_rejected(client, count_orders):
    await client.post("/orders", json=BODY, headers=key("k-3"))

    other_value = await client.post(
        "/orders", json={**BODY, "value": "99.90"}, headers=key("k-3")
    )
    other_product = await client.post(
        "/orders", json={**BODY, "product": "calça"}, headers=key("k-3")
    )

    assert other_value.status_code == 422
    assert other_product.status_code == 422
    assert await count_orders() == 1


async def test_concurrent_requests_with_same_key_and_different_bodies(
    client, count_orders
):
    """Na corrida, quem vence cria o pedido; os de corpo diferente recebem 422."""
    requests = [
        client.post(
            "/orders", json={**BODY, "value": str(10 + i % 2)}, headers=key("race-2")
        )
        for i in range(10)
    ]
    responses = await asyncio.gather(*requests)

    statuses = Counter(r.status_code for r in responses)
    assert statuses[201] == 1
    assert set(statuses) <= {200, 201, 422}
    assert sum(statuses.values()) == 10
    assert await count_orders() == 1


async def test_requests_without_key_always_create_orders(client, count_orders):
    first = await client.post("/orders", json=BODY)
    second = await client.post("/orders", json=BODY)

    assert first.status_code == second.status_code == 201
    assert first.json()["id"] != second.json()["id"]
    assert await count_orders() == 2
