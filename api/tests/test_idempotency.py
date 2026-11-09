import asyncio
import logging
from collections import Counter

from sqlalchemy.ext.asyncio import AsyncSession

BODY = {"product": "camiseta", "value": "49.90"}
RACE_LOG = "Corrida de idempotência"


def hold_commits_until_all_checked(monkeypatch, parties: int) -> None:
    """Faz as `parties` requisições chegarem juntas ao commit.

    Sem isso a corrida é só provável: se a primeira requisição der commit antes
    de as outras fazerem a pré-consulta, as demais encontram a chave e nunca
    chegam ao IntegrityError (o teste passaria sem testar a UNIQUE). Com a
    barreira, todas já viram "chave inexistente" antes de qualquer insert. Se
    as requisições não rodarem de fato em paralelo (ex.: pool de 1 conexão), a
    barreira estoura por timeout e o teste falha em vez de passar em falso.
    """
    barrier = asyncio.Barrier(parties)
    original_commit = AsyncSession.commit

    async def commit(self):
        await asyncio.wait_for(barrier.wait(), timeout=5)
        return await original_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", commit)


def race_hits(caplog) -> int:
    """Quantas requisições caíram no ramo do IntegrityError."""
    return sum(RACE_LOG in message for message in caplog.messages)


def key(value: str) -> dict:
    return {"Idempotency-Key": value}


async def test_concurrent_requests_with_same_key_create_one_order(
    client, published, count_orders, caplog, monkeypatch
):
    """Cenário do README: 10 requisições simultâneas com a mesma chave
    resultam em um 201, nove 200 e uma única linha no banco."""
    hold_commits_until_all_checked(monkeypatch, parties=10)
    with caplog.at_level(logging.INFO, logger="main"):
        responses = await asyncio.gather(
            *[client.post("/orders", json=BODY, headers=key("race-1")) for _ in range(10)]
        )

    # Prova de que a corrida aconteceu: pelo menos uma requisição passou da
    # pré-consulta e foi barrada pela UNIQUE. Sem isso, o teste passaria mesmo
    # com as requisições rodando uma por vez (ex.: pool de conexões de 1).
    assert race_hits(caplog) >= 1, "nenhuma requisição chegou ao IntegrityError"

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
    client, count_orders, caplog, monkeypatch
):
    """Na corrida, quem vence cria o pedido; os de corpo diferente recebem 422."""
    hold_commits_until_all_checked(monkeypatch, parties=10)
    requests = [
        client.post(
            "/orders", json={**BODY, "value": str(10 + i % 2)}, headers=key("race-2")
        )
        for i in range(10)
    ]
    with caplog.at_level(logging.INFO, logger="main"):
        responses = await asyncio.gather(*requests)

    assert race_hits(caplog) >= 1, "nenhuma requisição chegou ao IntegrityError"

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
