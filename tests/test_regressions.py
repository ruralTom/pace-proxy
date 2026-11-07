"""Regression tests for the three failure modes found on 2026-11-06.

Each of these shipped as a real bug:

1. The pacer stamped its clock when a request STARTED, so a nominal gap
   collapsed to near-zero idle once real latency was in play.
2. An hourly quota rejection was classified as a 5-second cooldown (its body
   contains the words "rate limit"), so it was retried and walked down the
   ladder, spending the budget it had just exhausted.
3. Throttles were passed through as HTTP 403, which every SDK reads as "bad
   API key" — telling the user to re-authenticate a working credential.
"""

import asyncio
import time

import httpx
import pytest

import pace_proxy


@pytest.fixture
def fresh_pacer(monkeypatch):
    pacer = pace_proxy.Pacer(pace_proxy.MIN_GAP, pace_proxy.PAID_MIN_GAP,
                             pace_proxy.MAX_CONCURRENT)
    monkeypatch.setattr(pace_proxy, "PACER", pacer)
    return pacer


# --- 1. Pacing is FINISH-to-START -----------------------------------------

@pytest.mark.anyio
async def test_gap_is_measured_from_response_completion(fresh_pacer, monkeypatch):
    """A gap of N seconds must be N seconds of IDLE, not N seconds minus latency.

    With start-to-start stamping, a slow upstream eats the whole gap: a 2.8s
    setting against 2.5s latency leaves 0.3s of real idle. This test simulates
    latency and asserts the idle is still the configured gap.
    """
    monkeypatch.setattr(pace_proxy, "MIN_GAP", 0.5)
    pacer = pace_proxy.Pacer(0.5, 0, 2)
    monkeypatch.setattr(pace_proxy, "PACER", pacer)

    await pacer.acquire("x:free")
    t_start = time.monotonic()
    await asyncio.sleep(0.3)          # pretend the upstream took 0.3s
    pacer.release("x:free")
    t_end = time.monotonic()

    await pacer.acquire("x:free")
    idle = time.monotonic() - t_end

    assert (t_end - t_start) > 0.25, "the simulated call should have taken time"
    assert 0.45 <= idle <= 0.75, f"idle was {idle:.2f}s, expected ~0.50s"


@pytest.mark.anyio
async def test_paced_tier_clock_starts_at_completion(fresh_pacer):
    """The free-tier clock must record completion time, not start time."""
    await fresh_pacer.acquire("free-model:free")
    before = fresh_pacer._last_free
    await asyncio.sleep(0.05)
    fresh_pacer.release("free-model:free")
    assert fresh_pacer._last_free > before, "release must stamp the free clock"


@pytest.mark.anyio
async def test_paced_tier_serializes_concurrent_callers(fresh_pacer, monkeypatch):
    """A gap tier cannot run two requests at once without collapsing idle.

    Two callers admitted simultaneously must NOT both be in flight; the second
    waits for the first to release plus the gap.
    """
    monkeypatch.setattr(pace_proxy, "MIN_GAP", 0.1)
    pacer = pace_proxy.Pacer(0.1, 0, 4)
    monkeypatch.setattr(pace_proxy, "PACER", pacer)

    order = []

    async def worker(n):
        await pacer.acquire("x:free")
        order.append(("start", n))
        await asyncio.sleep(0.05)
        pacer.release("x:free")
        order.append(("end", n))

    await asyncio.gather(worker(1), worker(2))

    # Strictly interleaved: worker 2 cannot start until worker 1 has ended.
    assert order == [("start", 1), ("end", 1), ("start", 2), ("end", 2)] or \
           order == [("start", 2), ("end", 2), ("start", 1), ("end", 1)]


@pytest.mark.anyio
async def test_paid_tier_allows_concurrency(fresh_pacer, monkeypatch):
    """Metered models have no idle window, so they keep real concurrency."""
    pacer = pace_proxy.Pacer(1.0, 0, 4)
    monkeypatch.setattr(pace_proxy, "PACER", pacer)

    peak = 0

    async def worker():
        nonlocal peak
        await pacer.acquire("x:metered")
        peak = max(peak, pacer.in_flight)
        await asyncio.sleep(0.05)
        pacer.release("x:metered")

    await asyncio.gather(*[worker() for _ in range(4)])
    assert peak > 1, "metered tier should permit concurrent requests"


# --- 2. Budget vs cooldown classification ----------------------------------

def test_hourly_cap_is_budget_not_cooldown():
    """The hourly body CONTAINS 'rate limit' — it must still classify as budget."""
    body = (b'{"error":{"message":"[LiteRouter] Hourly rate limit exceeded for '
            b'free models. Please try again in a few minutes."}}')
    assert pace_proxy._classify(403, body) == "budget"


def test_five_second_window_is_cooldown():
    body = (b'{"error":{"message":"[LiteRouter] Rate limit exceeded for your tier '
            b'(5 seconds between messages)."}}')
    assert pace_proxy._classify(403, body) == "cooldown"


def test_daily_is_exhausted_and_walks_the_ladder():
    body = b'{"error":{"message":"Daily limit reached and no permanent credit available."}}'
    assert pace_proxy._classify(403, body) == "exhausted"


def test_bad_key_stays_hard():
    assert pace_proxy._classify(403, b'{"error":{"message":"invalid api key"}}') == "hard"


# --- 3. Budget is surfaced as 429 with no ladder walk ----------------------

@pytest.fixture
def upstream_budget(monkeypatch):
    """Every upstream call returns the hourly-cap body."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(403, json={"error": {"message":
            "[LiteRouter] Hourly rate limit exceeded for free models."}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(pace_proxy, "CLIENT", client)
    return calls


@pytest.mark.anyio
async def test_budget_emits_429_and_makes_exactly_one_call(fresh_pacer, upstream_budget):
    """One upstream call, 429 out, retry-after set — no retry, no ladder walk."""
    transport = httpx.ASGITransport(app=pace_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://shim") as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 429, "a throttle must not surface as 403 (reads as auth)"
    assert r.headers.get("retry-after") == "300"
    assert len(upstream_budget) == 1, (
        f"expected 1 upstream call, made {len(upstream_budget)} — "
        "the ladder walked a budget error"
    )


@pytest.fixture
def upstream_cooldown(monkeypatch):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(403, json={"error": {"message":
            "Rate limit exceeded for your tier (5 seconds between messages)."}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(pace_proxy, "CLIENT", client)
    return calls


@pytest.mark.anyio
async def test_cooldown_emits_429_not_403(fresh_pacer, upstream_cooldown):
    transport = httpx.ASGITransport(app=pace_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://shim") as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 429
    assert r.headers.get("retry-after") == "10"


@pytest.mark.anyio
async def test_cooldown_does_not_walk_the_ladder(fresh_pacer, upstream_cooldown):
    """An account-wide window makes every ladder entry fail identically."""
    transport = httpx.ASGITransport(app=pace_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://shim") as c:
        await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    # 1 initial + COOLDOWN_RETRIES retries (default 1), and then STOP.
    assert len(upstream_cooldown) == 1 + pace_proxy.COOLDOWN_RETRIES
    assert len(upstream_cooldown) < len(pace_proxy.LADDER) * 3, (
        "the full ladder was walked on an account-wide throttle"
    )
