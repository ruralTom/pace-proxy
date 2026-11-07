"""Behavioural tests against a mock upstream. No network, no API key.

Each test replaces pace_proxy.PACER with a fresh instance so pacing state never
leaks between tests.
"""

import asyncio
import json
import time

import httpx
import pytest

import pace_proxy


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def fresh_pacer(monkeypatch):
    pacer = pace_proxy.Pacer(pace_proxy.MIN_GAP, pace_proxy.PAID_MIN_GAP, pace_proxy.MAX_CONCURRENT)
    monkeypatch.setattr(pace_proxy, "PACER", pacer)
    return pacer


@pytest.fixture
def upstream(monkeypatch):
    """Install a mock transport and record every upstream call."""

    class Recorder:
        def __init__(self):
            self.calls = []

        def install(self, handler):
            def wrapped(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content) if request.content else {}
                self.calls.append(body.get("model", "<none>"))
                return handler(request, body)

            client = httpx.AsyncClient(transport=httpx.MockTransport(wrapped))
            monkeypatch.setattr(pace_proxy, "CLIENT", client)
            return client

    return Recorder()


def client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=pace_proxy.app),
                             base_url="http://shim")


def ok_body(model="primary:free"):
    return {"id": "x", "model": model, "choices": [{"message": {"content": "ok"}}]}


def err(msg, code=403):
    return httpx.Response(code, json={"error": {"message": msg}})


@pytest.mark.anyio
async def test_gap_is_enforced_between_requests(fresh_pacer, upstream):
    upstream.install(lambda req, body: httpx.Response(200, json=ok_body()))

    async with client() as c:
        t0 = time.monotonic()
        for _ in range(3):
            r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})
            assert r.status_code == 200
        elapsed = time.monotonic() - t0

    # Three calls at 50ms apart cannot finish faster than two gaps.
    assert elapsed >= 2 * pace_proxy.MIN_GAP * 0.9, f"no pacing happened: {elapsed:.3f}s"
    assert fresh_pacer.paces >= 2


@pytest.mark.anyio
async def test_paid_model_skips_the_cooldown(fresh_pacer, upstream):
    upstream.install(lambda req, body: httpx.Response(200, json=ok_body()))

    async with client() as c:
        t0 = time.monotonic()
        for _ in range(3):
            await c.post("/v1/responses", json={"model": "primary:metered", "input": "hi"})
        elapsed = time.monotonic() - t0

    # Paid gap is 0, so this must not be throttled to the free cadence.
    assert elapsed < 2 * pace_proxy.MIN_GAP, f"paid tier was paced: {elapsed:.3f}s"


@pytest.mark.anyio
async def test_cooldown_is_retried_not_surfaced(fresh_pacer, upstream):
    state = {"n": 0}

    def handler(req, body):
        state["n"] += 1
        if state["n"] == 1:
            return err("rate limit exceeded, slow down")
        return httpx.Response(200, json=ok_body())

    upstream.install(handler)

    async with client() as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 200, "a throttled request should be retried, not failed"
    assert len(upstream.calls) == 2


@pytest.mark.anyio
async def test_exhausted_model_falls_down_the_ladder(fresh_pacer, upstream):
    def handler(req, body):
        if body.get("model") == "primary:free":
            return err("daily limit reached", 429)
        return httpx.Response(200, json=ok_body("backup:free"))

    upstream.install(handler)

    async with client() as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 200
    assert upstream.calls == ["primary:free", "backup:free"]


@pytest.mark.anyio
async def test_spent_ladder_returns_a_real_error(fresh_pacer, upstream):
    """The design rule: never fabricate a cheerful response when out of options."""
    upstream.install(lambda req, body: err("daily limit reached", 429))

    async with client() as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 429
    assert "daily limit" in r.text
    # It must not pretend to have answered.
    assert "choices" not in r.text


@pytest.mark.anyio
async def test_hard_error_is_not_retried(fresh_pacer, upstream):
    upstream.install(lambda req, body: err("invalid api key", 401))

    async with client() as c:
        r = await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    assert r.status_code == 401
    assert len(upstream.calls) == 1, "a genuine auth error must not burn the ladder"


@pytest.mark.anyio
async def test_stream_holds_its_slot_until_the_body_is_done(fresh_pacer):
    """The bug this project exists to not have.

    Releasing the semaphore in a `finally` around the upstream call frees the
    slot before the streamed body is delivered, so the concurrency cap becomes
    decorative. Asserted against `_stream_body` directly: httpx's ASGI transport
    buffers the whole response, so a through-the-app test cannot observe this.
    """
    await fresh_pacer.acquire("primary:free")
    assert fresh_pacer.in_flight == 1

    released = {"n": 0}

    def on_close():
        released["n"] += 1
        fresh_pacer.release()

    class FakeStream:
        closed = False

        async def aiter_raw(self):
            for chunk in (b'data: {"delta":"hel"}\n\n', b'data: {"delta":"lo"}\n\n', b"data: [DONE]\n\n"):
                # Slot is still held while the body is flowing.
                assert fresh_pacer.in_flight == 1, "slot released mid-stream"
                yield chunk

        async def aclose(self):
            self.closed = True

    resp = FakeStream()
    seen = [chunk async for chunk in pace_proxy._stream_body(resp, on_close)]

    assert b"[DONE]" in b"".join(seen)
    assert resp.closed is True
    assert released["n"] == 1, "release must fire exactly once"
    assert fresh_pacer.in_flight == 0


@pytest.mark.anyio
async def test_stream_releases_even_if_the_upstream_stream_dies(fresh_pacer):
    """A broken upstream stream must not leak the slot forever."""
    await fresh_pacer.acquire("primary:free")
    released = {"n": 0}

    def on_close():
        released["n"] += 1
        fresh_pacer.release()

    class ExplodingStream:
        async def aiter_raw(self):
            yield b"data: partial\n\n"
            raise httpx.ReadError("upstream died")

        async def aclose(self):
            pass

    with pytest.raises(httpx.ReadError):
        async for _ in pace_proxy._stream_body(ExplodingStream(), on_close):
            pass

    assert released["n"] == 1
    assert fresh_pacer.in_flight == 0, "slot leaked after a transport error"


@pytest.mark.anyio
async def test_buffered_response_falls_back_to_content(fresh_pacer):
    """A transport that preloads the body raises StreamConsumed; don't fail the request."""
    await fresh_pacer.acquire("primary:free")
    released = {"n": 0}

    class BufferedStream:
        content = b"data: [DONE]\n\n"

        async def aiter_raw(self):
            raise httpx.StreamConsumed()
            yield  # pragma: no cover

        async def aclose(self):
            pass

    def on_close():
        released["n"] += 1
        fresh_pacer.release()

    seen = [c async for c in pace_proxy._stream_body(BufferedStream(), on_close)]
    assert b"".join(seen) == b"data: [DONE]\n\n"
    assert released["n"] == 1
    assert fresh_pacer.in_flight == 0


@pytest.mark.anyio
async def test_concurrency_cap_is_respected(fresh_pacer, upstream):
    peak = {"n": 0}

    def handler(req, body):
        peak["n"] = max(peak["n"], pace_proxy.PACER.in_flight)
        return httpx.Response(200, json=ok_body())

    upstream.install(handler)

    async def one(c):
        return await c.post("/v1/responses", json={"model": "primary:free", "input": "hi"})

    async with client() as c:
        await asyncio.gather(*[one(c) for _ in range(6)])

    assert peak["n"] <= pace_proxy.MAX_CONCURRENT, f"cap breached: {peak['n']}"
    assert pace_proxy.PACER.in_flight == 0


@pytest.mark.anyio
async def test_healthz_reports_state_without_calling_upstream(fresh_pacer, upstream):
    upstream.install(lambda req, body: httpx.Response(200, json=ok_body()))

    async with client() as c:
        r = await c.get("/healthz")

    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["ladder"] == ["primary:free", "backup:free"]
    assert upstream.calls == [], "health check must not touch the upstream"
