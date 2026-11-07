"""pace-proxy — an out-of-process pacing proxy for OpenAI-compatible upstreams.

Why this exists
---------------
Some free tiers throttle **account-wide**, not per-model: a fixed gap between
messages for the whole account, plus a concurrency cap that includes the parent
request. Worse, they signal the throttle with a status code stock SDKs treat as
permanent — LiteRouter returns HTTP 403, which every OpenAI SDK maps to an auth
error, so retry logic never fires and the request just dies.

Clients cannot fix this from the inside:

* In-process middleware only paces the process it lives in. Cron jobs, sibling
  agents and ad-hoc scripts each get their own clock and collectively blow
  through the limit.
* Fallback chains fire ~35ms apart. Every fallback attempt lands inside the
  cooldown window and is guaranteed to be rejected.

One global clock has to sit *outside* every caller. That is this process.

Design rule
-----------
Swallow only what we can actually fix. When the ladder is spent, return a real
HTTP error so the caller keeps honest failure reporting and its own backstop.
Never fabricate a cheerful response.

Endpoints
---------
POST /v1/chat/completions   passthrough, SSE-aware
POST /v1/responses          passthrough, SSE-aware
GET  /v1/models             passthrough
GET  /healthz               local state, no upstream call

Licence: MIT
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

LOG = logging.getLogger("pace")
logging.basicConfig(
    level=os.environ.get("PACE_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)


def _env_list(name: str, default: str) -> tuple[str, ...]:
    return tuple(x.strip().lower() for x in os.environ.get(name, default).split(",") if x.strip())


UPSTREAM = os.environ.get("PACE_UPSTREAM", "https://api.openai.com/v1").rstrip("/")
API_KEY = os.environ.get("PACE_API_KEY", "") or os.environ.get("OPENAI_API_KEY", "")

# --- Pacing ---------------------------------------------------------------
# Gap between requests that share the throttled (usually free) tier. Measure
# your own provider's cliff: send N calls at each candidate gap and find the
# smallest value with zero rejections, then add margin. Guessing wastes
# throughput; guessing low wastes requests.
MIN_GAP = float(os.environ.get("PACE_FREE_GAP", "2.8"))
# Metered/premium tiers usually have no cooldown. Pacing them at the free gap
# is pure added latency for zero protection.
PAID_MIN_GAP = float(os.environ.get("PACE_PAID_GAP", "0"))
# Provider concurrency cap, minus headroom for callers you don't control.
MAX_CONCURRENT = int(os.environ.get("PACE_MAX_CONCURRENT", "4"))
# How many times to wait out a cooldown rejection before giving up on a model.
COOLDOWN_RETRIES = int(os.environ.get("PACE_COOLDOWN_RETRIES", "2"))
# Extra wait added after a cooldown rejection so we clear the window with margin.
COOLDOWN_MARGIN = float(os.environ.get("PACE_COOLDOWN_MARGIN", "1.0"))

# The ladder. First entry that answers wins. Leftmost = preferred. If the
# caller asks for a model not in the ladder, it is prepended so an explicit
# choice is still honoured.
LADDER = [m.strip() for m in os.environ.get("PACE_LADDER", "").split(",") if m.strip()]

# Substrings meaning "the account is throttled, wait and retry" rather than
# "this request is broken". Matched against the response BODY, never the status
# code, because providers overload 403/400 for throttling. Extend this for your
# provider — a throttle you fail to classify costs you a whole request.
COOLDOWN_MARKERS = _env_list(
    "PACE_COOLDOWN_MARKERS",
    "rate limit exceeded,too many concurrent requests,rate limit reached,"
    "quota exceeded,slow down",
)
# Substrings meaning this model is spent for now; move down the ladder.
EXHAUSTED_MARKERS = _env_list(
    "PACE_EXHAUSTED_MARKERS",
    "daily limit reached,no permanent credit,insufficient quota,out of credits",
)
# Substring that marks a model as metered/premium (no cooldown applies).
PAID_MARKER = os.environ.get("PACE_PAID_MARKER", ":metered")

PASS_THROUGH_HEADERS = {"content-type", "accept"}

# Some providers sit behind a WAF that allowlists browser User-Agents and
# rejects SDK defaults (Cloudflare error 1010). Override if yours needs it.
DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
FORCED_UA = os.environ.get("PACE_USER_AGENT", DEFAULT_UA)


class Pacer:
    """One global clock, with a per-tier gap.

    Throttled tiers carry an account-wide cooldown; metered tiers usually do
    not. The gap is therefore chosen per request, and the clocks are kept
    separate so a metered call is never delayed by a free call's window and
    vice versa.
    """

    def __init__(self, free_gap: float, paid_gap: float, max_concurrent: int) -> None:
        self.free_gap = free_gap
        self.paid_gap = paid_gap
        self._lock = asyncio.Lock()
        self._last_free: float = 0.0
        self._last_paid: float = 0.0
        self._sem = asyncio.Semaphore(max_concurrent)
        self.in_flight = 0
        self.total = 0
        self.paces = 0

    @staticmethod
    def is_paid(model: str) -> bool:
        return bool(PAID_MARKER) and PAID_MARKER in model

    async def acquire(self, model: str = "") -> None:
        await self._sem.acquire()
        paid = self.is_paid(model)
        gap = self.paid_gap if paid else self.free_gap
        async with self._lock:
            last = self._last_paid if paid else self._last_free
            if gap > 0:
                now = time.monotonic()
                wait = (last + gap) - now
                if wait > 0:
                    self.paces += 1
                    await asyncio.sleep(wait)
            stamp = time.monotonic()
            if paid:
                self._last_paid = stamp
            else:
                self._last_free = stamp
        self.in_flight += 1
        self.total += 1

    def release(self) -> None:
        self.in_flight = max(0, self.in_flight - 1)
        self._sem.release()

    async def penalty(self, seconds: float, model: str = "") -> None:
        """Push the relevant clock forward after an upstream cooldown rejection."""
        async with self._lock:
            gap = self.paid_gap if self.is_paid(model) else self.free_gap
            target = time.monotonic() + seconds - gap
            if self.is_paid(model):
                self._last_paid = max(self._last_paid, target)
            else:
                self._last_free = max(self._last_free, target)


PACER = Pacer(MIN_GAP, PAID_MIN_GAP, MAX_CONCURRENT)


def _classify(status: int, body: bytes) -> str:
    """Return ok | cooldown | exhausted | hard.

    Status code alone is not enough: providers reuse 403 and 400 for
    throttling, and SDKs treat those as permanent. The body says what actually
    happened.
    """
    text = body.decode("utf-8", "replace").lower()
    if 200 <= status < 300:
        return "ok"
    if any(m in text for m in COOLDOWN_MARKERS):
        return "cooldown"
    if any(m in text for m in EXHAUSTED_MARKERS):
        return "exhausted"
    return "hard"


def _ladder_for(requested: str) -> list[str]:
    """Requested model first if unknown, otherwise the ladder as configured."""
    if not requested:
        return list(LADDER)
    if requested in LADDER:
        return list(LADDER)
    return [requested, *LADDER]


def _headers(request: Request) -> dict[str, str]:
    out = {"authorization": f"Bearer {API_KEY}", "user-agent": FORCED_UA}
    for k, v in request.headers.items():
        k = k.lower()
        # user-agent is always ours: a WAF may allowlist exactly one shape.
        if k in PASS_THROUGH_HEADERS and k != "user-agent":
            out[k] = v
    return out


async def _proxy(client: httpx.AsyncClient, method: str, path: str, body: Any, headers: dict, stream: bool):
    url = f"{UPSTREAM}{path}"
    req = client.build_request(method, url, json=body, headers=headers)
    return await client.send(req, stream=stream)


async def _handle(request: Request, path: str) -> Response:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": {"message": "pace-proxy: request body was not JSON"}}, status_code=400)

    requested = str(body.get("model") or "")
    stream = bool(body.get("stream"))
    ladder = _ladder_for(requested)
    headers = _headers(request)
    headers["accept"] = "text/event-stream" if stream else "application/json"

    last_error: Optional[tuple[int, bytes]] = None

    for model in ladder:
        cooldown_attempts = 0
        while True:
            # Slot + gap are held ACROSS the upstream call and, for a stream,
            # across the whole body — released exactly once by whichever path
            # returns. Releasing in a `finally` here instead would free the slot
            # before the stream had finished, making the concurrency cap a lie.
            await PACER.acquire(model)
            released = False

            def _done() -> None:
                nonlocal released
                if not released:
                    released = True
                    PACER.release()

            try:
                resp = await _proxy(
                    client=get_client(), method="POST", path=path,
                    body={**body, "model": model}, headers=headers, stream=stream,
                )
            except Exception as exc:
                _done()
                LOG.warning("transport error on %s: %s", model, exc)
                last_error = (502, json.dumps(
                    {"error": {"message": f"pace-proxy: upstream transport error: {exc}"}}
                ).encode())
                break

            # Any non-2xx is a JSON error body, even when we asked to stream.
            if resp.status_code >= 400:
                raw = await resp.aread()
                await resp.aclose()
                _done()
                kind = _classify(resp.status_code, raw)
                last_error = (resp.status_code, raw)

                if kind == "cooldown" and cooldown_attempts < COOLDOWN_RETRIES:
                    cooldown_attempts += 1
                    await PACER.penalty(
                        (PAID_MIN_GAP if PACER.is_paid(model) else MIN_GAP) + COOLDOWN_MARGIN,
                        model,
                    )
                    LOG.info("cooldown on %s (retry %d/%d) — waiting out the window",
                             model, cooldown_attempts, COOLDOWN_RETRIES)
                    continue          # same model, after the window
                if kind == "hard":
                    LOG.warning("hard error %s on %s — surfacing without retry",
                                resp.status_code, model)
                    return Response(content=raw, status_code=resp.status_code,
                                    media_type=resp.headers.get("content-type", "application/json"))
                LOG.info("moving down ladder past %s (%s)", model, kind)
                break             # exhausted, or cooldown retries spent -> next model

            if stream:
                LOG.info("streaming %s via %s", path, model)
                return StreamingResponse(
                    _stream_body(resp, on_close=_done),
                    status_code=resp.status_code,
                    media_type=resp.headers.get("content-type", "text/event-stream"),
                )

            raw = await resp.aread()
            await resp.aclose()
            _done()
            LOG.info("serving %s via %s", path, model)
            return Response(content=raw, status_code=resp.status_code,
                            media_type=resp.headers.get("content-type", "application/json"))

    status, raw = last_error or (502, b'{"error":{"message":"pace-proxy: ladder exhausted"}}')
    LOG.warning("ladder spent for %s (requested %r) — surfacing %s upstream",
                path, requested, status)
    return Response(content=raw, status_code=status, media_type="application/json")


async def _stream_body(resp: httpx.Response, on_close):
    """Relay the upstream stream verbatim, releasing the pacing slot exactly once.

    Some transports (and any mocked one) hand back an already-buffered response,
    where ``aiter_raw()`` raises ``StreamConsumed``. Fall back to the buffered
    bytes rather than failing the request.
    """
    try:
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        except httpx.StreamConsumed:
            yield resp.content
    finally:
        try:
            await resp.aclose()
        finally:
            on_close()


CLIENT: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:
    """Shared upstream client. Tests may assign CLIENT before the app starts."""
    global CLIENT
    if CLIENT is None:
        CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(600.0, connect=20.0), follow_redirects=False
        )
    return CLIENT


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_client()
    LOG.info("pace-proxy up: upstream=%s free_gap=%.1fs paid_gap=%.1fs concurrent=%d ladder=%s",
             UPSTREAM, MIN_GAP, PAID_MIN_GAP, MAX_CONCURRENT, LADDER)
    try:
        yield
    finally:
        global CLIENT
        if CLIENT is not None:
            await CLIENT.aclose()
            CLIENT = None


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "upstream": UPSTREAM,
        "free_gap_seconds": MIN_GAP,
        "paid_gap_seconds": PAID_MIN_GAP,
        "max_concurrent": MAX_CONCURRENT,
        "in_flight": PACER.in_flight,
        "requests_served": PACER.total,
        "times_paced": PACER.paces,
        "ladder": LADDER,
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    return await _handle(request, "/chat/completions")


@app.post("/v1/responses")
async def responses(request: Request):
    return await _handle(request, "/responses")


@app.get("/v1/models")
async def models(request: Request):
    h = _headers(request)
    h["accept"] = "application/json"
    # Catalog fetch, not a chat request: no cooldown applies, only the slot.
    await PACER.acquire(f"{PAID_MARKER}:catalog")
    try:
        try:
            resp = await _proxy(client=get_client(), method="GET", path="/models", body=None,
                                headers=h, stream=False)
        except Exception as exc:
            LOG.warning("transport error on /models: %s", exc)
            return JSONResponse(
                {"error": {"message": f"pace-proxy: upstream transport error: {exc}"}},
                status_code=502)
        raw = await resp.aread()
        await resp.aclose()
        return Response(content=raw, status_code=resp.status_code,
                        media_type=resp.headers.get("content-type", "application/json"))
    finally:
        PACER.release()
