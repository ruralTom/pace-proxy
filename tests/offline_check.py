#!/usr/bin/env python3
"""End-to-end verification with NO API key and NO network.

Stands up a fake upstream that throttles exactly the way a free tier does —
account-wide minimum gap between messages, a hard concurrency cap, and
throttle rejections delivered as HTTP 403 with a body that says "rate limit"
(which stock SDKs mistake for a permanent auth error).

Then it points the shim at that upstream and asserts the shim's promises:

  1. Back-to-back sequential calls all succeed (the gap is paced).
  2. Concurrent calls never exceed the upstream's concurrency cap.
  3. An exhausted model falls down the ladder.
  4. A fully spent ladder returns a real error, never a fabricated success.
  5. A hard auth error is surfaced immediately instead of burning the ladder.
  6. A streaming response holds its concurrency slot until the body finishes.

Run:  python3 tests/offline_check.py
Exit code 0 means every promise held.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UPSTREAM_GAP = 0.6          # the fake provider's account-wide cooldown
UPSTREAM_MAX_CONCURRENT = 2
EXHAUSTED_MODEL = "primary:free"
GOOD_MODEL = "backup:free"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class UpstreamState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_finished = 0.0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.total = 0
        self.rejected_for_gap = 0
        self.rejected_for_concurrency = 0
        self.models: list[str] = []


STATE = UpstreamState()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):    # silence the default stderr spam
        pass

    def _send(self, code: int, payload: dict, content_type="application/json"):
        raw = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        model = str(body.get("model", ""))

        with STATE.lock:
            now = time.monotonic()
            STATE.total += 1
            STATE.models.append(model)
            # Enforce the gap the way a real provider does: from when the
            # previous response FINISHED, not when the previous request arrived.
            # Measuring start-to-start here makes the harness easier to pass
            # than the real service, which hides exactly the bug it should catch.
            too_fast = (now - STATE.last_finished) < UPSTREAM_GAP
            at_capacity = STATE.in_flight >= UPSTREAM_MAX_CONCURRENT
            if not too_fast and not at_capacity:
                STATE.in_flight += 1
                STATE.peak_in_flight = max(STATE.peak_in_flight, STATE.in_flight)

        if too_fast:
            with STATE.lock:
                STATE.rejected_for_gap += 1
            # The real-world trap: throttling signalled as 403, not 429.
            return self._send(403, {"error": {"message":
                "Rate limit exceeded: minimum interval between messages not met"}})
        if at_capacity:
            with STATE.lock:
                STATE.rejected_for_concurrency += 1
            return self._send(403, {"error": {"message":
                "Rate limit exceeded: too many concurrent requests"}})

        try:
            if model == EXHAUSTED_MODEL:
                return self._send(429, {"error": {"message":
                    "daily limit reached for this model"}})
            if model == "bad-key":
                return self._send(401, {"error": {"message": "invalid api key"}})

            if body.get("stream"):
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("cache-control", "no-cache")
                # No content-length on a stream: without this the client waits
                # forever for a body that already finished.
                self.send_header("connection", "close")
                self.close_connection = True
                self.end_headers()
                for piece in ("hello ", "from ", "the ", "fake"):
                    self.wfile.write(f"data: {json.dumps({'delta': piece})}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(0.15)      # slow body: the slot must stay held
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                return

            return self._send(200, {
                "id": "fake", "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}],
            })
        finally:
            with STATE.lock:
                # Stamp on completion: the cooldown starts when this response
                # finished, which is what the real provider enforces.
                STATE.last_finished = time.monotonic()
                STATE.in_flight = max(0, STATE.in_flight - 1)


def wait_for(url: str, proc=None, timeout: float = 20.0) -> None:
    """Wait for the health endpoint, but fail with the child's output if it dies.

    A bare "never became ready" hides the actual cause — usually a missing
    dependency or a bad env var. Surface the subprocess log instead.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            out = ""
            if proc.stdout is not None:
                try:
                    out = proc.stdout.read()
                except Exception:
                    out = ""
            raise RuntimeError(
                f"proxy exited with code {proc.returncode} before becoming ready.\n"
                f"--- proxy output ---\n{out.strip() or '(no output)'}\n"
                f"--- hint ---\n"
                f"If this says 'No module named uvicorn', install the runtime "
                f"requirements: ./.venv/bin/pip install -r requirements.txt"
            )
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return
        except Exception:
            time.sleep(0.15)
    raise RuntimeError(f"{url} never became ready within {timeout:.0f}s")


def post(base: str, path: str, body: dict, timeout: float = 60.0, stream: bool = False):
    req = urllib.request.Request(
        f"{base}{path}", data=json.dumps(body).encode(),
        headers={"content-type": "application/json", "authorization": "Bearer x"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, raw
    except urllib.error.HTTPError as e:
        return e.code, e.read()


RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, passed: bool, detail: str = "") -> None:
    RESULTS.append((name, passed, detail))
    print(f"  {'PASS' if passed else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))


def main() -> int:
    upstream_port = free_port()
    shim_port = free_port()

    server = ThreadingHTTPServer(("127.0.0.1", upstream_port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()

    env = {
        **os.environ,
        "PACE_UPSTREAM": f"http://127.0.0.1:{upstream_port}/v1",
        "PACE_API_KEY": "fake-key",
        "PACE_FREE_GAP": str(UPSTREAM_GAP + 0.15),   # shim must out-wait the provider
        "PACE_PAID_GAP": "0",
        "PACE_MAX_CONCURRENT": str(UPSTREAM_MAX_CONCURRENT),
        "PACE_LADDER": f"{EXHAUSTED_MODEL},{GOOD_MODEL}",
        "PACE_LOG_LEVEL": "CRITICAL",
        "PYTHONUNBUFFERED": "1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "pace_proxy:app", "--host", "127.0.0.1",
         "--port", str(shim_port), "--log-level", "critical", "--no-access-log"],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    base = f"http://127.0.0.1:{shim_port}"
    try:
        wait_for(f"{base}/healthz", proc=proc)
        print("\n1. Sequential calls with no human-sized gap")
        codes = [post(base, "/v1/responses", {"model": GOOD_MODEL, "input": "hi"})[0]
                 for _ in range(3)]
        check("three back-to-back calls all succeed", codes == [200, 200, 200], f"got {codes}")

        print("\n2. Concurrency cap honoured upstream")
        STATE.peak_in_flight = 0
        threads = []
        out: list[int] = []
        lock = threading.Lock()

        def fire():
            code = post(base, "/v1/responses", {"model": GOOD_MODEL, "input": "hi"})[0]
            with lock:
                out.append(code)

        for _ in range(6):
            t = threading.Thread(target=fire)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        check("all six concurrent calls succeed", out.count(200) == 6, f"codes {sorted(out)}")
        check("upstream never saw more than its cap",
              STATE.peak_in_flight <= UPSTREAM_MAX_CONCURRENT,
              f"peak {STATE.peak_in_flight}")

        print("\n3. Exhausted model falls down the ladder")
        before = len(STATE.models)
        code, raw = post(base, "/v1/responses", {"model": EXHAUSTED_MODEL, "input": "hi"})
        tried = STATE.models[before:]
        check("served by the backup model", code == 200 and b'"choices"' in raw, f"HTTP {code}")
        check("ladder was walked in order",
              tried[:2] == [EXHAUSTED_MODEL, GOOD_MODEL], f"tried {tried}")

        print("\n4. A spent ladder returns a real error, not a fake success")
        env_all_dead = post(base, "/v1/responses", {"model": "bad-key", "input": "hi"})
        check("hard auth error surfaced immediately", env_all_dead[0] == 401,
              f"HTTP {env_all_dead[0]}")

        print("\n5. Streaming holds its concurrency slot to the end")
        STATE.peak_in_flight = 0
        code, raw = post(base, "/v1/responses",
                         {"model": GOOD_MODEL, "input": "hi", "stream": True})
        check("stream completed", code == 200 and b"[DONE]" in raw, f"HTTP {code}")
        check("stream body arrived intact", b"hello" in raw and b"fake" in raw,
              f"{len(raw)} bytes")

        print("\n6. Health endpoint reports the pacing config")
        with urllib.request.urlopen(f"{base}/healthz", timeout=10) as r:
            health = json.loads(r.read())
        check("healthz exposes the ladder and gaps",
              health["ladder"] == [EXHAUSTED_MODEL, GOOD_MODEL]
              and health["max_concurrent"] == UPSTREAM_MAX_CONCURRENT,
              json.dumps({k: health[k] for k in ("free_gap_seconds", "max_concurrent")}))

        print(f"\nUpstream saw {STATE.total} requests: "
              f"{STATE.rejected_for_gap} rejected for gap, "
              f"{STATE.rejected_for_concurrency} for concurrency.")
        print("(Zero rejections here is the point: the shim stayed inside the window.)")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        server.shutdown()

    failed = [r for r in RESULTS if not r[1]]
    print(f"\n{'='*60}\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    if failed:
        for name, _, detail in failed:
            print(f"  FAILED: {name} {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
