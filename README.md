# pace-proxy

A small, single-file proxy that paces requests to an [OI]-compatible API from
**outside** your process, so account-wide throttles stop killing your agent.

```
your client ──▶ pace-proxy :8899 ──▶ upstream provider
                 one clock
                 one ladder
                 one concurrency cap
```

~400 lines. FastAPI + httpx. No API key needed to test it.

## The problem

Some providers — free tiers especially — throttle **account-wide** and signal it
in a way your SDK misreads. Three failure modes follow, all invisible from
inside the client:

**1. In-process pacing can't work.** Middleware inside one app only paces that
app. Cron jobs, sibling agents, a `curl` in another terminal — each keeps its
own clock, so together they blow through a shared limit. The clock has to live
outside every caller.

**2. Throttles arrive disguised as permanent errors.** Some providers return
HTTP **403** with `"rate limit exceeded"` in the body. Every [OI]-compatible SDK
maps 403 to "auth failed, don't retry" — so the request dies instead of waiting
five seconds and succeeding. The status code lies; the body tells the truth.

**3. Fallback chains fire in milliseconds.** A naive ladder tries model A, then
B, then C within ~35 ms. Every attempt lands inside the same account-wide
cooldown and is rejected. Unpaced, a fallback chain makes things worse.

## What it does

- **One global clock**, with a separate gap per tier — throttled models get the
  cooldown, metered models get zero added latency.
- **Concurrency cap**, held for the whole response including streamed bodies.
- **Ladder fallback** — an exhausted model moves you to the next one instead of
  failing the request.
- **Body-based classification** into four outcomes:

  | Outcome | Meaning | Action |
  |---|---|---|
  | `ok` | 2xx | serve it |
  | `cooldown` | throttled, temporary | wait out the window, retry same model |
  | `exhausted` | this model is spent | move down the ladder |
  | `hard` | genuinely broken (auth, bad request) | surface immediately, no retry |

  The `hard` case matters as much as the others: retrying a real auth error just
  burns your ladder and turns a clear failure into a confusing one.

- **Honest failure.** When every model is spent it returns the real upstream
  error. It never fabricates a cheerful empty response.

## Quickstart

```bash
git clone https://github.com/ruralTom/pace-proxy
cd pace-proxy
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt

cp .env.example .env      # edit: PACE_UPSTREAM, PACE_API_KEY, PACE_LADDER
set -a; . ./.env; set +a
./.venv/bin/python -m uvicorn pace_proxy:app --host 127.0.0.1 --port 8899
```

Point your client at it:

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8899/v1
export OPENAI_API_KEY=anything        # the proxy holds the real key
```

Check it's alive:

```bash
curl -s localhost:8899/healthz | python3 -m json.tool
```

```json
{
  "ok": true,
  "upstream": "https://api.example.com/v1",
  "free_gap_seconds": 2.8,
  "paid_gap_seconds": 0.0,
  "max_concurrent": 4,
  "in_flight": 0,
  "requests_served": 0,
  "times_paced": 0,
  "ladder": ["model-a:free", "model-b:free"]
}
```

## Test it yourself, with no API key

This is the part that matters if you're deciding whether to trust code from a
stranger. Everything below runs **offline**, against a fake upstream that
throttles exactly the way a real free tier does — account-wide gap, hard
concurrency cap, throttles returned as 403.

```bash
./.venv/bin/pip install -r requirements-dev.txt
./.venv/bin/python -m pytest tests/ -q        # 19 unit + behavioural tests
./.venv/bin/python tests/offline_check.py     # end-to-end, 9 checks
```

`offline_check.py` starts the fake provider on a random port, launches the proxy
against it, and asserts each promise:

```
1. Sequential calls with no human-sized gap
  PASS  three back-to-back calls all succeed  — got [200, 200, 200]
2. Concurrency cap honoured upstream
  PASS  all six concurrent calls succeed
  PASS  upstream never saw more than its cap  — peak 1
3. Exhausted model falls down the ladder
  PASS  served by the backup model  — HTTP 200
  PASS  ladder was walked in order  — tried ['primary:free', 'backup:free']
4. A spent ladder returns a real error, not a fake success
  PASS  hard auth error surfaced immediately  — HTTP 401
5. Streaming holds its concurrency slot to the end
  PASS  stream completed  — HTTP 200
  PASS  stream body arrived intact  — 117 bytes
6. Health endpoint reports the pacing config
  PASS  healthz exposes the ladder and gaps

9/9 checks passed
```

In that run the upstream rejected **zero** of 23 requests. That's the whole
claim: the proxy stayed inside the window instead of finding out the hard way.

## The bug worth knowing about

If you build something like this, you will write this first:

```python
await pacer.acquire()
try:
    resp = await upstream(...)
    return StreamingResponse(relay(resp))
finally:
    pacer.release()          # ← releases before the stream finishes
```

The `finally` fires when the *handler* returns — which for a stream is before a
single byte of the body has been delivered. The concurrency cap becomes
decorative: you think you allow four in-flight requests and you actually allow
however many the client opens. Release the slot when the body is done, on every
path, exactly once.

`tests/test_behaviour.py::test_stream_holds_its_slot_until_the_body_is_done`
is the regression test. It fails against the naive version.

## Measure your own provider's cliff

Don't guess the gap. Every provider has a minimum viable interval; guessing low
burns requests, guessing high burns throughput.

```bash
export PACE_UPSTREAM=https://api.example.com/v1
export PACE_API_KEY=...
python3 scripts/probe_gap.py --model model-a:free
```

Output against a test server with a known 0.5 s cliff:

```
upstream  http://127.0.0.1:58995/v1
model     test:free
plan      3 gaps x 6 calls = 18 requests

   gap        ok  rejected  verdict
------  --------  --------  --------------------------------------
   0.2    1/6            5  HTTP 403: {"error": {"message": "Rate limit exceeded"}}
   0.6    6/6            0  CLEAN
   1.0    6/6            0  CLEAN

Smallest clean gap: 0.6s
Recommended PACE_FREE_GAP=0.9  (cliff + margin)
```

Set `PACE_FREE_GAP` to the recommended value. The shipped default is `2.8`,
which is what the free tier this was built against needed — yours will differ,
which is exactly why the script exists.

## Configuration

Everything is an environment variable; `.env.example` is annotated.

| Variable | Default | What it does |
|---|---|---|
| `PACE_UPSTREAM` | `https://api.openai.com/v1` | the real API base URL |
| `PACE_API_KEY` | `$OPENAI_API_KEY` | credential sent upstream |
| `PACE_FREE_GAP` | `2.8` | seconds between throttled-tier requests |
| `PACE_PAID_GAP` | `0` | seconds between metered requests |
| `PACE_MAX_CONCURRENT` | `4` | provider cap, minus headroom |
| `PACE_LADDER` | *(empty)* | comma-separated fallback order |
| `PACE_COOLDOWN_RETRIES` | `2` | waits per model before moving on |
| `PACE_COOLDOWN_MARGIN` | `1.0` | extra wait after a rejection |
| `PACE_COOLDOWN_MARKERS` | *(see example)* | body substrings meaning "wait" |
| `PACE_EXHAUSTED_MARKERS` | *(see example)* | body substrings meaning "spent" |
| `PACE_PAID_MARKER` | `:metered` | substring marking a metered model |
| `PACE_USER_AGENT` | browser UA | some WAFs reject SDK default UAs |

If your provider's throttle wording isn't in the default markers, **add it** — an
unclassified throttle costs you a whole request.

## Run it as a service

`deploy/pace-proxy.service` is a systemd *user* unit — no root, survives logout
with `loginctl enable-linger`.

```bash
cp deploy/pace-proxy.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now pace-proxy
journalctl --user -u pace-proxy -f
```

The unit reads `.env` from the install directory and binds to loopback only.

## Design rule

**Swallow only what you can actually fix.**

When the ladder is spent this returns a real HTTP error, so the caller keeps
honest failure reporting and its own backstop. A proxy that fabricates success
is worse than no proxy, because it turns a visible rate limit into a silent
wrong answer.

## Scope

A small tool with a narrow job. It is not a router, not a cache, and not a
security boundary — it binds to loopback and holds a key, so don't expose it.
For token accounting, per-tenant keys and spend budgets, use
[LiteLLM](https://github.com/BerriAI/litellm); it's excellent and far more
complete.

MIT licensed.
