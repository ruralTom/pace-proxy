#!/usr/bin/env python3
"""Find your provider's minimum viable gap between requests.

Every throttled tier has a cliff: below some interval, requests are rejected;
above it, everything passes. Guessing low burns requests, guessing high burns
throughput. This measures it.

It talks DIRECTLY to the upstream (not through the proxy), sends N calls at each
candidate gap, and reports the smallest gap that drew zero rejections.

    export PACE_UPSTREAM=https://api.example.com/v1
    export PACE_API_KEY=...
    python3 scripts/probe_gap.py --model model-a:free

Then set PACE_FREE_GAP to the recommended value it prints.

Costs one request per probe. With the defaults that's 5 gaps x 20 calls = 100
requests, so point it at a model you don't mind spending.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def one_call(base: str, key: str, model: str, timeout: float) -> tuple[int, str]:
    body = json.dumps({
        "model": model,
        "max_output_tokens": 8,
        "input": "say ok",
        "stream": False,
    }).encode()
    req = urllib.request.Request(
        f"{base.rstrip('/')}/responses",
        data=body,
        headers={"content-type": "application/json",
                 "authorization": f"Bearer {key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
            return r.status, ""
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:160]
    except Exception as exc:
        return 0, str(exc)[:160]


def probe(base: str, key: str, model: str, gap: float, calls: int, timeout: float):
    ok = rejected = 0
    sample = ""
    # Settle before the first call. Without this the batch's opening request
    # inherits the PREVIOUS batch's interval and gets rejected for a gap it
    # never had — which reads as "this gap fails" when it doesn't.
    time.sleep(gap)
    for i in range(calls):
        if i:
            time.sleep(gap)
        status, err = one_call(base, key, model, timeout)
        if 200 <= status < 300:
            ok += 1
        else:
            rejected += 1
            if not sample:
                sample = f"HTTP {status}: {err.strip()[:70]}"
    return ok, rejected, sample


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True, help="model id to probe")
    ap.add_argument("--gaps", default="1.0,1.5,2.0,2.5,3.0",
                    help="comma-separated candidate gaps in seconds")
    ap.add_argument("--calls", type=int, default=20, help="requests per gap (default 20)")
    ap.add_argument("--timeout", type=float, default=180.0, help="per-request timeout")
    args = ap.parse_args()

    base = os.environ.get("PACE_UPSTREAM") or os.environ.get("OPENAI_BASE_URL", "")
    key = os.environ.get("PACE_API_KEY") or os.environ.get("OPENAI_API_KEY", "")
    if not base or not key:
        print("error: set PACE_UPSTREAM and PACE_API_KEY (or OPENAI_BASE_URL/"
              "OPENAI_API_KEY)", file=sys.stderr)
        return 2

    gaps = [float(g) for g in args.gaps.split(",") if g.strip()]
    print(f"upstream  {base}")
    print(f"model     {args.model}")
    print(f"plan      {len(gaps)} gaps x {args.calls} calls = {len(gaps) * args.calls} requests\n")
    print(f"{'gap':>6}  {'ok':>8}  {'rejected':>8}  verdict")
    print(f"{'-'*6}  {'-'*8}  {'-'*8}  {'-'*38}")

    clean: list[float] = []
    for gap in gaps:
        ok, rejected, sample = probe(base, key, args.model, gap, args.calls, args.timeout)
        verdict = "CLEAN" if rejected == 0 else (sample or f"{rejected} rejected")
        print(f"{gap:6.1f}  {ok:3d}/{args.calls:<4d}  {rejected:8d}  {verdict}")
        if rejected == 0:
            clean.append(gap)

    print()
    if not clean:
        print("No clean gap found. Either the tier is already exhausted, or the cliff is")
        print("above your highest candidate — try --gaps with larger values.")
        return 1

    best = min(clean)
    print(f"Smallest clean gap: {best:.1f}s")
    print(f"Recommended PACE_FREE_GAP={best + 0.3:.1f}  (cliff + margin)")
    print("\nRe-run once to confirm — a single clean pass can be luck.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
