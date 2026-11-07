"""Throttle classification.

The whole point of this project: a provider that signals throttling with a
status code stock SDKs treat as permanent. Classification must read the body.
"""

import pace_proxy


def test_403_with_rate_limit_body_is_cooldown_not_auth_failure():
    # LiteRouter's actual behaviour: 403 + "rate limit exceeded". Every [OI] SDK
    # maps 403 to a permanent auth error, so this would otherwise be fatal.
    body = b'{"error":{"message":"rate limit exceeded, try again in 5s"}}'
    assert pace_proxy._classify(403, body) == "cooldown"


def test_403_with_auth_body_is_hard():
    body = b'{"error":{"message":"invalid api key"}}'
    assert pace_proxy._classify(403, body) == "hard"


def test_429_quota_is_exhausted():
    body = b'{"error":{"message":"daily limit reached for this model"}}'
    assert pace_proxy._classify(429, body) == "exhausted"


def test_2xx_is_ok_even_with_odd_body():
    assert pace_proxy._classify(200, b'{"choices":[]}') == "ok"


def test_unclassified_4xx_is_hard():
    # Unrecognised error: do not swallow it, do not retry it.
    assert pace_proxy._classify(400, b'{"error":{"message":"bad json"}}') == "hard"


def test_ladder_puts_unknown_model_first():
    assert pace_proxy._ladder_for("brand-new:free") == ["brand-new:free", "primary:free", "backup:free"]


def test_ladder_known_model_uses_configured_order():
    assert pace_proxy._ladder_for("backup:free") == ["primary:free", "backup:free"]


def test_paid_detection():
    assert pace_proxy.PACER.is_paid("x:metered:full-context") is True
    assert pace_proxy.PACER.is_paid("x:free") is False
