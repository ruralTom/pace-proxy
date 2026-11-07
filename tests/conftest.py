"""Test configuration.

Environment is set here, before any test module imports pace_proxy, because the
module reads its settings at import time. These tests never touch a real
upstream: everything goes through an httpx MockTransport.
"""

import os

os.environ["PACE_UPSTREAM"] = "https://upstream.test/v1"
os.environ["PACE_API_KEY"] = "test-key"
os.environ["PACE_FREE_GAP"] = "0.05"
os.environ["PACE_PAID_GAP"] = "0"
os.environ["PACE_MAX_CONCURRENT"] = "2"
os.environ["PACE_LADDER"] = "primary:free,backup:free"
os.environ["PACE_COOLDOWN_RETRIES"] = "1"
os.environ["PACE_COOLDOWN_MARGIN"] = "0.01"
os.environ["PACE_LOG_LEVEL"] = "CRITICAL"
