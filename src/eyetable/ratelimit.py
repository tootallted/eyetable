"""Small in-process rate limiting for the public API."""

import ipaddress
import threading
import time
from collections import defaultdict, deque

from starlette.requests import Request


def client_ip(request: Request) -> str:
    """The caller's address.

    Behind a Cloudflare tunnel every request arrives from cloudflared on this
    machine, so the real client is in CF-Connecting-IP. That header is only
    trusted when the direct peer is loopback; anyone else could forge it.
    """
    peer = request.client.host if request.client else ""
    try:
        loopback = ipaddress.ip_address(peer).is_loopback
    except ValueError:
        loopback = peer in ("localhost", "testclient")
    forwarded = request.headers.get("cf-connecting-ip", "").strip()
    return forwarded if loopback and forwarded else peer


class SlidingWindow:
    """At most `limit` events per `window` seconds for each key."""

    def __init__(self, limit: int, window: float, clock=time.monotonic):
        self.limit = limit
        self.window = window
        self._clock = clock
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def _trim(self, key: str, now: float) -> deque[float]:
        events = self._events[key]
        while events and events[0] <= now - self.window:
            events.popleft()
        return events

    def blocked(self, key: str) -> float:
        """Seconds until `key` may act again (0 when it may act now)."""
        with self._lock:
            now = self._clock()
            events = self._trim(key, now)
            if len(events) < self.limit:
                return 0.0
            return max(events[0] + self.window - now, 0.0)

    def hit(self, key: str) -> float:
        """Record an event if allowed; return seconds to wait if not (0 = recorded)."""
        with self._lock:
            now = self._clock()
            events = self._trim(key, now)
            if len(events) >= self.limit:
                return max(events[0] + self.window - now, 0.0)
            events.append(now)
            if len(self._events) > 10_000:  # forget idle callers so memory stays bounded
                for k in [k for k, v in self._events.items() if not v]:
                    del self._events[k]
            return 0.0
