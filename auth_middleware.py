"""HTTP security boundary for the orchestrator.

Local mode stays zero-configuration, but it is deliberately limited to direct
loopback requests.  A reverse proxy, tunnel or remote peer must use an API key.
The Host header is always checked to prevent DNS-rebinding attacks.

Authentication accepts only headers:

* ``X-API-Key: <key>``
* ``Authorization: Bearer <key>``

Secrets in query parameters are rejected by omission because URLs are commonly
recorded by browsers, proxies and access logs.
"""
from __future__ import annotations

import hmac
import logging
import os
import time
from collections import OrderedDict, deque
from typing import Callable, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from security_policy import (
    is_allowed_host,
    is_unproxied_local_request,
    parse_allowed_hosts,
)


log = logging.getLogger(__name__)


def get_api_secret() -> str:
    """Current API key (empty means local-only mode)."""
    return os.getenv("API_SECRET_KEY", "").strip()


def is_auth_enabled() -> bool:
    return bool(get_api_secret())


def _positive_env_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        log.warning("[AUTH] %s invalide ; valeur par defaut %d.", name, default)
        return default
    return value if value > 0 else default


# The UI shell remains readable. Sensitive operations all live outside these
# paths and are protected by local-only mode or the configured API key.
PUBLIC_PATHS: set[str] = {
    "/", "/control", "/workspace", "/sw.js",
    "/favicon.ico", "/manifest.webmanifest", "/static/manifest.webmanifest",
    "/health", "/api/health",
}
PUBLIC_PREFIXES: tuple[str, ...] = ("/static/",)

RATE_LIMIT_REQUESTS = _positive_env_int("RATE_LIMIT_REQUESTS", 120)
RATE_LIMIT_WINDOW = _positive_env_int("RATE_LIMIT_WINDOW", 60)
RATE_LIMIT_MAX_CLIENTS = _positive_env_int("RATE_LIMIT_MAX_CLIENTS", 4096)
MIN_API_SECRET_LENGTH = 24

# LRU of fixed-size, expiring deques. The peer IP comes from the socket, never
# from a caller-controlled X-Forwarded-For header.
_rate_counters: OrderedDict[str, deque[float]] = OrderedDict()


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _rate_ok(ip: str, now: float | None = None) -> bool:
    current = time.monotonic() if now is None else now
    window_start = current - RATE_LIMIT_WINDOW
    bucket = _rate_counters.get(ip)
    if bucket is None:
        if len(_rate_counters) >= RATE_LIMIT_MAX_CLIENTS:
            _rate_counters.popitem(last=False)
        bucket = deque()
        _rate_counters[ip] = bucket
    else:
        _rate_counters.move_to_end(ip)

    while bucket and bucket[0] <= window_start:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_REQUESTS:
        return False
    bucket.append(current)
    return True


def _extract_token(request: Request) -> Optional[str]:
    api_key = request.headers.get("X-API-Key", "").strip()
    if api_key:
        return api_key
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def _is_public(path: str) -> bool:
    return path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES)


def _json_error(status: int, detail: str, **headers: str) -> JSONResponse:
    response_headers = {"Cache-Control": "no-store", **headers}
    return JSONResponse(status_code=status, content={"detail": detail}, headers=response_headers)


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, api_secret: str, allowed_hosts: tuple[str, ...]) -> None:
        super().__init__(app)
        self._api_secret = api_secret
        self._allowed_hosts = allowed_hosts

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        host = request.headers.get("host")
        if not is_allowed_host(host, self._allowed_hosts):
            log.warning("[AUTH] Host refuse=%r peer=%s", host, _client_ip(request))
            return _json_error(400, "Hote HTTP non autorise.")

        path = request.url.path
        if _is_public(path):
            return await call_next(request)

        peer_ip = _client_ip(request)
        if not self._api_secret:
            if is_unproxied_local_request(peer_ip, request.headers):
                return await call_next(request)
            log.warning("[AUTH] acces distant refuse sans cle peer=%s path=%s", peer_ip, path)
            return _json_error(
                403,
                "Acces distant refuse : configurez API_SECRET_KEY.",
            )

        if not _rate_ok(peer_ip):
            log.warning("[AUTH] rate-limit peer=%s path=%s", peer_ip, path)
            return _json_error(
                429,
                "Too Many Requests",
                **{"Retry-After": str(RATE_LIMIT_WINDOW)},
            )

        token = _extract_token(request)
        if token is None:
            return _json_error(401, "Cle API requise (X-API-Key ou Bearer).")
        if not hmac.compare_digest(token, self._api_secret):
            log.warning("[AUTH] cle invalide peer=%s path=%s", peer_ip, path)
            return _json_error(403, "Cle API invalide.")
        return await call_next(request)


def add_auth_middleware(app: FastAPI) -> bool:
    """Always mount the security boundary; return whether key auth is active."""
    secret = get_api_secret()
    if secret and len(secret) < MIN_API_SECRET_LENGTH:
        raise RuntimeError(
            "API_SECRET_KEY est trop courte (minimum {} caracteres).".format(
                MIN_API_SECRET_LENGTH
            )
        )
    allowed_hosts = parse_allowed_hosts(os.getenv("ORCHESTRATOR_ALLOWED_HOSTS"))
    app.add_middleware(
        APIKeyAuthMiddleware,
        api_secret=secret,
        allowed_hosts=allowed_hosts,
    )
    if secret:
        log.info(
            "[AUTH] protection API active. Rate-limit %d req/%ds, hotes=%s.",
            RATE_LIMIT_REQUESTS,
            RATE_LIMIT_WINDOW,
            ",".join(allowed_hosts),
        )
    else:
        log.warning(
            "[AUTH] API_SECRET_KEY vide : API limitee aux acces loopback directs, hotes=%s.",
            ",".join(allowed_hosts),
        )
    return bool(secret)
