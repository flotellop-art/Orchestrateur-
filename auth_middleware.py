"""auth_middleware.py — Protection par cle API, DESACTIVEE par defaut.

Principe : tant que `API_SECRET_KEY` est vide/absente dans l'environnement, le
middleware n'est meme pas monte — l'application se comporte exactement comme
avant (aucun risque de blocage). Des qu'une cle est definie, l'API exige cette
cle (sinon 401/403), avec un rate-limit par IP.

Active uniquement si `API_SECRET_KEY` est non vide :
- Header `X-API-Key: <cle>` OU `Authorization: Bearer <cle>`
- Query param `?api_key=<cle>` (indispensable pour EventSource/SSE qui ne sait
  pas envoyer de header personnalise)
- Pages d'UI et fichiers statiques restent publics (l'auth porte sur l'API ;
  les pages portent la cle via leurs requetes fetch/SSE)
- Rate-limit en memoire par IP (defaut 120 req / 60 s, configurable)

Integration (orchestrator.py) ::

    from auth_middleware import add_auth_middleware
    add_auth_middleware(app)   # no-op si API_SECRET_KEY est vide

Pour activer : definir API_SECRET_KEY dans .env et coller la MEME cle dans la
page de reglages de control.html (champ « Cle API »).
"""
from __future__ import annotations

import hmac
import logging
import os
import time
from collections import defaultdict
from typing import Callable, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

log = logging.getLogger(__name__)


def get_api_secret() -> str:
    """Cle API courante (chaine vide = protection desactivee)."""
    return os.getenv("API_SECRET_KEY", "").strip()


def is_auth_enabled() -> bool:
    return bool(get_api_secret())


# Pages et ressources toujours accessibles sans cle : ce sont des coquilles
# statiques ; leurs appels /api/... portent la cle (header ou ?api_key=).
PUBLIC_PATHS: set[str] = {
    "/", "/control", "/workspace", "/sw.js",
    "/favicon.ico", "/manifest.webmanifest", "/static/manifest.webmanifest",
    "/health", "/api/health",
}
PUBLIC_PREFIXES: tuple[str, ...] = ("/static/",)

RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "120"))
RATE_LIMIT_WINDOW: int = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
_rate_counters: dict[str, list[float]] = defaultdict(list)


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _rate_ok(ip: str) -> bool:
    now = time.monotonic()
    window_start = now - RATE_LIMIT_WINDOW
    _rate_counters[ip] = [t for t in _rate_counters[ip] if t > window_start]
    if len(_rate_counters[ip]) >= RATE_LIMIT_REQUESTS:
        return False
    _rate_counters[ip].append(now)
    return True


def _extract_token(request: Request) -> Optional[str]:
    api_key = request.headers.get("X-API-Key", "").strip()
    if api_key:
        return api_key
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    # Indispensable pour EventSource (SSE) : impossible d'y mettre un header.
    qp = request.query_params.get("api_key", "").strip()
    if qp:
        return qp
    return None


def _is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(p) for p in PUBLIC_PREFIXES)


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, api_secret: str) -> None:
        super().__init__(app)
        self._api_secret = api_secret

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path
        if _is_public(path):
            return await call_next(request)

        ip = _client_ip(request)
        if not _rate_ok(ip):
            log.warning("[AUTH] rate-limit IP=%s path=%s", ip, path)
            return JSONResponse(status_code=429, content={"detail": "Too Many Requests"},
                                headers={"Retry-After": str(RATE_LIMIT_WINDOW)})

        token = _extract_token(request)
        if token is None:
            return JSONResponse(status_code=401,
                                content={"detail": "Cle API requise (X-API-Key, Bearer ou ?api_key=)."})
        if not hmac.compare_digest(token, self._api_secret):
            log.warning("[AUTH] cle invalide IP=%s path=%s", ip, path)
            return JSONResponse(status_code=403, content={"detail": "Cle API invalide."})
        return await call_next(request)


def add_auth_middleware(app: FastAPI) -> bool:
    """Monte la protection SI une cle est definie. Retourne True si active.

    No-op (rien monte, aucun risque) quand API_SECRET_KEY est vide.
    """
    secret = get_api_secret()
    if not secret:
        log.info("[AUTH] protection API desactivee (API_SECRET_KEY vide).")
        return False
    app.add_middleware(APIKeyAuthMiddleware, api_secret=secret)
    log.info("[AUTH] protection API ACTIVE. Rate-limit %d req/%ds.",
             RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
    return True
