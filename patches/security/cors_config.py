"""cors_config.py — Configuration CORS securisee pour FastAPI.

Fonctionnalites :
- Origines autorisees : localhost uniquement par defaut
- Configurable via .env (CORS_ALLOWED_ORIGINS)
- Headers et methodes restreints au strict necessaire
- Credentials controles
- Aucune origine wildcard (*) sauf en mode dev explicite

Integration : voir SECURITY_INSTALL.md
"""
from __future__ import annotations

import logging
import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Origines par defaut (localhost uniquement)
# ---------------------------------------------------------------------------

_DEFAULT_ORIGINS: List[str] = [
    "http://localhost:8000",
    "http://localhost:8002",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8002",
    # Electron charge via file:// ou http://localhost — les deux sont couverts
    "http://localhost",
    "http://127.0.0.1",
]

# Headers HTTP autorises (minimum necessaire pour l'API + auth)
_ALLOWED_HEADERS: List[str] = [
    "Accept",
    "Accept-Language",
    "Content-Type",
    "X-API-Key",
    "Authorization",
    "Cache-Control",
    "X-Requested-With",
]

# Methodes HTTP autorisees
_ALLOWED_METHODS: List[str] = [
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "OPTIONS",
]


def _parse_origins() -> List[str]:
    """Lit les origines autorisees depuis .env (CORS_ALLOWED_ORIGINS).

    Format attendu dans .env ::

        CORS_ALLOWED_ORIGINS=http://localhost:8000,http://localhost:3000

    Si la variable est absente, on utilise _DEFAULT_ORIGINS.
    Si elle vaut *, on refuse et on emet un avertissement (dangereux).
    """
    raw = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()

    if not raw:
        log.info("[CORS] CORS_ALLOWED_ORIGINS non defini, utilisation des origines localhost par defaut.")
        return list(_DEFAULT_ORIGINS)

    if raw == "*":
        log.error(
            "[CORS] CORS_ALLOWED_ORIGINS=* est INTERDIT en production. "
            "Utilisation des origines localhost par defaut."
        )
        return list(_DEFAULT_ORIGINS)

    origins = [o.strip().rstrip("/") for o in raw.split(",") if o.strip()]

    # Validation basique : rejeter les origines non-http et les wildcards
    safe_origins: List[str] = []
    for origin in origins:
        if origin.startswith(("http://", "https://")):
            safe_origins.append(origin)
        else:
            log.warning("[CORS] Origine ignoree (schema invalide) : %s", origin)

    if not safe_origins:
        log.warning("[CORS] Aucune origine valide trouvee, repli sur localhost.")
        return list(_DEFAULT_ORIGINS)

    log.info("[CORS] Origines autorisees : %s", safe_origins)
    return safe_origins


def get_cors_config() -> dict:
    """Retourne la configuration CORS sous forme de dictionnaire.

    Utile pour inspecter la config sans modifier l'app.
    """
    return {
        "allow_origins": _parse_origins(),
        "allow_credentials": True,
        "allow_methods": _ALLOWED_METHODS,
        "allow_headers": _ALLOWED_HEADERS,
        # max_age : cache preflight 10 minutes (reduction des OPTIONS requests)
        "max_age": 600,
    }


def add_cors_middleware(app: FastAPI) -> None:
    """Ajoute le middleware CORS securise a l'application FastAPI.

    Usage dans orchestrator.py ::

        from patches.security.cors_config import add_cors_middleware
        add_cors_middleware(app)

    IMPORTANT : ajouter apres add_auth_middleware (les middlewares s'appliquent
    dans l'ordre inverse d'ajout dans Starlette).
    """
    config = get_cors_config()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config["allow_origins"],
        allow_credentials=config["allow_credentials"],
        allow_methods=config["allow_methods"],
        allow_headers=config["allow_headers"],
        max_age=config["max_age"],
    )
    log.info(
        "[CORS] Middleware actif. Origines: %s, Credentials: %s",
        config["allow_origins"],
        config["allow_credentials"],
    )
