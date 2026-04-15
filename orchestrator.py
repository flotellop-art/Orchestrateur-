"""Orchestrateur — backend FastAPI.

Phase 1 integration : synchronisation de clé API depuis Appfacade / Arty.

Priorité de résolution de la clé Anthropic :
  1. runtime_api_key  (injectée via POST /api/set-key)
  2. ANTHROPIC_API_KEY (variable d'environnement)
  3. HTTP 503          (aucune clé configurée)
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="Orchestrateur", version="1.0.0")

# ---------------------------------------------------------------------------
# CORS — autorise l'app Appfacade (dev + prod) et le domaine Arty
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://tryarty.com",
        "https://appfacade.pages.dev",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Clé API Anthropic injectée à chaud (Phase 1)
# ---------------------------------------------------------------------------
runtime_api_key: str | None = None


def get_anthropic_api_key() -> str:
    """Retourne la clé Anthropic active.

    Priorité : runtime_api_key > ANTHROPIC_API_KEY > 503.
    """
    if runtime_api_key:
        return runtime_api_key
    env_key = os.environ.get("ANTHROPIC_API_KEY")
    if env_key:
        return env_key
    raise HTTPException(status_code=503, detail="Aucune clé API configurée")


# ---------------------------------------------------------------------------
# Modèles Pydantic
# ---------------------------------------------------------------------------
class SetKeyRequest(BaseModel):
    api_key: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Endpoints d'intégration Phase 1
# ---------------------------------------------------------------------------
@app.get("/api/stats")
async def get_stats() -> dict[str, object]:
    """Heartbeat utilisé par Appfacade pour détecter l'Orchestrateur."""
    return {
        "status": "ok",
        "service": "orchestrateur",
        "has_key": runtime_api_key is not None
        or os.environ.get("ANTHROPIC_API_KEY") is not None,
    }


@app.post("/api/set-key")
async def set_api_key(payload: SetKeyRequest) -> dict[str, str]:
    """Injecte la clé Anthropic en mémoire (non persistée).

    Validation stricte : doit commencer par ``sk-ant-``.
    Ne journalise jamais la valeur.
    """
    global runtime_api_key

    key = payload.api_key.strip()
    if not key.startswith("sk-ant-"):
        raise HTTPException(status_code=400, detail="Format de clé invalide")

    runtime_api_key = key
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
