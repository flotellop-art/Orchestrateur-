"""app_settings.py — Reglages persistants saisis dans l'application.

Table cle/valeur dans apps.db. Premier usage : le jeton GitHub, saisi dans
l'UI (page /control, onglet Settings) au lieu du .env.

Priorite de resolution du jeton : valeur enregistree dans l'app, sinon la
variable d'environnement GITHUB_TOKEN (.env reste un plan B).

Securite : le jeton n'est JAMAIS renvoye par l'API (seulement un apercu
masque type "ghp_...abcd"). Les endpoints /api/settings/* sont proteges par
le middleware d'auth quand API_SECRET_KEY est active.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "apps.db"


# ── Stockage cle/valeur ───────────────────────────────────────────────────────
async def init_settings_db() -> None:
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        await db.commit()


async def get_setting(key: str) -> Optional[str]:
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


async def set_setting(key: str, value: Optional[str]) -> None:
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        if value is None:
            await db.execute("DELETE FROM settings WHERE key=?", (key,))
        else:
            await db.execute(
                "INSERT INTO settings (key, value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()


# ── Jeton GitHub ──────────────────────────────────────────────────────────────
async def get_github_token() -> str:
    """Jeton GitHub effectif : celui saisi dans l'app, sinon GITHUB_TOKEN (.env)."""
    try:
        tok = (await get_setting("github_token") or "").strip()
    except Exception as e:
        log.warning("[settings] lecture du jeton KO (%s) -> repli .env", str(e)[:120])
        tok = ""
    return tok or os.getenv("GITHUB_TOKEN", "").strip()


def _mask(tok: str) -> str:
    """Apercu non exploitable du jeton (debut + fin)."""
    return (tok[:4] + "…" + tok[-4:]) if len(tok) >= 12 else "***"


# ── API HTTP ──────────────────────────────────────────────────────────────────
router = APIRouter()


class TokenIn(BaseModel):
    token: str


@router.get("/api/settings/github-token")
async def http_token_status():
    """Statut du jeton (jamais le jeton lui-meme) : defini ? d'ou ? apercu masque."""
    stored = (await get_setting("github_token") or "").strip()
    env = os.getenv("GITHUB_TOKEN", "").strip()
    tok = stored or env
    return {
        "set": bool(tok),
        "source": "app" if stored else ("env" if env else None),
        "hint": _mask(tok) if tok else None,
    }


@router.post("/api/settings/github-token")
async def http_save_token(body: TokenIn):
    tok = (body.token or "").strip()
    if len(tok) < 8 or any(c.isspace() for c in tok):
        raise HTTPException(400, "Jeton invalide (trop court ou contient des espaces).")
    await set_setting("github_token", tok)
    log.info("[settings] jeton GitHub enregistre (%s).", _mask(tok))
    return {"saved": True, "hint": _mask(tok)}


@router.delete("/api/settings/github-token")
async def http_delete_token():
    await set_setting("github_token", None)
    log.info("[settings] jeton GitHub efface.")
    return {"deleted": True}
