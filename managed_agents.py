"""managed_agents.py — Pont entre l'orchestrateur et les Agents gérés Anthropic.

Permet au chef de déléguer une sous-tâche à un agent hébergé chez Anthropic
(console.anthropic.com → "Agents gérés") : on crée une session sur l'agent,
on lui envoie l'instruction, on streame ses réponses dans le chat de la tâche,
et on remonte le texte final au chef.

L'environnement (le « container » côté Anthropic) est cherché ou créé une fois
au premier usage et mis en cache (`get_or_create_environment`).

Docs : https://platform.claude.com/docs/en/managed-agents
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Awaitable, Callable, Optional

from anthropic import AsyncAnthropic
from fastapi import APIRouter, HTTPException

log = logging.getLogger(__name__)

DEFAULT_ENV_NAME = os.getenv("ANTHROPIC_DEFAULT_ENV_NAME", "default-orchestrateur")
DEFAULT_ENV_ID = (os.getenv("ANTHROPIC_DEFAULT_ENV_ID") or "").strip() or None
SESSION_TIMEOUT_S = float(os.getenv("MANAGED_AGENT_TIMEOUT", "300"))

_client: Optional[AsyncAnthropic] = None
_env_id_cache: Optional[str] = None
_env_lock = asyncio.Lock()


def _get_client() -> AsyncAnthropic:
    global _client
    if _client is None:
        key = os.getenv("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY manquant (.env).")
        _client = AsyncAnthropic(api_key=key)
    return _client


async def list_agents() -> list[dict]:
    """Liste les agents gérés du compte : [{id, name, model, status}, ...]."""
    client = _get_client()
    out: list[dict] = []
    async for ag in client.beta.agents.list():
        out.append({
            "id": getattr(ag, "id", ""),
            "name": getattr(ag, "name", "") or "",
            "model": getattr(ag, "model", "") or "",
            "status": getattr(ag, "status", "") or "",
        })
    return out


async def get_or_create_environment(name: str = DEFAULT_ENV_NAME) -> str:
    """Renvoie l'ID d'un environnement utilisable.

    Priorité : ANTHROPIC_DEFAULT_ENV_ID si défini → environnement existant nommé `name`
    → premier environnement trouvé → en crée un nouveau s'il n'y en a aucun.
    Le résultat est mis en cache pour éviter de relister à chaque appel.
    """
    global _env_id_cache
    if DEFAULT_ENV_ID:
        _env_id_cache = DEFAULT_ENV_ID
        return _env_id_cache
    if _env_id_cache:
        return _env_id_cache
    async with _env_lock:
        if _env_id_cache:
            return _env_id_cache
        client = _get_client()
        matching: Optional[str] = None
        any_env: Optional[str] = None
        async for e in client.beta.environments.list():
            eid = getattr(e, "id", "")
            ename = getattr(e, "name", "") or ""
            if not eid:
                continue
            if any_env is None:
                any_env = eid
            if ename == name:
                matching = eid
                break
        if matching:
            _env_id_cache = matching
        elif any_env:
            _env_id_cache = any_env
        else:
            env = await client.beta.environments.create(name=name)
            _env_id_cache = getattr(env, "id", None)
        if not _env_id_cache:
            raise RuntimeError("Impossible d'obtenir un environnement Anthropic.")
        log.info("[managed-agents] environnement utilise : %s", _env_id_cache)
        return _env_id_cache


def _extract_text(content_blocks) -> str:
    """Concat les textes d'une liste de blocs `agent.message`."""
    if not content_blocks:
        return ""
    out = []
    for b in content_blocks:
        if getattr(b, "type", "") == "text":
            out.append(getattr(b, "text", "") or "")
    return "".join(out)


async def run_session(
    agent_id: str,
    instruction: str,
    on_text: Optional[Callable[[str], Awaitable[None]]] = None,
    on_thinking: Optional[Callable[[str], Awaitable[None]]] = None,
    timeout_s: float = SESSION_TIMEOUT_S,
) -> dict:
    """Démarre une session sur `agent_id`, envoie `instruction`, streame jusqu'à idle.

    Retourne `{"text", "stop_reason", "session_id", "status"}`.
    Appelle `on_text(chunk)` pour chaque message texte de l'agent (utile pour
    pousser dans le flux SSE de la tâche en cours).
    """
    client = _get_client()
    env_id = await get_or_create_environment()
    session = await client.beta.sessions.create(agent=agent_id, environment_id=env_id)
    session_id = session.id
    parts: list[str] = []
    stop_reason: Optional[str] = None
    status = "running"

    async def _consume():
        nonlocal stop_reason, status
        stream = await client.beta.sessions.events.stream(session_id)
        try:
            await client.beta.sessions.events.send(
                session_id,
                events=[{"type": "user.message",
                         "content": [{"type": "text", "text": instruction}]}],
            )
            async for event in stream:
                etype = getattr(event, "type", "")
                if etype == "agent.message":
                    text = _extract_text(getattr(event, "content", None))
                    if text:
                        parts.append(text)
                        if on_text:
                            try:
                                await on_text(text)
                            except Exception as e:
                                log.warning("[managed-agents] on_text erreur: %s", e)
                elif etype == "agent.thinking" and on_thinking:
                    txt = getattr(event, "text", "") or _extract_text(
                        getattr(event, "content", None))
                    if txt:
                        try:
                            await on_thinking(txt)
                        except Exception:
                            pass
                elif etype == "session.status_idle":
                    stop_reason = getattr(event, "stop_reason", None) or "end_turn"
                    status = "idle"
                    break
                elif etype == "session.status_terminated":
                    status = "terminated"
                    break
        finally:
            close = getattr(stream, "close", None) or getattr(stream, "aclose", None)
            if close:
                try:
                    res = close()
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass

    try:
        await asyncio.wait_for(_consume(), timeout=timeout_s)
    except asyncio.TimeoutError:
        status = "timeout"
        log.warning("[managed-agents] timeout (%ss) sur session %s", timeout_s, session_id)

    return {
        "text": "\n".join(parts).strip(),
        "stop_reason": stop_reason,
        "session_id": session_id,
        "status": status,
    }


# ── Router FastAPI : endpoints publics pour l'UI ─────────────────────────────
router = APIRouter()


@router.get("/api/managed-agents")
async def http_list_agents():
    """Liste les agents gérés du compte (pour l'UI et le chef)."""
    try:
        return {"agents": await list_agents()}
    except Exception as e:
        raise HTTPException(500, "Impossible de lister les agents Anthropic : " + str(e)[:200])


@router.get("/api/managed-agents/environment")
async def http_get_environment():
    """Retourne (ou crée) l'environnement par défaut utilisé pour les sessions."""
    try:
        eid = await get_or_create_environment()
        return {"environment_id": eid, "name": DEFAULT_ENV_NAME}
    except Exception as e:
        raise HTTPException(500, "Environnement Anthropic indisponible : " + str(e)[:200])
