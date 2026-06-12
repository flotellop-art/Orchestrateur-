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
from pathlib import Path
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
        model = getattr(ag, "model", "") or ""
        if model and not isinstance(model, str):
            # `model` peut etre un objet {id, speed} : on normalise en chaine.
            model = getattr(model, "id", None) or str(model)
        out.append({
            "id": getattr(ag, "id", ""),
            "name": getattr(ag, "name", "") or "",
            "model": model,
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


async def _download_outputs(client: AsyncAnthropic, session_id: str, dest: Path) -> list[str]:
    """Telecharge les fichiers ecrits par l'agent dans /mnt/session/outputs/.

    Renvoie des chemins relatifs au dossier parent de `dest` (ex: "ab12cd34/rapport.md").
    L'indexation cote Anthropic prend ~1-3 s apres la fin -> petites retentatives.
    Best-effort : toute erreur est loggee, jamais propagee.
    """
    data = []
    for _ in range(3):
        try:
            page = await client.beta.files.list(
                scope_id=session_id, betas=["managed-agents-2026-04-01"])
        except TypeError:
            # SDK trop ancien : ne connait pas scope_id -> pas de recuperation possible.
            log.info("[managed-agents] SDK sans scope_id : sorties de session non recuperees.")
            return []
        data = list(getattr(page, "data", []) or [])
        if data:
            break
        await asyncio.sleep(1.5)
    if not data:
        return []
    dest.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for f in data:
        # basename : empeche un nom de fichier malveillant de sortir du dossier cible.
        name = os.path.basename(getattr(f, "filename", "") or "") or str(getattr(f, "id", "fichier"))
        if not name or name in (".", ".."):
            continue
        target = dest / name
        try:
            resp = await client.beta.files.download(getattr(f, "id", ""))
            res = resp.write_to_file(str(target))
            if asyncio.iscoroutine(res):
                await res
            saved.append(dest.name + "/" + name)
        except Exception as e:
            log.warning("[managed-agents] telechargement de %s KO : %s", name, str(e)[:120])
    return saved


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
    resources: Optional[list[dict]] = None,
    outputs_dir: Optional[str] = None,
    rubric: Optional[str] = None,
    max_outcome_iterations: int = 3,
) -> dict:
    """Démarre une session sur `agent_id`, envoie `instruction`, streame jusqu'à idle.

    Options :
      - `resources`  : ressources montées dans la session (ex. dépôt GitHub
        `{"type":"github_repository","url":...,"authorization_token":...}`).
      - `outputs_dir`: dossier local où récupérer les fichiers que l'agent a
        déposés dans /mnt/session/outputs/ (sous-dossier par session).
      - `rubric`     : critères de réussite vérifiables. Si fourni, la mission
        est envoyée comme OBJECTIF NOTÉ (`user.define_outcome`) : un examinateur
        indépendant note le travail et l'agent itère jusqu'à validation
        (au plus `max_outcome_iterations` tours).

    Retourne `{"text", "stop_reason", "session_id", "status", "console_url",
    "files", "outcome"}`. Appelle `on_text(chunk)` pour chaque message texte de
    l'agent. En cas de coupure du flux SSE, reconnecte et recoupe avec
    l'historique (dédoublonnage par id).
    """
    client = _get_client()
    env_id = await get_or_create_environment()
    create_kwargs: dict[str, Any] = {"agent": agent_id, "environment_id": env_id}
    if resources:
        create_kwargs["resources"] = resources
    session = await client.beta.sessions.create(**create_kwargs)
    session_id = session.id
    # Lien direct vers la session dans la Console Anthropic (observabilite).
    console_url = "https://platform.claude.com/workspaces/default/sessions/" + session_id
    log.info("[managed-agents] session %s ouverte -> %s", session_id, console_url)
    if outputs_dir:
        instruction = (instruction + "\n\nIMPORTANT : depose tes fichiers livrables dans "
                       "/mnt/session/outputs/ -- c'est de la qu'ils sont recuperes automatiquement.")
    # Evenement de demarrage : mission notee (define_outcome) OU simple message.
    # Avec un outcome, on n'envoie PAS de user.message en plus (l'agent demarre seul).
    if rubric:
        kickoff = {"type": "user.define_outcome",
                   "description": instruction,
                   "rubric": {"type": "text", "content": rubric},
                   "max_iterations": max(1, min(int(max_outcome_iterations or 3), 20))}
    else:
        kickoff = {"type": "user.message",
                   "content": [{"type": "text", "text": instruction}]}
    parts: list[str] = []
    seen_ids: set[str] = set()  # dedoublonnage des evenements (reconnexion)
    stop_reason: Optional[str] = None
    status = "running"
    outcome: dict = {}  # dernier verdict de l'examinateur (mission notee), mute en place
    sent = False

    async def _handle(event) -> bool:
        """Traite un evenement (dedoublonne par id). Renvoie True si terminal."""
        nonlocal stop_reason, status
        eid = getattr(event, "id", None)
        if eid:
            if eid in seen_ids:
                return False
            seen_ids.add(eid)
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
        elif etype == "session.error":
            err = getattr(event, "error", None)
            msg = getattr(err, "message", None) or str(err or "")[:300]
            log.warning("[managed-agents] session.error %s : %s", session_id, msg)
        elif etype == "span.outcome_evaluation_end":
            # Verdict de l'examinateur (mission notee) : satisfied / needs_revision /
            # max_iterations_reached / failed / interrupted. Mute en place (closure).
            outcome.clear()
            outcome.update({
                "result": getattr(event, "result", None),
                "explanation": (getattr(event, "explanation", "") or "")[:1000],
                "iteration": getattr(event, "iteration", None),
            })
        elif etype == "session.status_idle":
            # `stop_reason` est un objet : on lit son `.type`. Une session
            # "idle" avec stop_reason "requires_action" attend une action du
            # client (confirmation d'outil, resultat d'outil custom) -- ce
            # pont ne sait pas y repondre ; on s'arrete proprement en le
            # signalant, au lieu de rendre un resultat partiel comme final.
            sr = getattr(event, "stop_reason", None)
            sr_type = getattr(sr, "type", None) if sr is not None else None
            stop_reason = sr_type or "end_turn"
            status = "requires_action" if sr_type == "requires_action" else "idle"
            return True
        elif etype == "session.status_terminated":
            status = "terminated"
            return True
        return False

    async def _replay_history() -> bool:
        """Recoupe avec l'historique (le flux SSE n'a pas de rattrapage) ; le
        dedoublonnage par id evite de traiter deux fois. True si terminal vu."""
        page = await client.beta.sessions.events.list(session_id)
        for ev in getattr(page, "data", []) or []:
            if await _handle(ev):
                return True
        return False

    async def _consume():
        nonlocal sent
        attempts = 0
        while True:
            stream = await client.beta.sessions.events.stream(session_id)
            try:
                if not sent:
                    # Flux ouvert AVANT l'envoi : aucun evenement precoce n'est perdu.
                    await client.beta.sessions.events.send(session_id, events=[kickoff])
                    sent = True
                elif await _replay_history():
                    return
                async for event in stream:
                    if await _handle(event):
                        return
                # Flux clos sans evenement terminal -> on retente (borne ci-dessous).
                attempts += 1
                if attempts > 2:
                    raise RuntimeError("flux clos sans fin de session")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                attempts += 1
                if attempts > 2:
                    raise
                log.warning("[managed-agents] flux interrompu (%s) -> reconnexion %d/2",
                            str(e)[:120], attempts)
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
    except Exception as e:
        status = "error"
        log.warning("[managed-agents] session %s en erreur : %s", session_id, str(e)[:200])

    # Recuperer les fichiers livres AVANT d'archiver (l'archive rend la session
    # en lecture seule). Uniquement quand la session a vraiment fini son travail.
    files: list[str] = []
    if outputs_dir and status in ("idle", "terminated"):
        try:
            files = await _download_outputs(
                client, session_id, Path(outputs_dir) / session_id[-8:])
        except Exception as e:
            log.warning("[managed-agents] recuperation des sorties KO : %s", str(e)[:150])

    # Archive la session une fois terminee pour ne pas les accumuler. On NE le fait
    # PAS en cas de timeout (la session tourne peut-etre encore : on la laisse pour
    # inspection). Best-effort : une erreur d'archivage ne doit pas casser le retour.
    if status in ("idle", "terminated", "requires_action"):
        try:
            await client.beta.sessions.archive(session_id)
        except Exception as e:
            log.info("[managed-agents] archivage de %s ignore : %s", session_id, str(e)[:120])

    return {
        "text": "\n".join(parts).strip(),
        "stop_reason": stop_reason,
        "session_id": session_id,
        "status": status,
        "console_url": console_url,
        "files": files,
        "outcome": outcome or None,
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
