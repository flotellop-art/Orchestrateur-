"""
Espace de travail collaboratif multi-agents.

L'utilisateur fixe un objectif ; un agent "chef" autonome construit et pilote
son equipe (creation dynamique d'agents, delegation), les agents agissent
(recherche web, fichiers, commandes, tests) dans un dossier de travail dedie,
et la boucle tourne jusqu'a l'objectif ou un plafond d'iterations.

Reutilise les patterns de orchestrator.py (FastAPI, SSE, aiosqlite) et de
chat_agent.py (boucle d'agent + outils). Branche dans orchestrator.py via
include_router.
"""
import asyncio
import base64
import io
import json
import logging
import os
import re
import shlex
import shutil
import sys
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

import managed_agents  # pont vers les Agents geres Anthropic (delegation depuis le chef)

load_dotenv(Path(__file__).parent / ".env", override=False)
log = logging.getLogger(__name__)

# Configuration
DB_PATH = Path(__file__).parent / "apps.db"
STATIC = Path(__file__).parent / "static"
PROJECTS = Path(__file__).parent / "projects"
PROJECTS.mkdir(exist_ok=True)

# Modeles par defaut par provider (verifies via recherche web, mai 2026).
# Gemini 3.5 Pro n'est pas encore dispo (prevu juin 2026) -> tier Flash, stable.
# Un identifiant errone n'est pas bloquant : le provider retombe sur Claude (voir call_model).
DEFAULT_CLAUDE = "claude-opus-4-8"          # chef (planification/securite) + repli generique
DEFAULT_WORKER_CLAUDE = "claude-sonnet-4-6"  # agents/workers par defaut (moins cher, le chef peut surclasser)
DEFAULT_GEMINI = "gemini-3.5-flash"
DEFAULT_OPENAI = "gpt-5.5"
STABLE_CLAUDE = "claude-sonnet-4-6"  # repli eprouve si le modele Claude demande echoue


def _default_model(provider: str) -> str:
    # Modele par defaut des AGENTS (workers) : Claude -> Sonnet (economique). gemini/openai inchanges.
    return {"gemini": DEFAULT_GEMINI, "openai": DEFAULT_OPENAI}.get(provider, DEFAULT_WORKER_CLAUDE)


# Tarifs estimatifs ($ par million de tokens : entree, sortie) — AJUSTABLES selon ta facturation.
# Sert au suivi du cout et au plafond de budget ; un modele inconnu utilise PRICE_DEFAULT.
PRICING = {
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-4-6": (5.0, 25.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
    "gpt-5.5": (5.0, 30.0),
    "gemini-3.5-flash": (0.30, 2.50),
    "gemini-3.5-pro": (2.50, 10.0),
}
PRICE_DEFAULT = (5.0, 25.0)


def _usage_cost(usage: dict) -> float:
    pin, pout = PRICING.get(usage.get("model"), PRICE_DEFAULT)
    return (usage.get("input", 0) / 1e6) * pin + (usage.get("output", 0) / 1e6) * pout


MAX_AGENTS_CEILING = 8
MAX_ITER_CEILING = 40
WORKER_MAX_STEPS = 6
MAX_TOKENS = 16000  # sortie max par appel : assez large pour ecrire des fichiers entiers
# Mode entreprise (autopilote d'audit de depot) : plafonds etendus.
COMPANY_MAX_AGENTS = 24
COMPANY_MAX_ITER = 300
COMPANY_WORKER_STEPS = 40

COMMAND_WHITELIST = {
    "python", "python3", "pip", "pip3", "pytest",
    "ls", "cat", "echo", "pwd", "head", "tail", "wc",
    "node", "npm",
}
# Outils strictement passifs -> executables en parallele (#15).
# mcp_call exclu : certains serveurs MCP mutent l'etat (filesystem, memory...).
PARALLEL_SAFE_TOOLS = {"web_search", "read_file", "search_knowledge"}

_client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# task_id -> {"task": asyncio.Task, "pause": asyncio.Event, "step": bool, "inbox": list}
running_tasks: dict[int, dict] = {}
# task_id -> {"proc": Process, "url": str|None, "output": list}
launched_apps: dict[int, dict] = {}
_KNOWLEDGE_FTS = True  # FTS5 disponible pour la memoire long terme ?
# Cache des recherches web (requete normalisee -> resultat). Borne pour ne pas grossir sans fin.
_web_cache: dict[str, str] = {}
_WEB_CACHE_MAX = 256

NL = chr(10)


def sse(data: dict) -> str:
    return "data: " + json.dumps(data, ensure_ascii=False) + NL + NL


# ── Base de donnees ─────────────────────────────────────────────────────────
async def init_team_db():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")  # ecritures concurrentes (workers paralleles)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                objective     TEXT NOT NULL,
                folder        TEXT NOT NULL,
                status        TEXT DEFAULT 'idle',
                iteration     INTEGER DEFAULT 0,
                max_iterations INTEGER DEFAULT 15,
                max_agents    INTEGER DEFAULT 4,
                web_enabled   INTEGER DEFAULT 0,
                chef_model    TEXT DEFAULT 'claude-sonnet-4-6',
                company_mode  INTEGER DEFAULT 0,
                target_path   TEXT,
                total_cost_usd REAL DEFAULT 0,
                max_cost_usd  REAL DEFAULT 0,
                created_at    TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS task_agents (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                name       TEXT NOT NULL,
                role       TEXT DEFAULT '',
                provider   TEXT DEFAULT 'claude',
                model      TEXT DEFAULT '',
                created_by TEXT DEFAULT 'chef',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                iteration  INTEGER DEFAULT 0,
                agent      TEXT DEFAULT '',
                kind       TEXT DEFAULT 'info',
                content    TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        # Migration douce : colonnes ajoutees pour les taches existantes.
        for ddl in ("ALTER TABLE tasks ADD COLUMN image_path TEXT",
                    "ALTER TABLE tasks ADD COLUMN company_mode INTEGER DEFAULT 0",
                    "ALTER TABLE tasks ADD COLUMN target_path TEXT",
                    "ALTER TABLE tasks ADD COLUMN total_cost_usd REAL DEFAULT 0",
                    "ALTER TABLE tasks ADD COLUMN max_cost_usd REAL DEFAULT 0"):
            try:
                await db.execute(ddl)
            except Exception:
                pass
        # Prompts envoyes a l'API, pour l'inspecteur (#18).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS prompts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                iteration  INTEGER DEFAULT 0,
                agent      TEXT DEFAULT '',
                payload    TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        # Historique des builds CI locaux (#16).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS builds (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                status     TEXT,
                report     TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        # Memoire long terme partagee (inter-taches). FTS5 si dispo, sinon table simple.
        global _KNOWLEDGE_FTS
        try:
            await db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS knowledge USING fts5(content, created_at UNINDEXED)")
            _KNOWLEDGE_FTS = True
        except Exception:
            await db.execute("CREATE TABLE IF NOT EXISTS knowledge "
                             "(id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, created_at TEXT)")
            _KNOWLEDGE_FTS = False
        # Les taches en cours ne survivent pas a un redemarrage du serveur.
        await db.execute("UPDATE tasks SET status='stopped' WHERE status IN ('running','paused')")
        await db.commit()


async def db_create_task(objective, folder, max_iterations, max_agents, web_enabled, chef_model,
                         company_mode=False, target_path=None, max_cost_usd=0.0):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "INSERT INTO tasks (objective,folder,status,max_iterations,max_agents,web_enabled,chef_model,"
            "company_mode,target_path,max_cost_usd,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (objective, folder, "idle", max_iterations, max_agents,
             1 if web_enabled else 0, chef_model,
             1 if company_mode else 0, target_path, max_cost_usd, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


async def db_get_task(task_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def db_list_tasks():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


# Colonnes modifiables de `tasks` : bloque l'injection d'un nom de colonne
# arbitraire (le nom de colonne ne peut pas etre un parametre SQL lie).
# Inclut total_cost_usd/max_cost_usd (suivi du cout) pour rester compatible.
_TASKS_COLUMNS_ALLOWED = frozenset({
    "objective", "folder", "status", "iteration", "max_iterations", "max_agents",
    "web_enabled", "chef_model", "company_mode", "target_path", "image_path",
    "total_cost_usd", "max_cost_usd",
})

async def db_update_task(task_id, **kwargs):
    invalid = set(kwargs) - _TASKS_COLUMNS_ALLOWED
    if invalid:
        raise ValueError("Colonnes non autorisees pour UPDATE tasks : " + ", ".join(sorted(invalid)))
    sets = ", ".join(k + "=?" for k in kwargs)
    vals = list(kwargs.values()) + [task_id]
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("UPDATE tasks SET " + sets + " WHERE id=?", vals)
        await db.commit()


async def db_delete_task(task_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        await db.execute("DELETE FROM task_agents WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM messages WHERE task_id=?", (task_id,))
        await db.commit()


async def db_add_agent(task_id, name, role, provider, model, created_by):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "INSERT INTO task_agents (task_id,name,role,provider,model,created_by,created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (task_id, name, role, provider, model, created_by, datetime.utcnow().isoformat()),
        )
        await db.commit()


async def db_list_agents(task_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM task_agents WHERE task_id=? ORDER BY id", (task_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_get_agent(task_id, name):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM task_agents WHERE task_id=? AND name=?", (task_id, name)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def emit(task_id, iteration, agent, kind, payload: dict):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "INSERT INTO messages (task_id,iteration,agent,kind,content,created_at) VALUES (?,?,?,?,?,?)",
            (task_id, iteration, agent, kind, json.dumps(payload, ensure_ascii=False),
             datetime.utcnow().isoformat()),
        )
        await db.commit()


async def _log_prompt(task_id, iteration, agent, system, messages, image=None):
    # Logge l'historique tel qu'envoye a l'API, image comprise (format Claude Vision),
    # pour que l'inspecteur de prompt restitue fidelement ce qu'a vu le modele.
    logged = _attach_image_claude(messages, image) if image else messages
    cap = 300000 if image else 50000
    payload = json.dumps({"system": system, "messages": logged}, ensure_ascii=False, default=str)[:cap]
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "INSERT INTO prompts (task_id, iteration, agent, payload, created_at) VALUES (?,?,?,?,?)",
            (task_id, iteration, agent, payload, datetime.utcnow().isoformat()))
        await db.commit()


_SNAPSHOT_IGNORE = shutil.ignore_patterns(
    ".venv", "venv", "node_modules", "__pycache__", ".pytest_cache", ".git")


def _snapshot(task_id, folder, iteration):
    dest = PROJECTS / ".snapshots" / str(task_id) / ("iter_" + str(iteration))
    try:
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)
        shutil.copytree(folder, dest, ignore=_SNAPSHOT_IGNORE)  # exclut les dossiers lourds
    except Exception as e:
        log.warning("snapshot iter %s echec: %s", iteration, e)


async def db_messages_after(task_id, after_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM messages WHERE task_id=? AND id>? ORDER BY id", (task_id, after_id)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ── Dispatch modele (Claude + Gemini) ───────────────────────────────────────
# ── Vision : maquette -> code (#17) ──────────────────────────────────────────
_EXT_MEDIA = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
              "webp": "image/webp", "gif": "image/gif"}
_MEDIA_EXT = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp", "image/gif": "gif"}


def _parse_data_url(data_url):
    m = re.match(r"data:([^;]+);base64,(.*)", data_url or "", re.DOTALL)
    if not m:
        return None
    return {"media_type": m.group(1), "data": m.group(2)}


def _load_image(path):
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    ext = p.suffix.lower().lstrip(".")
    return {"media_type": _EXT_MEDIA.get(ext, "image/png"),
            "data": base64.b64encode(p.read_bytes()).decode("ascii")}


def _attach_image_claude(messages, image):
    if not image:
        return messages
    out = [dict(m) for m in messages]
    for i in range(len(out) - 1, -1, -1):
        if out[i]["role"] == "user" and isinstance(out[i]["content"], str):
            out[i] = {"role": "user", "content": [
                {"type": "text", "text": out[i]["content"]},
                {"type": "image", "source": {"type": "base64",
                 "media_type": image["media_type"], "data": image["data"]}},
            ]}
            break
    return out


def _attach_image_openai(messages, image):
    if not image:
        return messages
    out = [dict(m) for m in messages]
    for i in range(len(out) - 1, -1, -1):
        if out[i]["role"] == "user" and isinstance(out[i]["content"], str):
            out[i] = {"role": "user", "content": [
                {"type": "text", "text": out[i]["content"]},
                {"type": "image_url", "image_url":
                 {"url": "data:" + image["media_type"] + ";base64," + image["data"]}},
            ]}
            break
    return out


def _gemini_available() -> bool:
    return bool(os.getenv("GEMINI_API_KEY"))


async def _claude_request(model, system, messages, image=None):
    resp = await _client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        messages=_attach_image_claude(messages, image),
    )
    text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
    u = resp.usage
    usage = {"model": model,
             "input": ((getattr(u, "input_tokens", 0) or 0)
                       + (getattr(u, "cache_creation_input_tokens", 0) or 0)
                       + (getattr(u, "cache_read_input_tokens", 0) or 0)),
             "output": getattr(u, "output_tokens", 0) or 0}
    return text, usage


async def _call_claude(model, system, messages, on_notice=None, image=None):
    target = model or DEFAULT_CLAUDE
    try:
        return await _claude_request(target, system, messages, image)
    except Exception as e:
        if target == STABLE_CLAUDE:
            raise  # le modele de repli a aussi echoue : on remonte l'erreur
        log.warning("[modele] Claude %s erreur: %s -> repli %s", target, e, STABLE_CLAUDE)
        if on_notice:
            await on_notice("Erreur Claude " + target + " (" + str(e)[:100]
                            + ") -> repli sur " + STABLE_CLAUDE + ".")
        return await _claude_request(STABLE_CLAUDE, system, messages, image)


async def _call_gemini(model, system, messages, image=None):
    from google import genai  # nouveau paquet google-genai, importe a la demande
    from google.genai import types
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    contents = [
        {"role": ("user" if m["role"] == "user" else "model"),
         "parts": [{"text": m["content"]}]}
        for m in messages
    ]
    if image and contents:
        for c in reversed(contents):
            if c["role"] == "user":
                c["parts"] = ([types.Part(text=p["text"]) for p in c["parts"]]
                              + [types.Part.from_bytes(data=base64.b64decode(image["data"]),
                                                       mime_type=image["media_type"])])
                break
    resp = await client.aio.models.generate_content(
        model=model or DEFAULT_GEMINI,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=MAX_TOKENS,
        ),
    )
    um = getattr(resp, "usage_metadata", None)
    usage = {"model": model or DEFAULT_GEMINI,
             "input": getattr(um, "prompt_token_count", 0) or 0,
             "output": getattr(um, "candidates_token_count", 0) or 0}
    return (resp.text or ""), usage


def _openai_available() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


async def _call_openai(model, system, messages, image=None):
    from openai import AsyncOpenAI  # importe a la demande
    oai = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    msgs = [{"role": "system", "content": system}]
    for m in messages:
        role = m["role"] if m["role"] in ("user", "assistant") else "user"
        msgs.append({"role": role, "content": m["content"]})
    msgs = msgs[:1] + _attach_image_openai(msgs[1:], image)
    resp = await oai.chat.completions.create(model=model or DEFAULT_OPENAI, messages=msgs)
    u = getattr(resp, "usage", None)
    usage = {"model": model or DEFAULT_OPENAI,
             "input": getattr(u, "prompt_tokens", 0) or 0,
             "output": getattr(u, "completion_tokens", 0) or 0}
    return (resp.choices[0].message.content or ""), usage


async def _account_cost(task_id, usage, on_notice=None):
    """Accumule cout + tokens d'un appel sur la tache, persiste, emet un evenement, applique le plafond."""
    ctrl = running_tasks.get(task_id)
    if ctrl is None or not usage:
        return
    ctrl["cost"] = ctrl.get("cost", 0.0) + _usage_cost(usage)
    ctrl["in_tok"] = ctrl.get("in_tok", 0) + usage.get("input", 0)
    ctrl["out_tok"] = ctrl.get("out_tok", 0) + usage.get("output", 0)
    total = ctrl["cost"]
    await db_update_task(task_id, total_cost_usd=round(total, 6))
    await emit(task_id, 0, "systeme", "cost",
               {"cost": round(total, 4), "in": ctrl["in_tok"], "out": ctrl["out_tok"]})
    cap = ctrl.get("max_cost", 0) or 0
    if cap > 0 and total >= cap and not ctrl.get("over_budget"):
        ctrl["over_budget"] = True
        if on_notice:
            await on_notice("Budget atteint (%.2f $ >= %.2f $) -> arret de la tache." % (total, cap))


async def call_model(provider, model, system, messages, on_notice=None, image=None, task_id=None) -> str:
    log.info("[modele] appel %s (%s)%s", provider, model or _default_model(provider),
             " +image" if image else "")
    if provider == "gemini":
        if not _gemini_available():
            if on_notice:
                await on_notice("Gemini indisponible (GEMINI_API_KEY manquante) -> repli sur Claude.")
            text, usage = await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice, image)
        else:
            try:
                text, usage = await _call_gemini(model, system, messages, image)
                log.info("[modele] Gemini OK (%d caracteres)", len(text))
            except Exception as e:
                log.warning("[modele] Gemini erreur: %s", e)
                if on_notice:
                    await on_notice("Erreur Gemini (" + str(e)[:120] + ") -> repli sur Claude.")
                text, usage = await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice, image)
    elif provider == "openai":
        if not _openai_available():
            if on_notice:
                await on_notice("OpenAI indisponible (OPENAI_API_KEY manquante) -> repli sur Claude.")
            text, usage = await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice, image)
        else:
            try:
                text, usage = await _call_openai(model, system, messages, image)
                log.info("[modele] OpenAI OK (%d caracteres)", len(text))
            except Exception as e:
                log.warning("[modele] OpenAI erreur: %s", e)
                if on_notice:
                    await on_notice("Erreur OpenAI (" + str(e)[:120] + ") -> repli sur Claude.")
                text, usage = await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice, image)
    else:
        text, usage = await _call_claude(model or DEFAULT_CLAUDE, system, messages, on_notice, image)
    if task_id is not None:
        await _account_cost(task_id, usage, on_notice)
    return text


# ── Extraction JSON (calque de repair_json d'orchestrator.py) ────────────────
def _repair_json(s: str) -> str:
    result = []
    in_string = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "\\" and in_string:
            result.append(ch)
            i += 1
            if i < len(s):
                result.append(s[i])
            i += 1
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            i += 1
            continue
        if in_string:
            if ch == "\n":
                result.append("\\n")
            elif ch == "\r":
                result.append("\\r")
            elif ch == "\t":
                result.append("\\t")
            elif ord(ch) < 32:
                result.append("\\u{:04x}".format(ord(ch)))
            else:
                result.append(ch)
        else:
            result.append(ch)
        i += 1
    return "".join(result)


def _extract_json(text: str) -> Optional[dict]:
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = text[start:end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(_repair_json(candidate))
    except json.JSONDecodeError:
        return None


# ── Outils (cadres au dossier de la tache) ──────────────────────────────────
def _safe_path(folder: str, rel: str) -> Path:
    base = Path(folder).resolve()
    p = (base / (rel or "")).resolve()
    if p != base and base not in p.parents:
        raise ValueError("Chemin hors du dossier de travail: " + str(rel))
    return p


def _list_files(folder: str) -> list[str]:
    base = Path(folder)
    if not base.exists():
        return []
    out = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            out.append(str(p.relative_to(base)))
    return out[:200]


# Repertoires ignores lors de la lecture/listing du depot cible (mode entreprise).
_TARGET_IGNORE = {".git", "node_modules", ".venv", "venv", "__pycache__", ".pytest_cache",
                  "dist", "build", ".next", ".cache"}


def _target_file(target_path: str, rel: str):
    """Resolution sure d'un fichier DANS le depot cible (lecture seule). None si hors/inexistant."""
    if not target_path:
        return None
    base = Path(target_path).resolve()
    try:
        p = (base / (rel or "")).resolve()
    except Exception:
        return None
    if p != base and base not in p.parents:
        return None
    return p if p.is_file() else None


def _list_target_files(target_path: str) -> list[str]:
    base = Path(target_path) if target_path else None
    if not base or not base.exists():
        return []
    out = []
    for p in sorted(base.rglob("*")):
        if p.is_file() and not any(part in _TARGET_IGNORE for part in p.relative_to(base).parts):
            out.append(str(p.relative_to(base)))
        if len(out) >= 400:
            break
    return out


def _rmtree_force(path):
    """Suppression robuste (Windows) : retire le flag lecture seule des fichiers .git puis reessaie."""
    import stat

    def _onerror(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass
    shutil.rmtree(path, onerror=_onerror)
    return not Path(path).exists()


async def _clone_repo(url, token=None):
    """Clone un depot GitHub en LECTURE SEULE dans un cache local, et renvoie son chemin.
    Le token (depot prive) sert au clone puis est scrubbe (remote supprime) ; jamais stocke/log."""
    url = (url or "").strip().rstrip("/")
    if not re.match(r"^https://", url):
        raise ValueError("URL invalide : une URL https est requise.")
    m = re.search(r"github\.com/([^/]+)/([^/]+?)(?:\.git)?$", url)
    name = (m.group(1) + "_" + m.group(2)) if m else re.sub(r"[^A-Za-z0-9_.-]", "_", url)[-40:]
    dest = PROJECTS / ".repos" / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        ok = await asyncio.to_thread(_rmtree_force, str(dest))  # re-clone propre (gere .git read-only Windows)
        if not ok and dest.exists():
            # dernier recours : cloner dans un dossier voisin unique
            dest = PROJECTS / ".repos" / (name + "_" + uuid.uuid4().hex[:6])
    auth_url = url.replace("https://", "https://" + token + "@", 1) if token else url
    proc = await asyncio.create_subprocess_exec(
        "git", "clone", "--depth", "1", auth_url, str(dest),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        raise ValueError("git clone : delai depasse (depot trop gros ou reseau lent).")
    except FileNotFoundError:
        raise ValueError("git introuvable sur la machine (installe Git).")
    if proc.returncode != 0:
        msg = out.decode("utf-8", "replace")
        if token:
            msg = msg.replace(token, "***")  # ne jamais fuiter le token
        raise ValueError(msg.strip()[-300:] or "echec inconnu")
    # Scrubber le remote : aucune trace du token dans .git/config.
    if (dest / ".git").exists():
        try:
            p2 = await asyncio.create_subprocess_exec(
                "git", "-C", str(dest), "remote", "remove", "origin",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await p2.communicate()
        except Exception:
            pass
    return str(dest.resolve())


async def _run_cmd(folder: str, cmd: str, timeout: int = 60) -> str:
    try:
        parts = shlex.split(cmd or "")
    except ValueError:
        return "Commande invalide (guillemets non fermes)."
    if not parts:
        return "Commande vide."
    if parts[0] not in COMMAND_WHITELIST:
        return "Commande refusee (hors liste blanche): " + parts[0]
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *parts, cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (out.decode("utf-8", errors="replace") or "(vide)")[:2500]
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        return "TIMEOUT apres " + str(timeout) + "s."
    except FileNotFoundError:
        return "Programme introuvable: " + parts[0]


async def _run_pytest(folder: str, timeout: int = 120) -> str:
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pytest", "-q", cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (out.decode("utf-8", errors="replace") or "(vide)")[:2500]
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        return "TIMEOUT pytest apres " + str(timeout) + "s."


async def _web_search_native(query: str) -> str:
    """Recherche via l'outil serveur natif d'Anthropic : un seul appel, aucune dependance.

    L'outil tourne cote Anthropic ; il peut rendre la main avec "pause_turn" s'il a
    besoin de poursuivre (boucle serveur) -> on renvoie l'echange pour qu'il reprenne.
    """
    messages = [{"role": "user",
                 "content": "Recherche sur le web et resume de facon concise et factuelle : " + query}]
    tools = [{"type": "web_search_20260209", "name": "web_search"}]
    resp = await _client.messages.create(
        model="claude-haiku-4-5", max_tokens=2048, messages=messages, tools=tools)
    guard = 0
    while resp.stop_reason == "pause_turn" and guard < 4:
        messages.append({"role": "assistant", "content": resp.content})
        resp = await _client.messages.create(
            model="claude-haiku-4-5", max_tokens=2048, messages=messages, tools=tools)
        guard += 1
    return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()


async def _web_search_sdk(query: str) -> str:
    """Repli : claude-agent-sdk (utilise si l'outil serveur natif n'est pas disponible)."""
    from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions, ResultMessage
    out = []
    async for msg in sdk_query(
        prompt="Recherche sur le web et resume de facon concise : " + query,
        options=ClaudeAgentOptions(
            allowed_tools=["WebSearch", "WebFetch"],
            permission_mode="dontAsk",
            model="claude-haiku-4-5",
            max_budget_usd=0.30,
            max_turns=8,
        ),
    ):
        if isinstance(msg, ResultMessage) and msg.subtype == "success":
            out.append(msg.result)
    return NL.join(out).strip()


async def web_search(query: str) -> str:
    # Cache : une meme requete (a la casse/espaces pres) ne repaye pas un appel.
    key = " ".join((query or "").lower().split())
    if key and key in _web_cache:
        return _web_cache[key] + NL + "(resultat en cache)"
    try:
        result = await _web_search_native(query)
    except Exception as e:
        log.info("[web] outil natif indisponible (%s) -> repli claude-agent-sdk", str(e)[:120])
        try:
            result = await _web_search_sdk(query)
        except ImportError:
            return "Recherche web indisponible (outil natif KO et claude-agent-sdk absent)."
        except Exception as e2:
            return "Erreur recherche web: " + str(e2)[:200]
    result = (result or "Aucun resultat.")[:3000]
    if key and not result.startswith("Erreur"):
        if len(_web_cache) >= _WEB_CACHE_MAX:
            _web_cache.pop(next(iter(_web_cache)), None)  # evince la plus ancienne entree
        _web_cache[key] = result
    return result


# ── MCP : serveurs OFFICIELS uniquement (liste blanche) ──────────────────────
# Verifie cote serveur : un agent ne peut activer qu'un serveur de cette liste
# (serveurs de reference du projet Model Context Protocol).
OFFICIAL_MCP = {
    # Python : auto-installables via pip + Python courant -> AUCUN prerequis pour l'utilisateur.
    "fetch": {"kind": "python", "pip": "mcp-server-fetch", "module": "mcp_server_fetch"},
    "git": {"kind": "python", "pip": "mcp-server-git", "module": "mcp_server_git",
            "args": ["--repository", "{folder}"]},
    "time": {"kind": "python", "pip": "mcp-server-time", "module": "mcp_server_time"},
    # Node : necessitent npx (Node.js), non auto-installable de facon fiable.
    "filesystem": {"kind": "npx", "package": "@modelcontextprotocol/server-filesystem",
                   "args": ["{folder}"]},
    "memory": {"kind": "npx", "package": "@modelcontextprotocol/server-memory"},
    "sequentialthinking": {"kind": "npx", "package": "@modelcontextprotocol/server-sequential-thinking"},
    "everything": {"kind": "npx", "package": "@modelcontextprotocol/server-everything"},
}


async def _pip_install(pkg):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "pip", "install", "-q", pkg,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), timeout=240)
    if proc.returncode != 0:
        raise RuntimeError("pip install " + pkg + " a echoue : "
                           + out.decode("utf-8", "replace")[-300:])


async def _ensure_mcp_command(name, folder):
    """Construit la commande de lancement, en installant le serveur si besoin (Python)."""
    spec = OFFICIAL_MCP[name]
    args = [a.replace("{folder}", folder) for a in spec.get("args", [])]
    if spec["kind"] == "python":
        import importlib.util
        try:
            present = importlib.util.find_spec(spec["module"]) is not None
        except Exception:
            present = False
        if not present:
            await _pip_install(spec["pip"])   # les agents installent eux-memes
        return [sys.executable, "-m", spec["module"]] + args
    return ["npx", "-y", spec["package"]] + args   # kind == "npx"


class MCPServer:
    """Client MCP minimal (JSON-RPC 2.0 sur stdio, messages delimites par newline)."""

    def __init__(self, name, command):
        self.name = name
        self.command = command
        self.proc = None
        self.tools = []
        self._id = 0

    async def _send(self, method, params=None, notification=False):
        self._id += 1
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notification:
            msg["id"] = self._id
        self.proc.stdin.write((json.dumps(msg) + NL).encode("utf-8"))
        await self.proc.stdin.drain()
        if notification:
            return None
        want = self._id
        while True:
            line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=30)
            if not line:
                raise RuntimeError("serveur MCP ferme")
            try:
                resp = json.loads(line)
            except json.JSONDecodeError:
                continue
            if resp.get("id") == want:
                return resp

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await self._send("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "orchestrateur", "version": "1.0"},
        })
        await self._send("notifications/initialized", notification=True)
        resp = await self._send("tools/list", {})
        self.tools = (resp.get("result") or {}).get("tools", [])

    async def call(self, tool, arguments):
        resp = await self._send("tools/call", {"name": tool, "arguments": arguments or {}})
        if "error" in resp:
            return "Erreur MCP : " + json.dumps(resp["error"], ensure_ascii=False)
        result = resp.get("result", {})
        parts = [c.get("text", "") for c in result.get("content", []) if c.get("type") == "text"]
        return (NL.join(parts) or json.dumps(result, ensure_ascii=False))[:3000]

    async def stop(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
            except Exception:
                pass


async def _tool_add_mcp(task_id, folder, server):
    server = (server or "").strip()
    if server not in OFFICIAL_MCP:
        return ("Serveur MCP refuse : '" + server + "' n'est pas un serveur OFFICIEL. "
                "Serveurs autorises : " + ", ".join(sorted(OFFICIAL_MCP)) + ".")
    ctrl = running_tasks.get(task_id)
    if ctrl is None:
        return "Tache non active."
    registry = ctrl.setdefault("mcp", {})
    if server in registry:
        return ("Serveur MCP '" + server + "' deja actif. Outils : "
                + ", ".join(t.get("name", "") for t in registry[server].tools))
    try:
        command = await _ensure_mcp_command(server, folder)
    except Exception as e:
        return "Echec preparation MCP '" + server + "' : " + str(e)[:250]
    s = MCPServer(server, command)
    try:
        await asyncio.wait_for(s.start(), timeout=90)
    except FileNotFoundError:
        return ("Le serveur MCP '" + server + "' necessite Node.js (npx), absent. "
                "Prefere un serveur Python qui s'installe seul : fetch, git ou time.")
    except Exception as e:
        return "Echec demarrage MCP '" + server + "' : " + str(e)[:200]
    registry[server] = s
    return ("Serveur MCP officiel '" + server + "' demarre. Outils : "
            + ", ".join(t.get("name", "") for t in s.tools)
            + ". Appelle-les avec mcp_call.")


async def _tool_mcp_call(task_id, server, tool, arguments):
    registry = (running_tasks.get(task_id) or {}).get("mcp", {})
    s = registry.get((server or "").strip())
    if not s:
        return "Serveur MCP '" + str(server) + "' non demarre. Fais d'abord add_mcp."
    try:
        return await asyncio.wait_for(s.call(tool, arguments), timeout=60)
    except Exception as e:
        return "Erreur mcp_call : " + str(e)[:200]


async def _stop_mcp(task_id):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        return
    for s in list(ctrl.get("mcp", {}).values()):
        try:
            await s.stop()
        except Exception:
            pass
    ctrl["mcp"] = {}


# ── Memoire long terme partagee (#12) ────────────────────────────────────────
async def _save_knowledge(content, agent_name=None):
    """Memorise une lecon. Si un agent l'appelle, range dans son namespace ; sinon _shared."""
    import memory
    content = (content or "").strip()
    if not content:
        return "Rien a memoriser."
    ns = memory.role_namespace(agent_name) if agent_name else "_shared"
    src = ("agent:" + agent_name) if agent_name else "chef"
    rid = await memory.remember(content[:4000], namespace=ns, source=src)
    return ("Memorise dans " + ns + " (id #" + str(rid) + ", reutilisable par les futures taches).") \
        if rid else "Memorisation refusee."


async def _search_knowledge(query, agent_name=None):
    """Cherche dans (namespace de l'agent + _shared + _user).
    Semantique si embeddings dispo (OPENAI_API_KEY), sinon LIKE."""
    import memory
    query = (query or "").strip()
    if not query:
        return "Requete vide."
    namespaces = ["_shared", "_user"]
    if agent_name:
        namespaces.insert(0, memory.role_namespace(agent_name))
    results = await memory.recall(query, namespaces=namespaces, k=5)
    if not results:
        return "Aucune connaissance trouvee."
    lines = []
    for r in results:
        prefix = "[" + str(r.get("namespace", "?")) + "] "
        score = " (~" + str(r.get("score", "")) + ")" if r.get("score") is not None else ""
        lines.append("- " + prefix + r["content"] + score)
    return NL.join(lines)


async def execute_tool(task_id: int, folder: str, web_enabled: bool, act: dict,
                       agent_name: Optional[str] = None) -> str:
    tool = act.get("tool")
    try:
        if tool == "write_file":
            p = _safe_path(folder, act.get("path", ""))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(act.get("content", ""), encoding="utf-8")
            return "Fichier ecrit: " + str(act.get("path"))
        if tool == "read_file":
            rel = act.get("path", "")
            p = _safe_path(folder, rel)
            if p.exists():
                return p.read_text(encoding="utf-8", errors="replace")[:5000]
            # Fallback lecture seule sur le depot cible (mode entreprise).
            t = await db_get_task(task_id)
            if t and t.get("company_mode") and t.get("target_path"):
                tp = _target_file(t["target_path"], rel)
                if tp:
                    return ("[depot cible, lecture seule] "
                            + tp.read_text(encoding="utf-8", errors="replace")[:5000])
            return "Introuvable: " + str(rel)
        if tool == "run_command":
            return await _run_cmd(folder, act.get("cmd", ""))
        if tool == "run_tests":
            return await _run_pytest(folder)
        if tool == "web_search":
            if not web_enabled:
                return "Recherche web desactivee pour cette tache."
            return await web_search(act.get("query", ""))
        if tool == "add_mcp":
            return await _tool_add_mcp(task_id, folder, act.get("server", ""))
        if tool == "mcp_call":
            return await _tool_mcp_call(task_id, act.get("server", ""),
                                        act.get("name") or act.get("tool_name") or "",
                                        act.get("arguments") or {})
        if tool == "save_knowledge":
            return await _save_knowledge(act.get("content", ""), agent_name=agent_name)
        if tool == "search_knowledge":
            return await _search_knowledge(act.get("query", ""), agent_name=agent_name)
        return "Outil inconnu: " + str(tool)
    except Exception as e:
        return "Erreur outil " + str(tool) + ": " + str(e)


# ── Prompts systeme ─────────────────────────────────────────────────────────
CHEF_SYSTEM = """Tu es le CHEF d'une equipe d'agents IA autonomes.

OBJECTIF GLOBAL (fixe par l'utilisateur) :
{objective}

Tu construis et diriges l'equipe toi-meme. Tu peux creer jusqu'a {max_agents} agents.
A CHAQUE TOUR, reponds UNIQUEMENT avec un objet JSON (aucun texte autour) decrivant ta PROCHAINE action :

- Creer un agent :
  {{"thought":"...","action":"create_agent","name":"NomCourt","role":"role / specialite","provider":"claude"}}
  ("provider" peut etre "claude", "gemini" ou "openai")
  Par defaut les agents tournent sur un modele economique (Claude Sonnet). Pour une tache complexe,
  tu peux ajouter "model":"claude-opus-4-8" afin de surclasser cet agent.

- Confier une sous-tache a un agent existant :
  {{"thought":"...","action":"assign_task","agent":"NomDeLAgent","instruction":"consigne precise et autonome"}}

- Confier PLUSIEURS sous-taches EN PARALLELE (gagne du temps) :
  {{"thought":"...","action":"parallel_assign","assignments":[{{"agent":"DevA","instruction":"le frontend"}},{{"agent":"DevB","instruction":"le backend"}}]}}
  IMPORTANT : agents DISTINCTS et FICHIERS DISJOINTS uniquement (sinon risque de conflits).

- Lancer un CHALLENGE (verification croisee entre les trois modeles) :
  {{"thought":"...","action":"challenge","instruction":"consigne precise","rounds":2}}
  Un Producteur (Claude par defaut) realise le travail, puis les DEUX autres modeles (Gemini et ChatGPT)
  le critiquent chacun de leur cote ; il n'est valide que si les deux critiques approuvent, sinon il
  corrige sur plusieurs tours. Utilise-le pour les livrables importants ou sensibles aux erreurs.

- Deleguer a un AGENT ANTHROPIC GERE (heberge cote Anthropic, identifie par son `agent_id`) :
  {{"thought":"...","action":"assign_managed_agent","agent_id":"agent_...","instruction":"consigne autonome"}}
  La liste des agents disponibles t'est fournie dans le contexte (champ "Agents Anthropic geres").
  Utilise-les quand l'un d'eux a deja le role/contexte recherche (ex. un "Watcher" pour la veille).
  Le retour est UN TEXTE seulement (lecture) -- l'agent gere ne touche pas a tes fichiers locaux.

- Terminer (objectif atteint) :
  {{"thought":"...","action":"finish","final":"resume du resultat livre"}}

Regles :
- Decompose l'objectif, cree les roles utiles (ex: chercheur, codeur, redacteur, critique), delegue, fais iterer puis termine.
- Un seul JSON, une seule action par tour.
- Cree un agent AVANT de lui assigner une tache.
- Les agents ecrivent leurs livrables dans des fichiers du dossier partage.
- Pour un livrable important ou sensible aux erreurs, prefere "challenge" a un simple "assign_task" afin que les deux modeles se confrontent.
- Quand le travail est valide, utilise "finish"."""


# Prompt du "CEO" en mode entreprise tech (autopilote d'audit de depot externe).
COMPANY_CHEF_SYSTEM = """Tu es le PDG/CTO d'une ENTREPRISE TECH autonome (une entite a part entiere,
distincte du projet que tu traites). Tu diriges une equipe d'agents comme une vraie boite.

CLIENT / PREMIER PROJET : le depot situe en LECTURE SEULE a :
{target_path}

MISSION (nord) : {objective}
Personne ne t'a donne de liste de taches : c'est a TOI de trouver quoi faire. Analyse le depot, puis
propose et realise : correction de bugs, nouvelles features, ameliorations front-end et back-end,
securite, performance, UX, qualite. Vise l'excellence (faire mieux que la concurrence) sans jamais
casser la securite.

REGLES ABSOLUES :
- Tu NE MODIFIES JAMAIS le depot d'origine (lecture seule). Les agents le LISENT (read_file) pour
  l'auditer, mais ecrivent UNIQUEMENT dans le sous-dossier "delivery/" du dossier de travail.
- Tu NE POUSSES PAS sur GitHub et tu NE MERGES PAS. C'est Claude Code qui verifiera et donnera son aval.
- Tu peux tester tes correctifs dans une sandbox locale (copie dans delivery/), pas sur l'original.

LIVRABLES attendus dans delivery/ :
- delivery/audit_report.md : analyse du depot (architecture, problemes, risques securite, dette).
- delivery/backlog.md : idees PRIORISEES que TU as trouvees (bugs, features, front, back), avec impact/effort.
- delivery/patches/ ou delivery/<domaine>/ : le code propose (correctifs, nouvelles features), teste localement.
- delivery/HANDOFF.md : resume pour Claude Code (quoi a verifier/appliquer, fichiers concernes, comment tester).

Tu peux creer jusqu'a {max_agents} agents (analystes, dev front, dev back, QA, securite, recherche...).
A CHAQUE TOUR, reponds UNIQUEMENT avec un objet JSON (aucun texte autour) :
- {{"thought":"...","action":"create_agent","name":"NomCourt","role":"role","provider":"claude"}}  (provider: claude|gemini|openai ; agents en modele economique par defaut, ajoute "model":"claude-opus-4-8" pour surclasser une tache complexe)
- {{"thought":"...","action":"assign_task","agent":"Nom","instruction":"consigne precise"}}
- {{"thought":"...","action":"parallel_assign","assignments":[{{"agent":"A","instruction":"..."}},{{"agent":"B","instruction":"..."}}]}}  (agents distincts, fichiers disjoints)
- {{"thought":"...","action":"challenge","instruction":"...","rounds":2}}  (verification croisee 3 modeles)
- {{"thought":"...","action":"finish","final":"resume des livrables dans delivery/ + chemin a transmettre a Claude Code"}}

Regles : un seul JSON par tour ; cree un agent avant de lui assigner une tache ; rappelle a chaque
agent d'ecrire UNIQUEMENT dans delivery/ ; quand le travail est livre, "finish".

METHODE DE TRAVAIL OBLIGATOIRE (qualite avant volume — un humain "Claude Code" relira et appliquera) :
1. VERITE TERRAIN D'ABORD. Avant TOUTE proposition, fais LIRE le code reel concerne (read_file sur le
   depot cible : `schema.sql`, migrations, config/bindings, le modele de stockage, les fichiers vises,
   et le CLAUDE.md/conventions du depot s'il existe). Ne SUPPOSE JAMAIS qu'une table, un binding, un
   endpoint, un stockage (serveur vs client/IndexedDB) existe : verifie. Toute hypothese non verifiee
   doit etre marquee "HYPOTHESE A VALIDER" dans le livrable.
2. NE JAMAIS AFFAIBLIR LA SECURITE. Lis les protections en place (CSRF, auth, rate-limit, sanitization,
   verif d'Origin) AVANT d'y toucher ; ne retire/contourne JAMAIS une protection existante ; si un
   changement peut regresser la securite, ecris-le en gras dans le HANDOFF.
3. PEU MAIS QUI MARCHE. Privilegie un PETIT nombre de correctifs reellement applicables (verifies contre
   le code reel et testes) plutot qu'un gros dump speculatif. Ne reference AUCUN fichier que tu n'as pas
   reellement produit dans delivery/.
4. BUSINESS = PROPOSITIONS, PAS DECRETS. Prix, strategie, roadmap, branding : propose des OPTIONS + une
   recommandation et laisse l'humain TRANCHER. Reste coherent (memes chiffres partout). N'ecris jamais
   "ne pas rediscuter".
5. HANDOFF HONNETE. Dans delivery/HANDOFF.md, classe chaque livrable : "VERIFIE contre le code reel" /
   "HYPOTHESE A VALIDER" / "BLOQUANT". Liste les fichiers reels concernes et comment tester.
6. RESPECTE LES PRIORITES deja fixees par l'utilisateur : ne supprime pas une feature demandee sans le
   signaler explicitement."""

WORKER_SYSTEM = """Tu es l'agent << {name} >>. Ton role : {role}.
Tu travailles dans un dossier de travail partage avec ton equipe. Recherche web disponible : {web}.

Reponds UNIQUEMENT avec un objet JSON decrivant tes actions pour avancer sur la mission confiee :
{{"thought":"ton raisonnement",
  "actions":[
    {{"tool":"write_file","path":"relatif.ext","content":"..."}},
    {{"tool":"read_file","path":"..."}},
    {{"tool":"run_command","cmd":"python script.py"}},
    {{"tool":"run_tests"}},
    {{"tool":"web_search","query":"..."}},
    {{"tool":"add_mcp","server":"filesystem"}},
    {{"tool":"mcp_call","server":"filesystem","name":"list_directory","arguments":{{}}}},
    {{"tool":"search_knowledge","query":"astuce dependance windows"}},
    {{"tool":"save_knowledge","content":"Lecon apprise reutilisable pour les futures taches"}}
  ],
  "done": false,
  "report":"(quand done=true) resume clair de ce que tu as produit"}}

Regles :
- Ecris tes livrables dans des fichiers (write_file), chemins relatifs au dossier de travail.
- Commandes autorisees (run_command) : {whitelist}.
- Tu peux installer des paquets Python via run_command (ex: "pip install requests").
- Outils MCP OFFICIELS uniquement : active un serveur avec add_mcp (autorises : {mcp_servers}),
  puis utilise ses outils via mcp_call. Tout serveur non officiel est refuse automatiquement.
  fetch/git/time s'installent tout seuls (Python) ; filesystem/memory/sequentialthinking/everything
  necessitent Node.js. Tu peux aussi installer des paquets toi-meme via run_command (pip install ...).
- Memoire persistante : search_knowledge(query) cherche dans (TON namespace de role + lecons
  partagees + notes utilisateur). save_knowledge(content) ENREGISTRE une astuce dans TON namespace
  de role -- elle te servira (ainsi qu'aux futurs agents du meme role) sur les taches a venir.
  Consulte la memoire AU DEBUT de chaque mission, et enregistre une lecon AVANT de finir si tu as
  appris quelque chose de generalisable.
- Quand ta mission est accomplie : "done": true et un "report". Sinon "done": false avec des actions.
- Un seul JSON par reponse, aucun texte autour."""


# ── Controle de la boucle ────────────────────────────────────────────────────
async def _wait_if_paused(task_id):
    ctrl = running_tasks.get(task_id)
    if ctrl:
        await ctrl["pause"].wait()


async def _is_stopped(task_id) -> bool:
    t = await db_get_task(task_id)
    return (t is None) or (t["status"] == "stopped")


# ── Moteur : un worker execute une sous-tache ───────────────────────────────
async def _run_worker(task_id, iteration, agent, instruction, folder, web_enabled,
                      worker_histories, notice, image=None, custom_hist=None) -> str:
    name = agent["name"]
    # custom_hist : historique isole (assignations paralleles au meme agent) ; sinon partage.
    hist = custom_hist if custom_hist is not None else worker_histories.setdefault(name, [])
    hist.append({"role": "user", "content":
                 "Mission du chef : " + instruction + NL + NL + "Reponds en JSON avec tes actions."})
    system = WORKER_SYSTEM.format(
        name=name, role=agent["role"],
        web=("oui" if web_enabled else "non"),
        whitelist=", ".join(sorted(COMMAND_WHITELIST)),
        mcp_servers=", ".join(sorted(OFFICIAL_MCP)),
    )
    # Mode entreprise : lire le depot cible (lecture seule), ecrire UNIQUEMENT dans delivery/.
    _t = await db_get_task(task_id)
    company = bool(_t and _t.get("company_mode"))
    if company:
        system += (NL + "MODE ENTREPRISE : le depot d'origine est en LECTURE SEULE (read_file le voit). "
                   "Tu ecris (write_file) UNIQUEMENT dans 'delivery/...'. Ne tente jamais d'ecrire hors du "
                   "dossier de travail. Teste tes correctifs dans delivery/ avant de livrer." + NL
                   + "AVANT de proposer un changement : LIS le code reel concerne (read_file sur la cible : "
                   "schema.sql, migrations, config/bindings, fichiers vises, CLAUDE.md). Ne suppose pas "
                   "qu'une table/binding/endpoint/stockage existe — verifie. Marque toute hypothese "
                   "'HYPOTHESE A VALIDER'. Ne RETIRE jamais une protection de securite existante (CSRF, "
                   "auth, rate-limit, sanitization). Peu de correctifs qui marchent > un gros dump. Ne "
                   "reference aucun fichier que tu n'as pas reellement ecrit.")
    max_steps = COMPANY_WORKER_STEPS if company else WORKER_MAX_STEPS
    last_report = "(aucun rapport)"
    for _step in range(max_steps):
        await _wait_if_paused(task_id)
        if await _is_stopped(task_id):
            break
        _c = running_tasks.get(task_id)
        if _c and _c.get("over_budget"):
            return last_report  # plafond budget atteint : on rend la main au chef
        await _log_prompt(task_id, iteration, name, system, hist, image)
        raw = await call_model(agent["provider"], agent["model"], system, hist,
                               on_notice=notice, image=image, task_id=task_id)
        hist.append({"role": "assistant", "content": raw})
        data = _extract_json(raw) or {}

        thought = data.get("thought", "")
        if thought:
            await emit(task_id, iteration, name, "agent_message", {"kind": "thought", "content": thought})

        async def _exec_one(act):
            tool = act.get("tool")
            await emit(task_id, iteration, name, "tool_call",
                       {"tool": tool, "input": {k: v for k, v in act.items() if k != "tool"}})
            out = await execute_tool(task_id, folder, web_enabled, act, agent_name=name)
            await emit(task_id, iteration, name, "tool_result", {"tool": tool, "output": out[:1500]})
            return tool, out

        actions = data.get("actions", []) or []
        # Lecture seule -> parallelisable (#15) ; mutations -> sequentiel (evite les races fichiers).
        parallel = [a for a in actions if a.get("tool") in PARALLEL_SAFE_TOOLS]
        sequential = [a for a in actions if a.get("tool") not in PARALLEL_SAFE_TOOLS]
        results = []
        touched = False
        if parallel:
            for tool, out in await asyncio.gather(*[_exec_one(a) for a in parallel]):
                results.append("[" + str(tool) + "] " + out)
        for act in sequential:
            tool, out = await _exec_one(act)
            results.append("[" + str(tool) + "] " + out)
            if tool in ("write_file", "run_command", "run_tests"):
                touched = True
        if touched:
            await emit(task_id, iteration, name, "files_changed", {"list": _list_files(folder)})

        if data.get("done"):
            last_report = data.get("report") or thought or "Termine."
            await emit(task_id, iteration, name, "agent_message", {"kind": "result", "content": last_report})
            return last_report

        if results:
            hist.append({"role": "user", "content":
                         ("Resultats :" + NL + NL.join(results))[:4000]
                         + NL + NL + "Continue, ou termine avec done:true et un report."})
        else:
            hist.append({"role": "user", "content":
                         "Aucune action executee. Propose des actions, ou termine avec done:true + report."})
    return last_report


# ── Moteur : challenge (critique croisee entre les deux modeles) ─────────────
CRITIC_SYSTEM = """Tu es un CRITIQUE adversarial. Ton seul but : trouver les ERREURS, oublis,
cas limites non geres, et ecarts par rapport a la consigne dans le travail d'un autre agent
(qui tourne sur un modele different du tien). Ne sois pas complaisant.

Tu recois la consigne, le rapport du producteur et le contenu des fichiers produits.
Reponds UNIQUEMENT avec un objet JSON :
{"verdict":"approved" ou "needs_changes",
 "issues":["probleme concret et verifiable 1","probleme 2"],
 "comment":"synthese courte de ton evaluation"}

N'approuve ("approved") que si le travail repond vraiment a la consigne sans erreur visible.
Sinon "needs_changes" avec des points precis et actionnables."""


async def _ensure_agent(task_id, name, role, provider, model, iteration):
    existing = await db_get_agent(task_id, name)
    if existing:
        return existing
    await db_add_agent(task_id, name, role, provider, model, "chef")
    await emit(task_id, iteration, "chef", "agent_created",
               {"name": name, "role": role, "model": provider + ":" + model})
    return await db_get_agent(task_id, name)


async def _run_critic(task_id, iteration, critic_agent, instruction, producer_report, folder, notice, image=None):
    name = critic_agent["name"]
    parts, budget = [], 0
    for f in _list_files(folder):
        try:
            content = _safe_path(folder, f).read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        snippet = content[:1500]
        budget += len(snippet)
        parts.append("### " + f + NL + snippet)
        if budget > 8000:
            break
    files_blob = (NL + NL).join(parts) or "(aucun fichier)"
    user = ("Consigne de la sous-tache :" + NL + instruction
            + NL + NL + "Rapport du producteur :" + NL + (producer_report or "(aucun)")
            + NL + NL + "Fichiers produits :" + NL + files_blob
            + NL + NL + "Evalue de facon critique. Reponds en JSON.")
    await _log_prompt(task_id, iteration, critic_agent["name"], CRITIC_SYSTEM,
                      [{"role": "user", "content": user}], image)
    raw = await call_model(critic_agent["provider"], critic_agent["model"],
                           CRITIC_SYSTEM, [{"role": "user", "content": user}],
                           on_notice=notice, image=image, task_id=task_id)
    data = _extract_json(raw) or {}
    verdict = "approved" if data.get("verdict") == "approved" else "needs_changes"
    issues = data.get("issues") or []
    comment = data.get("comment", "")
    await emit(task_id, iteration, name, "critique",
               {"verdict": verdict, "issues": issues, "comment": comment})
    return verdict, issues, comment


PROVIDER_LABEL = {"claude": "Claude", "gemini": "Gemini", "openai": "ChatGPT"}


async def _run_challenge(task_id, iteration, instruction, folder, web_enabled, rounds,
                         worker_histories, notice,
                         producer_provider="claude", critic_providers=None, image=None):
    rounds = max(1, min(int(rounds or 2), 4))
    if not critic_providers:
        critic_providers = [p for p in ("claude", "gemini", "openai") if p != producer_provider]
    seen = set()
    critic_providers = [p for p in critic_providers if not (p in seen or seen.add(p))]

    prod = await _ensure_agent(
        task_id, "Producteur (" + PROVIDER_LABEL.get(producer_provider, producer_provider) + ")",
        "produit et corrige la solution",
        producer_provider, _default_model(producer_provider), iteration)
    critics = []
    for cp in critic_providers:
        critics.append(await _ensure_agent(
            task_id, "Critique (" + PROVIDER_LABEL.get(cp, cp) + ")",
            "cherche les erreurs et conteste le travail",
            cp, _default_model(cp), iteration))

    last_issues, final_report, last_comments = [], "", []
    for r in range(1, rounds + 1):
        await _wait_if_paused(task_id)
        if await _is_stopped(task_id):
            break
        await emit(task_id, iteration, "chef", "challenge_round", {"round": r, "total": rounds})

        prod_instruction = instruction
        if last_issues:
            prod_instruction = (instruction + NL + NL
                                + "Les critiques ont souleve ces points, corrige-les :" + NL
                                + NL.join("- " + str(i) for i in last_issues))
        final_report = await _run_worker(task_id, iteration, prod, prod_instruction,
                                         folder, web_enabled, worker_histories, notice, image)

        round_issues, all_approved, last_comments = [], True, []
        for crit in critics:
            await _wait_if_paused(task_id)
            if await _is_stopped(task_id):
                all_approved = False
                break
            verdict, issues, comment = await _run_critic(
                task_id, iteration, crit, instruction, final_report, folder, notice, image)
            last_comments.append(crit["name"] + " : " + (comment or verdict))
            if verdict != "approved":
                all_approved = False
                round_issues.extend(issues)
        last_issues = round_issues
        if all_approved:
            return ("Challenge approuve a l'unanimite au round " + str(r) + "/" + str(rounds)
                    + " par " + ", ".join(c["name"] for c in critics)
                    + ". Resultat : " + final_report[:700])
    return ("Challenge termine apres " + str(rounds) + " round(s) sans approbation unanime. "
            + "Avis : " + (" | ".join(last_comments) or "n/a")
            + ". Points restants : " + ("; ".join(str(i) for i in last_issues) if last_issues else "aucun")
            + ". Resultat : " + final_report[:600])


# ── Moteur : la boucle du chef ───────────────────────────────────────────────
async def _resume_context(task_id, folder):
    """Reconstruit un resume du travail deja fait (depuis la DB + les fichiers) pour reprendre une tache."""
    msgs = await db_messages_after(task_id, 0)
    decisions, results = [], []
    for m in msgs:
        try:
            c = json.loads(m["content"])
        except Exception:
            continue
        if m["kind"] == "chef" and c.get("decision"):
            line = "tour " + str(m["iteration"]) + " : " + str(c.get("decision"))
            if c.get("instruction"):
                line += " — " + str(c["instruction"])[:120]
            decisions.append(line)
        elif m["kind"] == "agent_message" and c.get("kind") == "result":
            results.append(str(m["agent"]) + " : " + str(c.get("content", ""))[:150])
    agents = await db_list_agents(task_id)
    files = _list_files(folder)
    parts = ["Tu REPRENDS une tache qui a ete interrompue (souvent coupee en plein milieu)."]
    if agents:
        parts.append("Ton equipe existe DEJA (ne les recree pas) : "
                     + ", ".join(a["name"] + " (" + a["role"] + ")" for a in agents))
    if decisions:
        parts.append("Tes decisions passees :" + NL + NL.join("- " + d for d in decisions[-15:]))
    if results:
        parts.append("Derniers resultats d'agents :" + NL + NL.join("- " + r for r in results[-10:]))
    parts.append("Fichiers deja produits : " + (", ".join(files[:80]) or "(aucun)"))
    parts.append("Relis les fichiers au besoin (read_file). NE REFAIS PAS le travail deja fait : "
                 "reprends la ou ca s'est arrete et avance vers l'objectif. Prochaine action en JSON.")
    return (NL + NL).join(parts)


def _company_sanity(folder):
    """Verifie la coherence de la livraison : liste reelle des fichiers de delivery/ et
    references 'delivery/...' citees dans les .md mais absentes du disque (fichiers fantomes)."""
    base = Path(folder)
    delivery = base / "delivery"
    if not delivery.exists():
        return None
    actual = sorted(str(p.relative_to(base)).replace("\\", "/")
                    for p in delivery.rglob("*") if p.is_file() and p.name != "_MANIFEST.md")
    actual_set = set(actual)
    referenced = set()
    for md in delivery.rglob("*.md"):
        try:
            txt = md.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for ref in re.findall(r"delivery/[A-Za-z0-9_./-]+\.[A-Za-z0-9]+", txt):
            referenced.add(ref.replace("\\", "/"))
    missing = sorted(r for r in referenced if r not in actual_set)
    return {"files": actual, "missing": missing}


def _write_manifest(folder, sanity):
    lines = ["# Manifest de livraison (genere automatiquement)", "",
             str(len(sanity["files"])) + " fichier(s) reellement presents dans delivery/ :", ""]
    lines += ["- " + f for f in sanity["files"]]
    if sanity["missing"]:
        lines += ["", "## ATTENTION : references absentes du disque (fichiers fantomes)", ""]
        lines += ["- " + m for m in sanity["missing"]]
    try:
        (Path(folder) / "delivery" / "_MANIFEST.md").write_text(NL.join(lines), encoding="utf-8")
    except Exception as e:
        log.warning("manifest non ecrit: %s", e)


async def _run_task(task_id, resume=False):
    try:
        task = await db_get_task(task_id)
        folder = task["folder"]
        Path(folder).mkdir(parents=True, exist_ok=True)
        objective = task["objective"]
        max_iter = task["max_iterations"]
        max_agents = task["max_agents"]
        web_enabled = bool(task["web_enabled"])
        chef_model = task["chef_model"]
        image = _load_image(task.get("image_path"))
        company = bool(task.get("company_mode"))
        target_path = task.get("target_path")

        # Maitrise du cout : plafond USD par tache + cumul (repris du total persiste).
        ctrl0 = running_tasks.get(task_id)
        if ctrl0 is not None:
            ctrl0["max_cost"] = float(task.get("max_cost_usd") or 0)
            ctrl0["cost"] = float(task.get("total_cost_usd") or 0) if resume else 0.0
            ctrl0.setdefault("in_tok", 0)
            ctrl0.setdefault("out_tok", 0)
            ctrl0["over_budget"] = False

        async def notice(msg):
            await emit(task_id, 0, "systeme", "info", {"msg": msg})

        if company:
            chef_system = COMPANY_CHEF_SYSTEM.format(
                objective=objective, max_agents=max_agents, target_path=target_path or "(non defini)")
        else:
            chef_system = CHEF_SYSTEM.format(objective=objective, max_agents=max_agents)
        start = "Demarre. Quelle est ta premiere action (create_agent, assign_task ou finish) ? Reponds en JSON."
        if image:
            start = ("Une maquette de design a ete fournie par l'utilisateur ; elle est transmise aux agents "
                     "qui produisent l'UI (demande-leur de s'en inspirer fidelement : couleurs, structure, composants). "
                     + start)

        # Memoire : notes utilisateur + leçons partagees les plus pertinentes pour cet objectif
        try:
            import memory as _mem
            user_notes = await _mem.list_namespace("_user", limit=20)
            shared_hits = await _mem.recall(objective, namespaces=["_shared"], k=5)
        except Exception as e:
            log.info("[memoire] indispo (%s)", e)
            user_notes, shared_hits = [], []
        memory_block = ""
        if user_notes:
            memory_block += (NL + "Notes utilisateur PERSISTANTES (a respecter) :" + NL
                             + NL.join("- " + n["content"] for n in user_notes[:20]))
        if shared_hits:
            memory_block += (NL + NL + "Lecons utiles deja apprises sur des taches passees :" + NL
                             + NL.join("- " + h["content"] for h in shared_hits[:5]))
        if memory_block:
            start = start + NL + memory_block

        # Agents Anthropic geres disponibles sur le compte : le chef peut leur deleguer
        # via "assign_managed_agent". Echec de listing -> on continue sans (fonctionne pareil).
        managed_block = ""
        try:
            managed = await managed_agents.list_agents()
            actives = [a for a in managed if (a.get("status") or "").lower() in ("actif", "active", "")]
            if actives:
                lines = [a["id"] + " — " + (a.get("name") or "") for a in actives[:60]]
                managed_block = (NL + "Agents Anthropic geres disponibles (action 'assign_managed_agent') :"
                                 + NL + NL.join("- " + l for l in lines))
        except Exception as e:
            log.info("[managed-agents] listing indisponible (%s) — le chef tournera sans.", e)

        worker_histories: dict[str, list] = {}
        if resume:
            iteration = int(task.get("iteration") or 0)
            ceiling = COMPANY_MAX_ITER if company else MAX_ITER_CEILING
            max_iter = min(max(max_iter, iteration + 20), ceiling)  # garantit du budget pour continuer
            chef_messages = [{"role": "user", "content": await _resume_context(task_id, folder)}]
            await emit(task_id, iteration, "systeme", "info",
                       {"msg": "Reprise de la tache au tour " + str(iteration) + " (contexte reconstruit)."})
        else:
            chef_messages = [{"role": "user", "content": start + managed_block}]
            iteration = 0

        await db_update_task(task_id, status="running")
        while iteration < max_iter:
            await _wait_if_paused(task_id)
            if await _is_stopped(task_id):
                break

            ctrl = running_tasks.get(task_id)
            if ctrl and ctrl.get("over_budget"):
                msg = ("Budget maximal atteint (%.2f $ depenses). Arret de la tache. "
                       "Augmente le plafond puis reprends si besoin." % ctrl.get("cost", 0.0))
                await emit(task_id, iteration, "systeme", "info", {"msg": msg})
                await emit(task_id, iteration, "chef", "done", {"summary": msg})
                await db_update_task(task_id, status="stopped")
                return
            if ctrl and ctrl.get("inbox"):
                pending, ctrl["inbox"] = ctrl["inbox"], []
                for um in pending:
                    chef_messages.append({"role": "user", "content":
                        "Nouvelle consigne de l'utilisateur (integre-la a l'objectif courant) : " + um})

            iteration += 1
            await db_update_task(task_id, iteration=iteration)
            await emit(task_id, iteration, "chef", "turn_start", {"iteration": iteration})
            await asyncio.to_thread(_snapshot, task_id, folder, iteration)  # #18 time-travel

            team = await db_list_agents(task_id)
            files = _list_files(folder)
            ctx = ("Equipe actuelle : "
                   + (", ".join(a["name"] + " (" + a["role"] + ")" for a in team) or "(vide)")
                   + NL + "Fichiers du dossier : " + (", ".join(files) or "(aucun)")
                   + NL + "Prochaine action ? Reponds en JSON.")
            chef_messages.append({"role": "user", "content": ctx})
            await _log_prompt(task_id, iteration, "chef", chef_system, chef_messages)
            raw = await call_model("claude", chef_model, chef_system, chef_messages,
                                   on_notice=notice, task_id=task_id)
            chef_messages.append({"role": "assistant", "content": raw})

            decision = _extract_json(raw) or {}
            action = decision.get("action", "")
            thought = decision.get("thought", "")
            await emit(task_id, iteration, "chef", "chef",
                       {"decision": action or "?", "thought": thought,
                        "instruction": decision.get("instruction")})

            if action == "finish" or decision.get("done"):
                final = decision.get("final") or thought or "Objectif traite."
                if company:
                    sanity = await asyncio.to_thread(_company_sanity, folder)
                    if sanity:
                        await asyncio.to_thread(_write_manifest, folder, sanity)
                        note = ("Livraison : " + str(len(sanity["files"])) + " fichier(s) dans delivery/ "
                                "(manifest : delivery/_MANIFEST.md).")
                        if sanity["missing"]:
                            note += (" ATTENTION : reference(s) ABSENTE(S) du disque -> "
                                     + ", ".join(sanity["missing"][:15]))
                        await emit(task_id, iteration, "systeme", "info", {"msg": note})
                        final = final + NL + note
                # Auto-resume : tirer 1-3 lecons reutilisables et les ranger dans la memoire partagee
                try:
                    import memory as _mem
                    msgs = await db_messages_after(task_id, 0)
                    blob = NL.join(
                        (m["agent"] + ": " + (json.loads(m["content"]).get("content")
                                              or json.loads(m["content"]).get("summary")
                                              or json.loads(m["content"]).get("instruction")
                                              or "")) if m["content"] and m["content"].startswith("{") else ""
                        for m in msgs if m["kind"] in ("agent_message", "chef", "done")
                    )
                    n_saved = await _mem.summarize_and_save(task_id, objective, blob, _client)
                    if n_saved:
                        await emit(task_id, iteration, "systeme", "info",
                                   {"msg": str(n_saved) + " lecon(s) memorisee(s) pour les futures taches."})
                except Exception as e:
                    log.info("[memoire] auto-resume KO : %s", e)
                await emit(task_id, iteration, "chef", "done", {"summary": final})
                await db_update_task(task_id, status="done")
                return

            if action == "create_agent":
                if len(team) >= max_agents:
                    chef_messages.append({"role": "user", "content":
                                          "Refuse : plafond de " + str(max_agents)
                                          + " agents atteint. Delegue a un agent existant ou termine."})
                    continue
                name = (decision.get("name") or ("agent" + str(len(team) + 1))).strip()
                role = decision.get("role", "")
                provider = decision.get("provider", "claude")
                if provider not in ("claude", "gemini", "openai"):
                    provider = "claude"
                model = decision.get("model") or _default_model(provider)
                if await db_get_agent(task_id, name):
                    chef_messages.append({"role": "user", "content":
                                          "Un agent nomme '" + name + "' existe deja. Choisis un autre nom ou delegue-lui."})
                    continue
                await db_add_agent(task_id, name, role, provider, model, "chef")
                await emit(task_id, iteration, "chef", "agent_created",
                           {"name": name, "role": role, "model": provider + ":" + model})
                chef_messages.append({"role": "user", "content": "Agent '" + name + "' cree."})
                continue

            if action == "assign_task":
                target = (decision.get("agent") or "").strip()
                instruction = decision.get("instruction", "")
                agent = await db_get_agent(task_id, target)
                if not agent:
                    chef_messages.append({"role": "user", "content":
                                          "Agent '" + target + "' introuvable. Cree-le d'abord (create_agent)."})
                    continue
                report = await _run_worker(task_id, iteration, agent, instruction,
                                           folder, web_enabled, worker_histories, notice, image)
                chef_messages.append({"role": "user", "content":
                                      "Rapport de " + target + " : " + report[:2000]})
                continue

            if action == "assign_managed_agent":
                agent_id = (decision.get("agent_id") or "").strip()
                instruction = decision.get("instruction", "") or ""
                if not agent_id or not agent_id.startswith("agent_"):
                    chef_messages.append({"role": "user", "content":
                                          "assign_managed_agent : champ 'agent_id' manquant ou invalide (doit etre 'agent_...')."})
                    continue
                label = (decision.get("name") or agent_id)
                await emit(task_id, iteration, label, "agent_message",
                           {"kind": "thought",
                            "content": "Delegation a l'agent gere Anthropic " + agent_id + "..."})

                async def _on_text(t, _l=label, _it=iteration):
                    await emit(task_id, _it, _l, "agent_message", {"kind": "result", "content": t})

                try:
                    res = await managed_agents.run_session(agent_id, instruction, on_text=_on_text)
                    text = res.get("text") or "(reponse vide)"
                    chef_messages.append({"role": "user", "content":
                                          "Rapport de l'agent gere " + agent_id + " (statut="
                                          + str(res.get("status")) + ", stop_reason="
                                          + str(res.get("stop_reason")) + ") :" + NL + text[:3000]})
                except Exception as e:
                    log.warning("[managed-agents] erreur session %s : %s", agent_id, e)
                    await notice("Agent gere " + agent_id + " : erreur " + str(e)[:200])
                    chef_messages.append({"role": "user", "content":
                                          "assign_managed_agent a echoue (" + str(e)[:200]
                                          + "). Essaie un autre agent ou une autre action."})
                continue

            if action == "parallel_assign":
                assigns = decision.get("assignments", []) or []
                # Compter les doublons : un agent assigne plusieurs fois en parallele
                # doit recevoir un historique CLONE par branche (evite l'entrelacement).
                counts = {}
                for a in assigns:
                    nm = (a.get("agent") or "").strip()
                    counts[nm] = counts.get(nm, 0) + 1
                coros, names = [], []
                for a in assigns:
                    nm = (a.get("agent") or "").strip()
                    ag = await db_get_agent(task_id, nm)
                    if ag:
                        hist = list(worker_histories.setdefault(nm, [])) if counts[nm] > 1 else None
                        coros.append(_run_worker(task_id, iteration, ag, a.get("instruction", ""),
                                                 folder, web_enabled, worker_histories, notice, image,
                                                 custom_hist=hist))
                        names.append(ag["name"])
                if not coros:
                    chef_messages.append({"role": "user", "content":
                                          "parallel_assign : aucun agent valide. Cree-les d'abord."})
                    continue
                reports = await asyncio.gather(*coros)
                summary = NL.join(names[i] + " : " + reports[i][:600] for i in range(len(names)))
                chef_messages.append({"role": "user", "content":
                                      "Rapports paralleles :" + NL + summary})
                continue

            if action == "challenge":
                instruction = decision.get("instruction", "")
                if not instruction:
                    chef_messages.append({"role": "user", "content":
                                          "challenge requiert un champ 'instruction'."})
                    continue
                report = await _run_challenge(
                    task_id, iteration, instruction, folder, web_enabled,
                    decision.get("rounds", 2), worker_histories, notice,
                    decision.get("producer_provider", "claude"),
                    decision.get("critic_providers"), image)
                chef_messages.append({"role": "user", "content":
                                      "Resultat du challenge : " + report[:2000]})
                continue

            chef_messages.append({"role": "user", "content":
                                  "Action non reconnue. Utilise create_agent, assign_task, "
                                  "parallel_assign, challenge, assign_managed_agent ou finish."})

        if not await _is_stopped(task_id):
            await emit(task_id, iteration, "chef", "done",
                       {"summary": "Limite de " + str(max_iter) + " iterations atteinte."})
            await db_update_task(task_id, status="failed")
    except asyncio.CancelledError:
        await db_update_task(task_id, status="stopped")
        raise
    except Exception as e:
        log.exception("Task %s crashed", task_id)
        await emit(task_id, 0, "systeme", "error", {"msg": str(e)[:300]})
        await db_update_task(task_id, status="failed")
    finally:
        await _stop_mcp(task_id)
        running_tasks.pop(task_id, None)


# ── API ──────────────────────────────────────────────────────────────────────
class AgentSeed(BaseModel):
    name: str
    role: str = ""
    model: str = "claude"  # "claude" ou "gemini"


class TaskCreate(BaseModel):
    objective: str
    chef_model: str = DEFAULT_CLAUDE
    agents: list[AgentSeed] = []
    web_enabled: bool = True
    max_iterations: int = 15
    max_agents: int = 4
    image: Optional[str] = None  # data URL (data:image/...;base64,...) d'une maquette
    company_mode: bool = False   # mode entreprise tech (audit d'un depot externe en lecture seule)
    target_path: Optional[str] = None  # chemin local du depot a auditer (lecture seule)
    target_repo_url: Optional[str] = None  # URL GitHub a cloner localement (lecture seule)
    github_token: Optional[str] = None  # token pour depot prive (utilise pour le clone, jamais stocke)
    max_cost_usd: float = 0  # plafond de cout par tache en USD (0 = pas de plafond)


router = APIRouter()


@router.get("/workspace")
async def workspace_page():
    return FileResponse(STATIC / "workspace.html", headers={"Cache-Control": "no-store"})


@router.get("/sw.js")
async def service_worker():
    # Servi a la racine pour que la portee du service worker couvre tout le site ("/").
    return FileResponse(STATIC / "sw.js", media_type="application/javascript",
                        headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})


@router.post("/api/tasks")
async def create_task(body: TaskCreate):
    objective = body.objective.strip()
    if not objective:
        raise HTTPException(400, "Objectif manquant")
    company = bool(body.company_mode)
    iter_ceiling = COMPANY_MAX_ITER if company else MAX_ITER_CEILING
    agents_ceiling = COMPANY_MAX_AGENTS if company else MAX_AGENTS_CEILING
    max_iter = max(1, min(body.max_iterations, iter_ceiling))
    max_agents = max(1, min(body.max_agents, agents_ceiling))
    target_path = None
    if company:
        if body.target_repo_url:
            try:
                target_path = await _clone_repo(body.target_repo_url, body.github_token)
            except Exception as e:
                raise HTTPException(400, "Connexion au depot GitHub impossible : " + str(e)[:300])
        elif body.target_path and Path(body.target_path).exists():
            target_path = str(Path(body.target_path).resolve())
        else:
            raise HTTPException(400, "Mode entreprise : fournis une URL GitHub (target_repo_url) "
                                     "OU un chemin local existant (target_path).")
    folder = str(PROJECTS / ("task_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                             + "_" + uuid.uuid4().hex[:6]))
    Path(folder).mkdir(parents=True, exist_ok=True)
    if company:
        (Path(folder) / "delivery").mkdir(exist_ok=True)  # tout ce que produit la boite va ici
    task_id = await db_create_task(objective, folder, max_iter, max_agents,
                                   body.web_enabled, body.chef_model or DEFAULT_CLAUDE,
                                   company_mode=company, target_path=target_path,
                                   max_cost_usd=max(0.0, float(body.max_cost_usd or 0)))
    # Amorcage optionnel d'agents par l'utilisateur
    for a in body.agents:
        name = (a.name or "").strip()
        if not name:
            continue
        provider = a.model if a.model in ("claude", "gemini", "openai") else "claude"
        await db_add_agent(task_id, name, a.role, provider, _default_model(provider), "user")
    if body.image:
        parsed = _parse_data_url(body.image)
        if parsed:
            ext = _MEDIA_EXT.get(parsed["media_type"], "png")
            ip = Path(folder) / ("design_reference." + ext)
            try:
                ip.write_bytes(base64.b64decode(parsed["data"]))
                await db_update_task(task_id, image_path=str(ip))
            except Exception as e:
                log.warning("image invalide: %s", e)
    return {"id": task_id, "folder": folder}


def _spawn_loop(task_id, resume=False):
    pause = asyncio.Event()
    pause.set()  # set = en marche ; clear = en pause
    running_tasks[task_id] = {"pause": pause, "step": False, "inbox": [], "mcp": {}}
    running_tasks[task_id]["task"] = asyncio.create_task(_run_task(task_id, resume=resume))


@router.post("/api/tasks/{task_id}/start")
async def start_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    if task_id in running_tasks:
        return {"status": "already_running"}
    _spawn_loop(task_id, resume=False)
    return {"status": "running"}


class RestartBody(BaseModel):
    max_cost_usd: Optional[float] = None  # nouveau plafond de cout (None = inchange)


@router.post("/api/tasks/{task_id}/restart")
async def restart_task(task_id: int, body: Optional[RestartBody] = None):
    """Reprend une tache terminee/arretee/echouee la ou elle s'etait arretee (contexte reconstruit)."""
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    if task_id in running_tasks:
        return {"status": "already_running"}
    if t["status"] not in ("failed", "stopped", "done", "idle"):
        raise HTTPException(400, "La tache n'est pas dans un etat reprenable (" + str(t["status"]) + ").")
    if body and body.max_cost_usd is not None:
        await db_update_task(task_id, max_cost_usd=max(0.0, float(body.max_cost_usd)))
    _spawn_loop(task_id, resume=True)
    return {"status": "running", "resumed": True}


@router.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: int):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    ctrl["pause"].clear()
    await db_update_task(task_id, status="paused")
    await emit(task_id, 0, "systeme", "paused", {})
    return {"status": "paused"}


@router.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: int):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    ctrl["pause"].set()
    await db_update_task(task_id, status="running")
    await emit(task_id, 0, "systeme", "resumed", {})
    return {"status": "running"}


@router.post("/api/tasks/{task_id}/step")
async def step_task(task_id: int):
    """Avance la tache si elle est en pause : on debloque brievement la boucle."""
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    pause = ctrl["pause"]
    pause.set()
    await asyncio.sleep(0.05)
    pause.clear()
    await db_update_task(task_id, status="paused")
    return {"status": "stepped"}


@router.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: int):
    await db_update_task(task_id, status="stopped")
    ctrl = running_tasks.get(task_id)
    if ctrl:
        ctrl["pause"].set()  # debloque la boucle pour qu'elle constate l'arret
    await _stop_launched(task_id)
    await _stop_mcp(task_id)
    await emit(task_id, 0, "systeme", "stopped", {})
    return {"status": "stopped"}


class TaskMessage(BaseModel):
    text: str


@router.post("/api/tasks/{task_id}/message")
async def task_message(task_id: int, body: TaskMessage):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active (terminee ou arretee)")
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(400, "Message vide")
    ctrl.setdefault("inbox", []).append(text)
    t = await db_get_task(task_id)
    await emit(task_id, t["iteration"] if t else 0, "utilisateur", "user_message", {"content": text})
    return {"queued": True}


# ── Lancement de l'app produite ──────────────────────────────────────────────
WEB_ENTRYPOINTS = ["app.py", "main.py", "server.py", "run.py"]


async def _stop_launched(task_id):
    info = launched_apps.pop(task_id, None)
    if info and info.get("proc") and info["proc"].returncode is None:
        try:
            info["proc"].terminate()
        except Exception:
            pass


async def _drain_proc(task_id, proc):
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            info = launched_apps.get(task_id)
            if info is None:
                break
            info["output"].append(line.decode("utf-8", "replace"))
            info["output"] = info["output"][-200:]
    except Exception:
        pass


async def _launch_python_app(task_id, base, name):
    await _stop_launched(task_id)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, name, cwd=str(base),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    launched_apps[task_id] = {"proc": proc, "url": None, "output": []}
    url, lines = None, []
    for _ in range(15):
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=0.5)
        except asyncio.TimeoutError:
            break
        if not line:
            break
        text = line.decode("utf-8", "replace")
        lines.append(text)
        m = re.search(r"https?://[^\s'\"]+", text)
        if m:
            url = m.group(0)
            break
        m2 = re.search(r"port[^\d]{0,6}(\d{2,5})", text, re.IGNORECASE)
        if m2:
            url = "http://localhost:" + m2.group(1)
            break
    launched_apps[task_id]["output"] = lines
    launched_apps[task_id]["url"] = url
    asyncio.create_task(_drain_proc(task_id, proc))
    return {"type": "python", "entry": name, "url": url,
            "running": proc.returncode is None, "output": "".join(lines)[-1500:]}


@router.post("/api/tasks/{task_id}/launch")
async def launch_app(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    base = Path(t["folder"])
    for name in WEB_ENTRYPOINTS:
        if (base / name).exists():
            return await _launch_python_app(task_id, base, name)
    htmls = [f for f in _list_files(t["folder"]) if f.endswith(".html")]
    if htmls:
        target = "index.html" if "index.html" in htmls else htmls[0]
        return {"type": "static", "url": "/api/tasks/" + str(task_id) + "/app/" + target}
    return {"type": "none", "message": "Aucun point d'entree lancable (app.py/main.py/index.html)."}


@router.post("/api/tasks/{task_id}/launch/stop")
async def launch_stop(task_id: int):
    await _stop_launched(task_id)
    return {"stopped": True}


@router.get("/api/tasks/{task_id}/app/{path:path}")
async def serve_app(task_id: int, path: str):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    try:
        p = _safe_path(t["folder"], path)
    except ValueError:
        raise HTTPException(400, "Chemin invalide")
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "Fichier introuvable")
    return FileResponse(p)


# ── Edition de fichier depuis l'UI (#13 Monaco) ──────────────────────────────
class SaveFile(BaseModel):
    path: str
    content: str


@router.post("/api/tasks/{task_id}/file")
async def save_file(task_id: int, body: SaveFile):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    try:
        p = _safe_path(t["folder"], body.path)
    except ValueError:
        raise HTTPException(400, "Chemin invalide")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body.content, encoding="utf-8")
    return {"saved": body.path}


# ── Export du projet produit (#14) ───────────────────────────────────────────
def _detect_entry(base: Path):
    for name in WEB_ENTRYPOINTS:
        if (base / name).exists():
            return ("python", name)
    if (base / "index.html").exists():
        return ("static", "index.html")
    return (None, None)


def _start_scripts(kind, entry):
    if kind == "python":
        bat = ("@echo off\r\npython -m venv venv\r\ncall venv\\Scripts\\activate\r\n"
               "if exist requirements.txt pip install -r requirements.txt\r\npython " + entry + "\r\npause\r\n")
        sh = ("#!/usr/bin/env bash\nset -e\npython3 -m venv venv\nsource venv/bin/activate\n"
              "[ -f requirements.txt ] && pip install -r requirements.txt\npython3 " + entry + "\n")
    elif kind == "static":
        bat = "@echo off\r\nstart \"\" \"" + entry + "\"\r\n"
        sh = "#!/usr/bin/env bash\nopen \"" + entry + "\" 2>/dev/null || xdg-open \"" + entry + "\"\n"
    else:
        bat = "@echo off\r\necho Aucun point d'entree detecte.\r\npause\r\n"
        sh = "#!/usr/bin/env bash\necho 'Aucun point d entree detecte.'\n"
    return bat, sh


@router.get("/api/tasks/{task_id}/export")
async def export_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    base = Path(t["folder"])
    kind, entry = _detect_entry(base)
    bat, sh = _start_scripts(kind, entry)
    run_help = ("- Windows : double-cliquez `start.bat`" + NL
                + "- Mac/Linux : `bash start.sh`" + NL)
    readme = ("# " + (t["objective"][:80] or "Projet") + NL + NL
              + "Projet genere par l'Orchestrateur multi-agents." + NL + NL
              + "## Objectif" + NL + NL + (t["objective"] or "") + NL + NL
              + "## Lancer" + NL + NL + run_help)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in base.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(base).as_posix())
        if not (base / "start.bat").exists():
            z.writestr("start.bat", bat)
        if not (base / "start.sh").exists():
            z.writestr("start.sh", sh)
        if not (base / "README.md").exists():
            z.writestr("README.md", readme)
    return Response(
        content=buf.getvalue(), media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=task_" + str(task_id) + ".zip"})


# ── CI locale : build & test + historique (#16) ──────────────────────────────
def _venv_python(venv_dir: Path) -> Path:
    sub = "Scripts" if os.name == "nt" else "bin"
    return venv_dir / sub / ("python.exe" if os.name == "nt" else "python")


async def _proc_run(args, cwd, timeout):
    """Lance une commande, renvoie (code, sortie tronquee)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except FileNotFoundError:
        return 1, "Programme introuvable: " + str(args[0])
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, out.decode("utf-8", "replace")[:3000]
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return 1, "TIMEOUT apres " + str(timeout) + "s."


def _http_ping(url, timeout=5):
    import urllib.request
    import urllib.error
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return getattr(resp, "status", resp.getcode())
    except urllib.error.HTTPError as e:
        return e.code  # 3xx/4xx -> le serveur repond quand meme
    except Exception as e:
        return "erreur: " + str(e)[:120]


async def _build_ping(folder, py_exe):
    base = Path(folder)
    entry = next((n for n in WEB_ENTRYPOINTS if (base / n).exists()), None)
    if not entry:
        return {"name": "ping HTTP /", "ok": True, "log": "Pas d'app web Python (ignore)."}
    # PYTHONUNBUFFERED : stdout non bufferise -> on lit l'URL de demarrage en temps reel.
    # FLASK_DEBUG=0 : decourage le reloader. On NE met PAS WERKZEUG_RUN_MAIN (sinon Werkzeug
    # cherche WERKZEUG_SERVER_FD -> KeyError fatale au demarrage).
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["FLASK_DEBUG"] = "0"
    proc = None
    url = None
    try:
        proc = await asyncio.create_subprocess_exec(
            py_exe, entry, cwd=str(base), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        for _ in range(16):
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=0.5)
            except asyncio.TimeoutError:
                break
            if not line:
                break
            m = re.search(r"https?://[^\s'\"]+", line.decode("utf-8", "replace"))
            if m:
                url = m.group(0).replace("0.0.0.0", "localhost")
                break
        if not url:
            return {"name": "ping HTTP /", "ok": False, "log": "URL non detectee au demarrage."}
        status = await asyncio.to_thread(_http_ping, url)
        ok = isinstance(status, int) and status < 500  # serveur qui repond = succes
        return {"name": "ping HTTP /", "ok": ok, "log": url + " -> " + str(status)}
    finally:
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass


async def _run_build(task_id, folder):
    import py_compile
    base = Path(folder)
    steps = []

    # 1. Verification syntaxique globale
    py_files = [p for p in base.rglob("*.py")
                if not any(x in p.parts for x in (".ci", "venv", ".venv", "__pycache__"))]
    errs = []
    for p in py_files:
        try:
            await asyncio.to_thread(py_compile.compile, str(p), None, str(p), True)
        except py_compile.PyCompileError as e:
            errs.append(str(e)[:300])
    steps.append({"name": "syntaxe", "ok": not errs,
                  "log": (str(len(py_files)) + " fichier(s) OK") if not errs else NL.join(errs)})

    # 2. Installation propre dans un venv dedie (si requirements.txt)
    py_exe = sys.executable
    if (base / "requirements.txt").exists():
        venv_dir = PROJECTS / ".ci" / str(task_id) / "venv"
        venv_dir.parent.mkdir(parents=True, exist_ok=True)
        code, vlog = await _proc_run([sys.executable, "-m", "venv", str(venv_dir)], base, 120)
        cand = _venv_python(venv_dir)
        if code == 0 and cand.exists():
            py_exe = str(cand)
            code, ilog = await _proc_run([py_exe, "-m", "pip", "install", "-q", "-r", "requirements.txt"], base, 300)
            steps.append({"name": "install (venv)", "ok": code == 0, "log": ilog or "OK"})
        else:
            steps.append({"name": "install (venv)", "ok": False, "log": "Creation du venv echouee: " + vlog})
    else:
        steps.append({"name": "install (venv)", "ok": True, "log": "Pas de requirements.txt (ignore)."})

    # 3. Tests unitaires (pytest)
    code, tlog = await _proc_run([py_exe, "-m", "pytest", "-q"], base, 180)
    # pytest renvoie 5 quand aucun test collecte -> on ne compte pas ca comme un echec
    steps.append({"name": "pytest", "ok": code in (0, 5), "log": tlog or "(aucune sortie)"})

    # 4. Ping HTTP /
    steps.append(await _build_ping(folder, py_exe))

    status = "success" if all(s["ok"] for s in steps) else "failed"
    report = json.dumps(steps, ensure_ascii=False)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("INSERT INTO builds (task_id, status, report, created_at) VALUES (?,?,?,?)",
                         (task_id, status, report, datetime.utcnow().isoformat()))
        await db.commit()
    return {"status": status, "steps": steps}


@router.post("/api/tasks/{task_id}/build")
async def build_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    return await _run_build(task_id, t["folder"])


@router.get("/api/tasks/{task_id}/builds")
async def list_builds(task_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, status, report, created_at FROM builds WHERE task_id=? ORDER BY id DESC LIMIT 20",
            (task_id,)) as cur:
            rows = await cur.fetchall()
    out = []
    for r in rows:
        try:
            steps = json.loads(r["report"])
        except Exception:
            steps = []
        out.append({"id": r["id"], "status": r["status"], "steps": steps, "created_at": r["created_at"]})
    return {"builds": out}


@router.get("/api/tasks")
async def list_tasks():
    return await db_list_tasks()


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    t["agents"] = await db_list_agents(task_id)
    t["active"] = task_id in running_tasks
    return t


@router.delete("/api/tasks/{task_id}")
async def delete_task(task_id: int):
    await stop_task(task_id)
    t = await db_get_task(task_id)
    if t:
        shutil.rmtree(t["folder"], ignore_errors=True)
    # Nettoyer aussi le venv de CI et les snapshots (sinon orphelins, ~50 Mo+).
    shutil.rmtree(PROJECTS / ".ci" / str(task_id), ignore_errors=True)
    shutil.rmtree(PROJECTS / ".snapshots" / str(task_id), ignore_errors=True)
    await db_delete_task(task_id)
    return {"deleted": True}


@router.get("/api/tasks/{task_id}/prompts")
async def get_prompts(task_id: int, iteration: int = Query(None), agent: str = Query(None)):
    q = "SELECT iteration, agent, payload FROM prompts WHERE task_id=?"
    params = [task_id]
    if iteration is not None:
        q += " AND iteration=?"
        params.append(iteration)
    if agent:
        q += " AND agent=?"
        params.append(agent)
    q += " ORDER BY id DESC LIMIT 1"
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Aucun prompt enregistre")
    try:
        payload = json.loads(row["payload"])
    except Exception:
        payload = {"raw": row["payload"]}
    return {"iteration": row["iteration"], "agent": row["agent"], "prompt": payload}


class RevertBody(BaseModel):
    iteration: int


_REVERT_KEEP = {".venv", "venv", "node_modules"}  # deps lourdes, non versionnees -> conservees


@router.post("/api/tasks/{task_id}/revert")
async def revert_task(task_id: int, body: RevertBody):
    if task_id in running_tasks:
        raise HTTPException(400, "Arrete la tache avant de revenir en arriere.")
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    # Liberer les verrous fichiers (app lancee, serveurs MCP) avant de restaurer.
    await _stop_launched(task_id)
    await _stop_mcp(task_id)
    n = body.iteration
    snap = PROJECTS / ".snapshots" / str(task_id) / ("iter_" + str(n))
    if not snap.exists():
        raise HTTPException(400, "Snapshot du tour " + str(n) + " inexistant.")
    folder = Path(t["folder"]).resolve()
    base = PROJECTS.resolve()
    if folder != base and base not in folder.parents:
        raise HTTPException(400, "Dossier de tache invalide.")

    def _restore():
        for child in folder.iterdir():
            if child.name in _REVERT_KEEP:
                continue  # conserver les dependances installees
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except Exception:
                    pass
        for item in snap.iterdir():
            dst = folder / item.name
            if item.is_dir():
                shutil.copytree(item, dst, ignore=_SNAPSHOT_IGNORE)
            else:
                shutil.copy2(item, dst)
        return True

    restored = await asyncio.to_thread(_restore)
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("DELETE FROM messages WHERE task_id=? AND iteration>?", (task_id, n))
        await db.execute("DELETE FROM prompts WHERE task_id=? AND iteration>?", (task_id, n))
        await db.commit()
    await db_update_task(task_id, iteration=n, status="stopped")
    return {"reverted_to": n, "files_restored": restored}


@router.get("/api/tasks/{task_id}/files")
async def task_files(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    local = _list_files(t["folder"])
    if t.get("company_mode") and t.get("target_path"):
        # Vue virtuelle unifiee : fichiers du depot cible + fichiers locaux (livraison).
        merged = set(local) | set(_list_target_files(t["target_path"]))
        return {"files": sorted(merged)}
    return {"files": local}


@router.get("/api/tasks/{task_id}/file")
async def task_file(task_id: int, path: str = Query(...)):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    try:
        p = _safe_path(t["folder"], path)
    except ValueError:
        raise HTTPException(400, "Chemin invalide")
    if p.exists() and p.is_file():
        return {"path": path, "content": p.read_text(encoding="utf-8", errors="replace")[:50000]}
    # Fallback lecture seule sur le depot cible (mode entreprise).
    if t.get("company_mode") and t.get("target_path"):
        tp = _target_file(t["target_path"], path)
        if tp:
            return {"path": path, "source": "target",
                    "content": tp.read_text(encoding="utf-8", errors="replace")[:50000]}
    raise HTTPException(404, "Fichier introuvable")


@router.get("/api/tasks/{task_id}/stream")
async def stream_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")

    async def gen() -> AsyncGenerator[str, None]:
        last_id = 0
        while True:
            rows = await db_messages_after(task_id, last_id)
            for r in rows:
                last_id = r["id"]
                payload = {"type": r["kind"], "agent": r["agent"],
                           "iteration": r["iteration"], "id": r["id"]}
                try:
                    payload.update(json.loads(r["content"]))
                except Exception:
                    payload["content"] = r["content"]
                yield sse(payload)
            cur = await db_get_task(task_id)
            if cur and cur["status"] in ("done", "stopped", "failed"):
                tail = await db_messages_after(task_id, last_id)
                if not tail:
                    yield sse({"type": "end", "status": cur["status"]})
                    return
            await asyncio.sleep(0.5)

    return StreamingResponse(
        gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
