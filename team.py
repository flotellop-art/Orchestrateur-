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
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

import managed_agents  # pont vers les Agents geres Anthropic (delegation depuis le chef)
import app_settings    # jeton GitHub saisi dans l'UI (repli sur GITHUB_TOKEN du .env)

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
                    "ALTER TABLE tasks ADD COLUMN max_cost_usd REAL DEFAULT 0",
                    "ALTER TABLE tasks ADD COLUMN target_repo_url TEXT",
                    "ALTER TABLE tasks ADD COLUMN github_token TEXT",
                    # Registre local -> agents geres : id de l'agent Anthropic cree depuis cet agent local.
                    "ALTER TABLE task_agents ADD COLUMN managed_agent_id TEXT"):
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
        # Missions PROGRAMMEES : lancees automatiquement a heure fixe (une fois,
        # tous les jours, ou a intervalle regulier). `config` = JSON des parametres
        # de la tache a creer (modele, web, plafonds, depot...). Le jeton GitHub
        # eventuel y est stocke mais JAMAIS renvoye par l'API (voir _public_schedule).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_missions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                objective         TEXT NOT NULL,
                config            TEXT NOT NULL DEFAULT '{}',
                schedule_kind     TEXT NOT NULL DEFAULT 'once',
                run_at            TEXT,
                daily_time        TEXT,
                interval_minutes  INTEGER,
                tz_offset_minutes INTEGER DEFAULT 0,
                enabled           INTEGER DEFAULT 1,
                next_run          TEXT,
                last_run          TEXT,
                last_task_id      INTEGER,
                last_status       TEXT,
                runs_count        INTEGER DEFAULT 0,
                created_at        TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sched_due "
                         "ON scheduled_missions(enabled, next_run)")
        # Les taches en cours ne survivent pas a un redemarrage du serveur.
        await db.execute("UPDATE tasks SET status='stopped' WHERE status IN ('running','paused')")
        await db.commit()


async def db_create_task(objective, folder, max_iterations, max_agents, web_enabled, chef_model,
                         company_mode=False, target_path=None, max_cost_usd=0.0,
                         target_repo_url=None, github_token=None):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "INSERT INTO tasks (objective,folder,status,max_iterations,max_agents,web_enabled,chef_model,"
            "company_mode,target_path,max_cost_usd,target_repo_url,github_token,created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (objective, folder, "idle", max_iterations, max_agents,
             1 if web_enabled else 0, chef_model,
             1 if company_mode else 0, target_path, max_cost_usd, target_repo_url, github_token,
             datetime.utcnow().isoformat()),
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
    "total_cost_usd", "max_cost_usd", "target_repo_url", "github_token",
})


def _public_task(t: Optional[dict]) -> Optional[dict]:
    """Version d'une tache SANS secret : le jeton GitHub du projet n'est JAMAIS
    renvoye par l'API (remplace par un booleen has_github_token)."""
    if not t:
        return t
    t = dict(t)
    t["has_github_token"] = bool((t.pop("github_token", None) or "").strip())
    return t

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


# ── Missions programmees : calcul d'horaire + CRUD ───────────────────────────
SCHEDULE_KINDS = ("once", "daily", "interval")


def _parse_local_dt(s: str) -> Optional[datetime]:
    """Parse une date-heure LOCALE 'YYYY-MM-DDTHH:MM[:SS]' (sans fuseau)."""
    if not s:
        return None
    s = s.strip().replace(" ", "T").rstrip("Z")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M")
        except Exception:
            return None


def _parse_hhmm(s: str) -> Optional[tuple[int, int]]:
    try:
        hh, mm = (s or "").strip().split(":")[:2]
        h, m = int(hh), int(mm)
        if 0 <= h < 24 and 0 <= m < 60:
            return h, m
    except Exception:
        pass
    return None


def _compute_next_run(kind: str, *, run_at=None, daily_time=None, interval_minutes=None,
                      tz_offset_minutes: int = 0, after: Optional[datetime] = None) -> Optional[str]:
    """Prochain declenchement en UTC (ISO) selon le type de planification.

    `tz_offset_minutes` = minutes a AJOUTER a l'UTC pour obtenir l'heure locale de
    l'utilisateur (soit -getTimezoneOffset() cote navigateur). On raisonne en heure
    locale puis on reconvertit en UTC pour le stockage.
    """
    base = after or datetime.utcnow()
    off = timedelta(minutes=int(tz_offset_minutes or 0))
    if kind == "once":
        dt_local = _parse_local_dt(run_at)
        if dt_local is None:
            return None
        return (dt_local - off).isoformat()  # local -> utc
    if kind == "interval":
        mins = int(interval_minutes or 0)
        if mins < 1:
            return None
        return (base + timedelta(minutes=mins)).isoformat()
    if kind == "daily":
        hm = _parse_hhmm(daily_time)
        if not hm:
            return None
        local_now = base + off
        target = local_now.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
        if target <= local_now:
            target += timedelta(days=1)
        return (target - off).isoformat()  # local -> utc
    return None


def _public_schedule(s: Optional[dict]) -> Optional[dict]:
    """Version sans secret : le jeton GitHub eventuel du config n'est jamais renvoye."""
    if not s:
        return s
    s = dict(s)
    try:
        cfg = json.loads(s.get("config") or "{}")
    except Exception:
        cfg = {}
    has_tok = bool((cfg.pop("github_token", None) or "").strip())
    s["config"] = cfg
    s["has_github_token"] = has_tok
    return s


async def db_create_schedule(objective, config, schedule_kind, next_run, *,
                             run_at=None, daily_time=None, interval_minutes=None,
                             tz_offset_minutes=0, enabled=True):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "INSERT INTO scheduled_missions (objective,config,schedule_kind,run_at,daily_time,"
            "interval_minutes,tz_offset_minutes,enabled,next_run,runs_count,created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,0,?)",
            (objective, json.dumps(config, ensure_ascii=False), schedule_kind, run_at, daily_time,
             interval_minutes, int(tz_offset_minutes or 0), 1 if enabled else 0, next_run,
             datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


async def db_list_schedules():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM scheduled_missions ORDER BY enabled DESC, next_run ASC, id DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_get_schedule(sched_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM scheduled_missions WHERE id=?", (sched_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


_SCHED_COLUMNS_ALLOWED = frozenset({
    "objective", "config", "schedule_kind", "run_at", "daily_time", "interval_minutes",
    "tz_offset_minutes", "enabled", "next_run", "last_run", "last_task_id", "last_status",
    "runs_count",
})


async def db_update_schedule(sched_id, **kwargs):
    invalid = set(kwargs) - _SCHED_COLUMNS_ALLOWED
    if invalid:
        raise ValueError("Colonnes non autorisees pour UPDATE scheduled_missions : "
                         + ", ".join(sorted(invalid)))
    if not kwargs:
        return
    sets = ", ".join(k + "=?" for k in kwargs)
    vals = list(kwargs.values()) + [sched_id]
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("UPDATE scheduled_missions SET " + sets + " WHERE id=?", vals)
        await db.commit()


async def db_delete_schedule(sched_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("DELETE FROM scheduled_missions WHERE id=?", (sched_id,))
        await db.commit()


async def db_due_schedules(now_iso: str):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM scheduled_missions WHERE enabled=1 AND next_run IS NOT NULL "
            "AND next_run<=? ORDER BY next_run ASC", (now_iso,)) as cur:
            return [dict(r) for r in await cur.fetchall()]


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


async def db_set_agent_managed(task_id, name, managed_agent_id):
    """Lie un agent local au vrai agent gere Anthropic cree a partir de lui."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("UPDATE task_agents SET managed_agent_id=? WHERE task_id=? AND name=?",
                         (managed_agent_id, task_id, name))
        await db.commit()


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


# ── Tool use NATIF + pensee adaptative (chef + workers Claude) ────────────────
def _usage_from_response(resp, model):
    # Construit le dict usage a partir d'une reponse Claude (memes champs que _claude_request).
    u = resp.usage
    return {"model": model,
            "input": ((getattr(u, "input_tokens", 0) or 0)
                      + (getattr(u, "cache_creation_input_tokens", 0) or 0)
                      + (getattr(u, "cache_read_input_tokens", 0) or 0)),
            "output": getattr(u, "output_tokens", 0) or 0}


async def _claude_request_native(model, system, messages, tools, image=None):
    # Appel non-streaming avec outils natifs + pensee adaptative (resume).
    kwargs = dict(
        model=model,
        max_tokens=MAX_TOKENS,
        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        messages=_attach_image_claude(messages, image),
        tools=tools,
        thinking={"type": "adaptive", "display": "summarized"},
    )
    try:
        resp = await _client.messages.create(**kwargs)
    except anthropic.BadRequestError as e:
        # Repli gracieux : les anciens modeles ne supportent pas la pensee adaptative.
        msg = str(getattr(e, "message", "") or e).lower()
        if "thinking" in msg or "adaptive" in msg:
            kwargs.pop("thinking", None)
            resp = await _client.messages.create(**kwargs)
        else:
            raise
    return resp, _usage_from_response(resp, model)


async def _call_claude_native(model, system, messages, tools, on_notice=None, image=None):
    # Mirroir de _call_claude pour le mode natif : repli sur STABLE_CLAUDE en cas d'erreur.
    # Rejouer des blocs de pensee produits par un autre modele Claude est accepte par l'API.
    target = model or DEFAULT_CLAUDE
    try:
        return await _claude_request_native(target, system, messages, tools, image)
    except Exception as e:
        if target == STABLE_CLAUDE:
            raise  # le modele de repli a aussi echoue : on remonte l'erreur
        log.warning("[modele] Claude natif %s erreur: %s -> repli %s", target, e, STABLE_CLAUDE)
        if on_notice:
            await on_notice("Erreur Claude " + target + " (" + str(e)[:100]
                            + ") -> repli sur " + STABLE_CLAUDE + ".")
        return await _claude_request_native(STABLE_CLAUDE, system, messages, tools, image)


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


# ── Outils natifs du chef (tool use Anthropic) ───────────────────────────────
# Schemas stricts : additionalProperties=False + required explicites. Le chef AGIT
# en appelant ces outils ; plus aucun protocole JSON maison.
CHEF_TOOLS = [
    {
        "name": "create_agent",
        "description": ("Cree un agent de l'equipe avec un nom court et un role. Par defaut les agents "
                        "tournent sur un modele economique (Claude Sonnet). Pour une tache complexe, "
                        "passe \"model\":\"claude-opus-4-8\" afin de surclasser cet agent. Cree un agent "
                        "AVANT de lui assigner une tache."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Nom court et unique de l'agent."},
                "role": {"type": "string", "description": "Role / specialite (ex: chercheur, dev back, QA)."},
                "provider": {"type": "string", "enum": ["claude", "gemini", "openai"],
                             "description": "Fournisseur du modele (par defaut claude)."},
                "model": {"type": "string",
                          "description": "Identifiant du modele (optionnel ; claude-opus-4-8 pour surclasser)."},
            },
            "required": ["name", "role"],
            "additionalProperties": False,
        },
    },
    {
        "name": "assign_task",
        "description": ("Confie une sous-tache precise et autonome a un agent EXISTANT. L'agent ecrit ses "
                        "livrables dans des fichiers du dossier partage."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "agent": {"type": "string", "description": "Nom de l'agent existant a qui confier la tache."},
                "instruction": {"type": "string", "description": "Consigne precise et autonome."},
            },
            "required": ["agent", "instruction"],
            "additionalProperties": False,
        },
    },
    {
        "name": "parallel_assign",
        "description": ("Confie plusieurs sous-taches EN PARALLELE pour gagner du temps. Utilise des agents "
                        "DISTINCTS et des FICHIERS DISJOINTS uniquement (sinon risque de conflits)."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "assignments": {
                    "type": "array",
                    "description": "Liste d'assignations (agents distincts, fichiers disjoints).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "agent": {"type": "string", "description": "Nom de l'agent existant."},
                            "instruction": {"type": "string", "description": "Consigne precise et autonome."},
                        },
                        "required": ["agent", "instruction"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["assignments"],
            "additionalProperties": False,
        },
    },
    {
        "name": "challenge",
        "description": ("Verification croisee adversariale entre modeles, pour les livrables importants ou "
                        "sensibles aux erreurs. Un Producteur (Claude par defaut) realise le travail, puis "
                        "les autres modeles (Gemini et ChatGPT) le critiquent ; il n'est valide que si tous "
                        "approuvent, sinon il corrige sur plusieurs tours."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "instruction": {"type": "string", "description": "Consigne precise du livrable a produire."},
                "rounds": {"type": "integer", "description": "Nombre de tours de correction (optionnel, defaut 2)."},
                "producer_provider": {"type": "string", "enum": ["claude", "gemini", "openai"],
                                      "description": "Fournisseur du Producteur (optionnel, defaut claude)."},
                "critic_providers": {"type": "array", "items": {"type": "string"},
                                     "description": "Fournisseurs des critiques (optionnel)."},
            },
            "required": ["instruction"],
            "additionalProperties": False,
        },
    },
    {
        "name": "assign_managed_agent",
        "description": ("Delegue une mission a un AGENT ANTHROPIC GERE (il travaille dans un environnement "
                        "isole cote Anthropic, pas sur cette machine), liste dans le contexte (champ "
                        "\"Agents Anthropic geres\"), identifie par son agent_id. Utilise-le quand l'un "
                        "d'eux a deja le role/contexte recherche, ou pour une mission qui doit etre isolee. "
                        "Ses fichiers livrables sont recuperes automatiquement dans le sous-dossier "
                        "managed/ du dossier de travail. Optionnel : github_repo_url monte un depot GitHub "
                        "dans sa session (il peut alors lire/tester le code reel) ; rubric transforme la "
                        "mission en OBJECTIF NOTE (un examinateur independant verifie les criteres et "
                        "l'agent itere jusqu'a validation) -- fournis alors des criteres VERIFIABLES. "
                        "Les missions d'agents geres PARTAGENT une memoire : chacune recoit "
                        "automatiquement les lecons des precedentes et y ajoute les siennes."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Identifiant de l'agent gere (agent_...)."},
                "instruction": {"type": "string", "description": "Consigne autonome pour l'agent gere."},
                "github_repo_url": {"type": "string",
                                    "description": "URL https d'un depot GitHub a monter dans la session "
                                                   "(optionnel ; en mode entreprise le depot cible est "
                                                   "monte automatiquement)."},
                "rubric": {"type": "string",
                           "description": "Criteres de reussite verifiables (optionnel). Si fourni, la "
                                          "mission est notee et l'agent corrige jusqu'a validation."},
            },
            "required": ["agent_id", "instruction"],
            "additionalProperties": False,
        },
    },
    {
        "name": "finish",
        "description": ("Appelle cet outil quand l'objectif est atteint, avec un resume de ce qui a ete "
                        "livre."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "final": {"type": "string", "description": "Resume du resultat livre."},
            },
            "required": ["final"],
            "additionalProperties": False,
        },
    },
]


# ── Prompts systeme ─────────────────────────────────────────────────────────
CHEF_SYSTEM = """Tu es le CHEF d'une equipe d'agents IA autonomes.

OBJECTIF GLOBAL (fixe par l'utilisateur) :
{objective}

Tu construis et diriges l'equipe toi-meme. Tu peux creer jusqu'a {max_agents} agents.
Tu AGIS en appelant tes outils (create_agent, assign_task, parallel_assign, challenge,
assign_managed_agent, finish). Pense d'abord, puis declenche l'outil adapte a ta prochaine action.

Regles :
- Decompose l'objectif, cree les roles utiles (ex: chercheur, codeur, redacteur, critique), delegue, fais iterer puis termine.
- Cree un agent AVANT de lui assigner une tache.
- Les agents ecrivent leurs livrables dans des fichiers du dossier partage.
- Pour confier plusieurs sous-taches d'un coup, utilise parallel_assign avec des agents DISTINCTS et des FICHIERS DISJOINTS.
- Pour un livrable important ou sensible aux erreurs, prefere challenge a un simple assign_task afin que les modeles se confrontent.
- Tu peux deleguer a un agent Anthropic gere (assign_managed_agent) quand l'un d'eux a deja le role/contexte recherche.
- Quand le travail est valide, appelle finish avec un resume du resultat livre."""


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
Tu AGIS en appelant tes outils (create_agent, assign_task, parallel_assign, challenge,
assign_managed_agent, finish). Cree un agent avant de lui assigner une tache ; rappelle a chaque agent
d'ecrire UNIQUEMENT dans delivery/ ; pour parallel_assign garde des agents distincts et des fichiers
disjoints ; utilise challenge (verification croisee) pour les livrables sensibles ; quand le travail
est livre, appelle finish avec un resume des livrables dans delivery/ et le chemin a transmettre a Claude Code.

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


# ── Outils natifs des workers Claude (tool use Anthropic) ────────────────────
# Schemas stricts (additionalProperties=False) SAUF mcp_call dont les "arguments" sont
# un objet libre : strict requiert additionalProperties=false partout, donc on n'active
# PAS strict sur mcp_call uniquement.
WORKER_TOOLS = [
    {
        "name": "write_file",
        "description": ("Ecrit un livrable dans un fichier du dossier de travail. Utilise des chemins "
                        "RELATIFS. C'est ainsi que tu produis tes resultats."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Chemin relatif du fichier a ecrire."},
                "content": {"type": "string", "description": "Contenu complet du fichier."},
            },
            "required": ["path", "content"],
            "additionalProperties": False,
        },
    },
    {
        "name": "read_file",
        "description": ("Lit un fichier du dossier de travail (ou, en mode entreprise, du depot cible en "
                        "lecture seule). Utilise-le pour consulter le code reel avant d'agir."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Chemin relatif du fichier a lire."},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_command",
        "description": ("Execute une commande autorisee dans le dossier de travail (ex: lancer un script, "
                        "installer un paquet Python avec pip)."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "cmd": {"type": "string", "description": "Commande a executer (premier mot en liste blanche)."},
            },
            "required": ["cmd"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_tests",
        "description": "Lance la suite de tests (pytest) sur le dossier de travail.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "web_search",
        "description": ("Recherche sur le web. Utilise-le quand l'information n'est pas dans le dossier de "
                        "travail et que la recherche web est activee pour cette tache."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Requete de recherche."},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": "add_mcp",
        "description": ("Active un serveur MCP OFFICIEL (fetch, git, time, filesystem, memory, "
                        "sequentialthinking, everything), puis appelle ses outils via mcp_call. Tout serveur "
                        "non officiel est refuse."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "server": {"type": "string", "description": "Nom du serveur MCP officiel a activer."},
            },
            "required": ["server"],
            "additionalProperties": False,
        },
    },
    {
        "name": "mcp_call",
        "description": ("Appelle un outil d'un serveur MCP deja active avec add_mcp. Les arguments sont un "
                        "objet libre propre a l'outil appele."),
        "input_schema": {
            "type": "object",
            "properties": {
                "server": {"type": "string", "description": "Nom du serveur MCP actif."},
                "name": {"type": "string", "description": "Nom de l'outil MCP a appeler."},
                "arguments": {"type": "object", "description": "Arguments de l'outil.",
                              "additionalProperties": True},
            },
            "required": ["server", "name"],
        },
    },
    {
        "name": "save_knowledge",
        "description": ("ENREGISTRE une astuce reutilisable dans TON namespace de role -- elle te servira "
                        "(ainsi qu'aux futurs agents du meme role) sur les taches a venir. Enregistre une "
                        "lecon AVANT de finir si tu as appris quelque chose de generalisable."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "Lecon ou astuce a memoriser."},
            },
            "required": ["content"],
            "additionalProperties": False,
        },
    },
    {
        "name": "search_knowledge",
        "description": ("Cherche dans la memoire persistante (TON namespace de role + lecons partagees + "
                        "notes utilisateur). Consulte la memoire AU DEBUT de chaque mission."),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Requete de recherche dans la memoire."},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
]


WORKER_SYSTEM_NATIVE = """Tu es l'agent << {name} >>. Ton role : {role}.
Tu travailles dans un dossier de travail partage avec ton equipe. Recherche web disponible : {web}.

Agis en appelant tes outils. Quand ta mission est accomplie, n'appelle plus d'outils et redige ton
rapport final en texte.

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
  appris quelque chose de generalisable."""


# ── Controle de la boucle ────────────────────────────────────────────────────
async def _wait_if_paused(task_id):
    ctrl = running_tasks.get(task_id)
    if ctrl:
        await ctrl["pause"].wait()


async def _is_stopped(task_id) -> bool:
    t = await db_get_task(task_id)
    return (t is None) or (t["status"] == "stopped")


# ── Moteur : un worker execute une sous-tache ───────────────────────────────
def _company_addendum(system):
    # Addendum du mode entreprise, ajoute au prompt systeme des workers (natif et legacy).
    return (system + NL + "MODE ENTREPRISE : le depot d'origine est en LECTURE SEULE (read_file le voit). "
            "Tu ecris (write_file) UNIQUEMENT dans 'delivery/...'. Ne tente jamais d'ecrire hors du "
            "dossier de travail. Teste tes correctifs dans delivery/ avant de livrer." + NL
            + "AVANT de proposer un changement : LIS le code reel concerne (read_file sur la cible : "
            "schema.sql, migrations, config/bindings, fichiers vises, CLAUDE.md). Ne suppose pas "
            "qu'une table/binding/endpoint/stockage existe — verifie. Marque toute hypothese "
            "'HYPOTHESE A VALIDER'. Ne RETIRE jamais une protection de securite existante (CSRF, "
            "auth, rate-limit, sanitization). Peu de correctifs qui marchent > un gros dump. Ne "
            "reference aucun fichier que tu n'as pas reellement ecrit.")


async def _run_worker(task_id, iteration, agent, instruction, folder, web_enabled,
                      worker_histories, notice, image=None, custom_hist=None) -> str:
    # Aiguillage : provider claude -> boucle native (tool use Anthropic) ;
    # gemini/openai -> boucle legacy (protocole JSON via call_model).
    if agent["provider"] != "claude":
        return await _run_worker_legacy(task_id, iteration, agent, instruction, folder, web_enabled,
                                        worker_histories, notice, image=image, custom_hist=custom_hist)
    name = agent["name"]
    hist = custom_hist if custom_hist is not None else worker_histories.setdefault(name, [])
    hist.append({"role": "user", "content": "Mission du chef : " + instruction})
    system = WORKER_SYSTEM_NATIVE.format(
        name=name, role=agent["role"],
        web=("oui" if web_enabled else "non"),
        whitelist=", ".join(sorted(COMMAND_WHITELIST)),
        mcp_servers=", ".join(sorted(OFFICIAL_MCP)),
    )
    _t = await db_get_task(task_id)
    company = bool(_t and _t.get("company_mode"))
    if company:
        system = _company_addendum(system)
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
        resp, usage = await _call_claude_native(
            agent["model"] or DEFAULT_WORKER_CLAUDE, system, hist, WORKER_TOOLS,
            on_notice=notice, image=image)
        await _account_cost(task_id, usage, notice)
        # On rejoue le contenu brut (blocs SDK : thinking/text/tool_use echoes a l'identique).
        hist.append({"role": "assistant", "content": resp.content})

        thinking_text = "".join(getattr(b, "thinking", "") for b in resp.content
                                if getattr(b, "type", "") == "thinking")
        text = "".join(getattr(b, "text", "") for b in resp.content
                       if getattr(b, "type", "") == "text")
        tool_uses = [b for b in resp.content if getattr(b, "type", "") == "tool_use"]

        # Pensee (ou narration accompagnant des appels d'outils) -> visible dans l'UI.
        thought = thinking_text or (text if tool_uses else "")
        if thought:
            await emit(task_id, iteration, name, "agent_message",
                       {"kind": "thought", "content": thought[:2000]})

        if not tool_uses:
            # Plus d'outil appele : la mission est terminee, le rapport est le texte final.
            last_report = text or thinking_text or "Termine."
            await emit(task_id, iteration, name, "agent_message",
                       {"kind": "result", "content": last_report})
            return last_report

        async def _exec_one(tu):
            tool = tu.name
            act = {"tool": tool, **tu.input}
            await emit(task_id, iteration, name, "tool_call", {"tool": tool, "input": tu.input})
            out = await execute_tool(task_id, folder, web_enabled, act, agent_name=name)
            await emit(task_id, iteration, name, "tool_result", {"tool": tool, "output": out[:1500]})
            return tu.id, tool, out

        # Lecture seule -> parallelisable (#15) ; mutations -> sequentiel (evite les races fichiers).
        parallel = [tu for tu in tool_uses if tu.name in PARALLEL_SAFE_TOOLS]
        sequential = [tu for tu in tool_uses if tu.name not in PARALLEL_SAFE_TOOLS]
        outputs = {}  # tool_use_id -> sortie texte
        touched = False
        if parallel:
            for tu_id, _tool, out in await asyncio.gather(*[_exec_one(tu) for tu in parallel]):
                outputs[tu_id] = out
        for tu in sequential:
            tu_id, tool, out = await _exec_one(tu)
            outputs[tu_id] = out
            if tool in ("write_file", "run_command", "run_tests"):
                touched = True
        if touched:
            await emit(task_id, iteration, name, "files_changed", {"list": _list_files(folder)})

        # Un tool_result par tool_use, dans l'ORDRE d'origine des tool_use.
        tool_results = [{"type": "tool_result", "tool_use_id": tu.id,
                         "content": outputs.get(tu.id, "")} for tu in tool_uses]
        hist.append({"role": "user", "content": tool_results})
    # Boucle epuisee : rendre le dernier texte connu, apres avoir emis un resultat s'il y a lieu.
    if last_report and last_report != "(aucun rapport)":
        await emit(task_id, iteration, name, "agent_message",
                   {"kind": "result", "content": last_report})
    return last_report


async def _run_worker_legacy(task_id, iteration, agent, instruction, folder, web_enabled,
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
        system = _company_addendum(system)
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
                 "reprends la ou ca s'est arrete et avance vers l'objectif. Appelle un de tes outils.")
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
        start = "Demarre. Quelle est ta premiere action ? Appelle un de tes outils (create_agent, assign_task ou finish)."
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
                   + NL + "Quelle est ta prochaine action ?")
            chef_messages.append({"role": "user", "content": ctx})
            await _log_prompt(task_id, iteration, "chef", chef_system, chef_messages)
            resp, usage = await _call_claude_native(chef_model, chef_system, chef_messages,
                                                    CHEF_TOOLS, on_notice=notice)
            await _account_cost(task_id, usage, notice)

            thinking_text = "".join(getattr(b, "thinking", "") for b in resp.content
                                    if getattr(b, "type", "") == "thinking")
            text = "".join(getattr(b, "text", "") for b in resp.content
                           if getattr(b, "type", "") == "text")
            tool_uses = [b for b in resp.content if getattr(b, "type", "") == "tool_use"]
            # On rejoue le contenu brut (blocs SDK : thinking echoes a l'identique - REQUIS).
            chef_messages.append({"role": "assistant", "content": resp.content})

            # Toujours emettre la premiere decision, meme sans texte de reflexion
            # (sinon un tool_use "sec" serait invisible dans l'UI).
            if thinking_text or text or tool_uses:
                await emit(task_id, iteration, "chef", "chef",
                           {"decision": (tool_uses[0].name if tool_uses else "reflexion"),
                            "thought": (thinking_text or text)[:2000],
                            "instruction": (tool_uses[0].input.get("instruction") if tool_uses else None)})
            # Une decision visible par tool_use supplementaire (l'UI affiche chaque decision).
            for tu in tool_uses[1:]:
                await emit(task_id, iteration, "chef", "chef",
                           {"decision": tu.name, "thought": "",
                            "instruction": tu.input.get("instruction")})

            if not tool_uses:
                chef_messages.append({"role": "user", "content":
                                      "Aucun outil appele. Utilise un de tes outils (create_agent, "
                                      "assign_task, parallel_assign, challenge, assign_managed_agent) ou finish."})
                continue

            # Execution SEQUENTIELLE des tool_use ; le retour devient le tool_result de chacun.
            tool_results = []
            for tu in tool_uses:
                act = tu.name
                inp = tu.input

                if act == "finish":
                    final = inp.get("final") or thinking_text or text or "Objectif traite."
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
                    return  # plus aucun appel API : inutile de repondre aux tool_use restants

                elif act == "create_agent":
                    if len(team) >= max_agents:
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "Refuse : plafond de " + str(max_agents)
                                             + " agents atteint. Delegue a un agent existant ou termine."})
                        continue
                    name = (inp.get("name") or ("agent" + str(len(team) + 1))).strip()
                    role = inp.get("role", "")
                    provider = inp.get("provider", "claude")
                    if provider not in ("claude", "gemini", "openai"):
                        provider = "claude"
                    model = inp.get("model") or _default_model(provider)
                    if await db_get_agent(task_id, name):
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "Un agent nomme '" + name
                                             + "' existe deja. Choisis un autre nom ou delegue-lui."})
                        continue
                    await db_add_agent(task_id, name, role, provider, model, "chef")
                    await emit(task_id, iteration, "chef", "agent_created",
                               {"name": name, "role": role, "model": provider + ":" + model})
                    team = await db_list_agents(task_id)  # le plafond suit les creations du tour
                    tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                         "content": "Agent '" + name + "' cree."})

                elif act == "assign_task":
                    target = (inp.get("agent") or "").strip()
                    instruction = inp.get("instruction", "")
                    agent = await db_get_agent(task_id, target)
                    if not agent:
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "Agent '" + target
                                             + "' introuvable. Cree-le d'abord (create_agent)."})
                        continue
                    report = await _run_worker(task_id, iteration, agent, instruction,
                                               folder, web_enabled, worker_histories, notice, image)
                    tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                         "content": "Rapport de " + target + " : " + report[:2000]})

                elif act == "assign_managed_agent":
                    agent_id = (inp.get("agent_id") or "").strip()
                    instruction = inp.get("instruction", "") or ""
                    if not agent_id or not agent_id.startswith("agent_"):
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "assign_managed_agent : champ 'agent_id' manquant "
                                             "ou invalide (doit etre 'agent_...')."})
                        continue
                    label = agent_id
                    # Depot GitHub a monter dans la session : explicite (github_repo_url),
                    # sinon celui du mode entreprise. Necessite GITHUB_TOKEN (.env) --
                    # le token est garde cote Anthropic, jamais visible de l'agent.
                    repo_url = (inp.get("github_repo_url") or "").strip()
                    if not repo_url and company:
                        repo_url = (task.get("target_repo_url") or "").strip()
                    resources = None
                    if repo_url:
                        # Priorite : jeton DU PROJET (limite les droits a ce depot),
                        # sinon jeton global de l'app, sinon GITHUB_TOKEN du .env.
                        gh_token = ((task.get("github_token") or "").strip()
                                    or await app_settings.get_github_token())
                        if gh_token:
                            resources = [{"type": "github_repository", "url": repo_url,
                                          "authorization_token": gh_token}]
                        else:
                            await notice("Depot " + repo_url + " non monte sur l'agent gere : "
                                         "aucun jeton GitHub (champ du projet a la creation, "
                                         "ou /control onglet Settings, ou GITHUB_TOKEN du .env).")
                    rubric = (inp.get("rubric") or "").strip() or None
                    # Memoire PARTAGEE entre missions d'agents geres : on injecte en tete
                    # de l'instruction les lecons tirees des missions precedentes (+ lecons
                    # communes + notes utilisateur). L'agent gere demarre ainsi avec le
                    # contexte accumule, sans qu'on ait a tout lui repeter.
                    instruction_for_agent = instruction
                    mem_hits = []
                    try:
                        import memory as _mem
                        mem_hits = await _mem.recall(
                            instruction or objective,
                            namespaces=[_mem.MANAGED_NS, "_shared", "_user"], k=6)
                    except Exception as e:
                        log.info("[memoire] rappel agent gere indisponible (%s)", e)
                    if mem_hits:
                        mem_block = (NL.join("- " + h["content"] for h in mem_hits))
                        instruction_for_agent = (
                            "MEMOIRE PARTAGEE (lecons des missions precedentes d'agents geres "
                            "et regles communes -- tiens-en compte, ne les repete pas betement) :"
                            + NL + mem_block + NL + NL
                            + "MISSION :" + NL + instruction)
                    await emit(task_id, iteration, label, "agent_message",
                               {"kind": "thought",
                                "content": "Delegation a l'agent gere Anthropic " + agent_id
                                + (" (depot monte : " + repo_url + ")" if resources else "")
                                + (" -- mission notee avec criteres de reussite" if rubric else "")
                                + (" -- " + str(len(mem_hits)) + " lecon(s) de memoire partagee injectee(s)"
                                   if mem_hits else "")
                                + "..."})

                    async def _on_text(t, _l=label, _it=iteration):
                        await emit(task_id, _it, _l, "agent_message", {"kind": "result", "content": t})

                    try:
                        res = await managed_agents.run_session(
                            agent_id, instruction_for_agent, on_text=_on_text,
                            resources=resources, rubric=rubric,
                            outputs_dir=str(Path(folder) / "managed"),
                            # Une mission notee itere plusieurs fois : delai elargi.
                            timeout_s=(managed_agents.SESSION_TIMEOUT_S * 3 if rubric
                                       else managed_agents.SESSION_TIMEOUT_S))
                        if res.get("console_url"):
                            await emit(task_id, iteration, "systeme", "info",
                                       {"msg": "Session agent gere (suivi en direct) : "
                                        + res["console_url"]})
                        mtext = res.get("text") or "(reponse vide)"
                        extra = ""
                        fl = res.get("files") or []
                        if fl:
                            await emit(task_id, iteration, label, "files_changed",
                                       {"list": _list_files(folder)})
                            extra += (NL + "Fichiers livres : "
                                      + ", ".join("managed/" + p for p in fl))
                        oc = res.get("outcome") or {}
                        if oc.get("result"):
                            extra += (NL + "Verdict de l'examinateur : " + str(oc["result"])
                                      + (" -- " + oc["explanation"][:400] if oc.get("explanation") else ""))
                        # Memoire PARTAGEE : a la fin d'une mission reussie, on tire 1-3 lecons
                        # reutilisables et on les range dans le namespace `managed`, pour que les
                        # PROCHAINES missions d'agents geres en beneficient (cercle vertueux).
                        if res.get("status") in ("idle", "terminated") and mtext and mtext != "(reponse vide)":
                            try:
                                import memory as _mem
                                blob = ("Mission confiee a un agent gere :" + NL + instruction
                                        + NL + NL + "Resultat de l'agent :" + NL + mtext[:6000])
                                n_mem = await _mem.summarize_and_save(
                                    task_id, instruction, blob, _client,
                                    namespace=_mem.MANAGED_NS,
                                    source_prefix="managed:" + agent_id + ":task:")
                                if n_mem:
                                    await emit(task_id, iteration, "systeme", "info",
                                               {"msg": str(n_mem) + " lecon(s) ajoutee(s) a la memoire "
                                                "partagee des agents geres."})
                            except Exception as e:
                                log.info("[memoire] capture mission agent gere KO : %s", e)
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "Rapport de l'agent gere " + agent_id + " (statut="
                                             + str(res.get("status")) + ", stop_reason="
                                             + str(res.get("stop_reason")) + ") :" + NL
                                             + mtext[:3000] + extra})
                    except Exception as e:
                        log.warning("[managed-agents] erreur session %s : %s", agent_id, e)
                        await notice("Agent gere " + agent_id + " : erreur " + str(e)[:200])
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "assign_managed_agent a echoue (" + str(e)[:200]
                                             + "). Essaie un autre agent ou une autre action."})

                elif act == "parallel_assign":
                    assigns = inp.get("assignments", []) or []
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
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "parallel_assign : aucun agent valide. Cree-les d'abord."})
                        continue
                    reports = await asyncio.gather(*coros)
                    summary = NL.join(names[i] + " : " + reports[i][:600] for i in range(len(names)))
                    tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                         "content": "Rapports paralleles :" + NL + summary})

                elif act == "challenge":
                    instruction = inp.get("instruction", "")
                    if not instruction:
                        tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                             "content": "challenge requiert un champ 'instruction'."})
                        continue
                    report = await _run_challenge(
                        task_id, iteration, instruction, folder, web_enabled,
                        inp.get("rounds", 2), worker_histories, notice,
                        inp.get("producer_provider", "claude"),
                        inp.get("critic_providers"), image)
                    tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                         "content": "Resultat du challenge : " + report[:2000]})

                else:
                    tool_results.append({"type": "tool_result", "tool_use_id": tu.id,
                                         "content": "Outil non reconnu : " + str(act)})

            # Un tool_result par tool_use, dans l'ordre des tool_use.
            chef_messages.append({"role": "user", "content": tool_results})

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
    github_token: Optional[str] = None  # jeton GitHub DU PROJET (clone + montage sur agents geres) ; conserve avec la tache, jamais renvoye par l'API
    max_cost_usd: float = 0  # plafond de cout par tache en USD (0 = pas de plafond)


class ScheduleCreate(BaseModel):
    objective: str
    schedule_kind: str = "once"          # once | daily | interval
    run_at: Optional[str] = None         # "YYYY-MM-DDTHH:MM" LOCAL (kind=once)
    daily_time: Optional[str] = None     # "HH:MM" LOCAL (kind=daily)
    interval_minutes: Optional[int] = None  # (kind=interval)
    tz_offset_minutes: int = 0           # -getTimezoneOffset() du navigateur
    enabled: bool = True
    # Parametres de la tache creee a l'echeance (memes champs que TaskCreate) :
    chef_model: str = DEFAULT_CLAUDE
    web_enabled: bool = True
    max_iterations: int = 15
    max_agents: int = 4
    company_mode: bool = False
    target_repo_url: Optional[str] = None
    github_token: Optional[str] = None   # jamais renvoye par l'API
    max_cost_usd: float = 0


# Champs de config d'une mission programmee transmis a TaskCreate a l'echeance.
_SCHEDULE_CONFIG_KEYS = ("chef_model", "web_enabled", "max_iterations", "max_agents",
                         "company_mode", "target_repo_url", "github_token", "max_cost_usd")


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
    # Jeton GitHub DU PROJET : conserve avec la tache (clone + montage du depot
    # sur les agents geres). Priorite : jeton du projet > jeton global de l'app
    # (onglet Settings) > GITHUB_TOKEN du .env. Jamais renvoye par l'API.
    project_token = (body.github_token or "").strip() or None
    if company:
        if body.target_repo_url:
            clone_token = project_token or await app_settings.get_github_token() or None
            try:
                target_path = await _clone_repo(body.target_repo_url, clone_token)
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
                                   max_cost_usd=max(0.0, float(body.max_cost_usd or 0)),
                                   target_repo_url=(body.target_repo_url or "").strip() or None,
                                   github_token=project_token)
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


# ── Planificateur de missions programmees ────────────────────────────────────
_SCHED_TICK_SECONDS = int(os.getenv("SCHEDULER_TICK_SECONDS", "20"))
_scheduler_task: Optional[asyncio.Task] = None


async def _fire_schedule(sched: dict):
    """Cree et lance la tache d'une mission programmee, puis reprogramme la suite.

    Une mission `once` est desactivee apres son unique declenchement ; les missions
    `daily`/`interval` recoivent un nouveau `next_run`. Toute erreur de lancement est
    consignee dans `last_status` sans interrompre le planificateur.
    """
    sid = sched["id"]
    try:
        cfg = json.loads(sched.get("config") or "{}")
    except Exception:
        cfg = {}
    body = TaskCreate(
        objective=sched["objective"],
        chef_model=cfg.get("chef_model") or DEFAULT_CLAUDE,
        web_enabled=bool(cfg.get("web_enabled", True)),
        max_iterations=int(cfg.get("max_iterations") or 15),
        max_agents=int(cfg.get("max_agents") or 4),
        company_mode=bool(cfg.get("company_mode", False)),
        target_repo_url=(cfg.get("target_repo_url") or None),
        github_token=(cfg.get("github_token") or None),
        max_cost_usd=float(cfg.get("max_cost_usd") or 0),
    )
    now = datetime.utcnow()
    last_status = ""
    task_id = None
    try:
        created = await create_task(body)        # reutilise toute la logique (clone, agents...)
        task_id = created.get("id")
        _spawn_loop(task_id, resume=False)
        last_status = "lancee (tache #" + str(task_id) + ")"
        log.info("[scheduler] mission programmee #%s -> tache #%s lancee.", sid, task_id)
    except Exception as e:
        last_status = "erreur au lancement : " + str(e)[:200]
        log.warning("[scheduler] mission #%s : %s", sid, last_status)

    # Reprogrammation : `once` -> desactivee ; sinon prochain creneau a partir de maintenant.
    upd = {"last_run": now.isoformat(), "last_status": last_status,
           "runs_count": int(sched.get("runs_count") or 0) + 1}
    if task_id is not None:
        upd["last_task_id"] = task_id
    if sched.get("schedule_kind") == "once":
        upd["enabled"] = 0
        upd["next_run"] = None
    else:
        nxt = _compute_next_run(
            sched.get("schedule_kind"),
            daily_time=sched.get("daily_time"),
            interval_minutes=sched.get("interval_minutes"),
            tz_offset_minutes=sched.get("tz_offset_minutes") or 0,
            after=now)
        upd["next_run"] = nxt
    await db_update_schedule(sid, **upd)


async def _scheduler_loop():
    log.info("[scheduler] planificateur demarre (tick %ss).", _SCHED_TICK_SECONDS)
    while True:
        try:
            due = await db_due_schedules(datetime.utcnow().isoformat())
            for sched in due:
                await _fire_schedule(sched)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("[scheduler] tick en erreur : %s", str(e)[:200])
        await asyncio.sleep(_SCHED_TICK_SECONDS)


def start_scheduler():
    """Demarre la boucle du planificateur (idempotent). A appeler dans un event loop."""
    global _scheduler_task
    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(_scheduler_loop())


async def stop_scheduler():
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except (asyncio.CancelledError, Exception):
            pass
    _scheduler_task = None


@router.get("/api/schedules")
async def list_schedules():
    items = await db_list_schedules()
    return {"schedules": [_public_schedule(s) for s in items]}


@router.post("/api/schedules")
async def create_schedule(body: ScheduleCreate):
    objective = (body.objective or "").strip()
    if not objective:
        raise HTTPException(400, "Objectif manquant.")
    kind = (body.schedule_kind or "once").strip()
    if kind not in SCHEDULE_KINDS:
        raise HTTPException(400, "Type de planification inconnu : " + kind
                            + " (attendu : once, daily ou interval).")
    if kind == "interval" and (not body.interval_minutes or body.interval_minutes < 1):
        raise HTTPException(400, "interval : interval_minutes doit etre >= 1.")
    if kind == "daily" and not _parse_hhmm(body.daily_time or ""):
        raise HTTPException(400, "daily : daily_time doit etre au format HH:MM.")
    if kind == "once" and _parse_local_dt(body.run_at or "") is None:
        raise HTTPException(400, "once : run_at doit etre une date-heure 'YYYY-MM-DDTHH:MM'.")
    if body.company_mode and not (body.target_repo_url or "").strip():
        raise HTTPException(400, "Mode entreprise : fournis target_repo_url (depot GitHub a auditer).")
    next_run = _compute_next_run(
        kind, run_at=body.run_at, daily_time=body.daily_time,
        interval_minutes=body.interval_minutes, tz_offset_minutes=body.tz_offset_minutes)
    if not next_run:
        raise HTTPException(400, "Impossible de calculer la prochaine echeance (parametres incomplets).")
    config = {k: getattr(body, k) for k in _SCHEDULE_CONFIG_KEYS}
    # Normalise : pas de jeton vide stocke.
    config["github_token"] = (config.get("github_token") or "").strip() or None
    config["target_repo_url"] = (config.get("target_repo_url") or "").strip() or None
    sid = await db_create_schedule(
        objective, config, kind, next_run,
        run_at=(body.run_at or None), daily_time=(body.daily_time or None),
        interval_minutes=body.interval_minutes, tz_offset_minutes=body.tz_offset_minutes,
        enabled=body.enabled)
    return _public_schedule(await db_get_schedule(sid))


@router.post("/api/schedules/{sched_id}/toggle")
async def toggle_schedule(sched_id: int):
    s = await db_get_schedule(sched_id)
    if not s:
        raise HTTPException(404, "Mission programmee introuvable.")
    new_enabled = 0 if s.get("enabled") else 1
    upd = {"enabled": new_enabled}
    # Reactivee : si l'echeance est passee (ou absente), on recalcule a partir de maintenant
    # pour eviter un declenchement immediat surprise (sauf `once` dont l'heure est figee).
    if new_enabled and s.get("schedule_kind") != "once":
        upd["next_run"] = _compute_next_run(
            s.get("schedule_kind"), daily_time=s.get("daily_time"),
            interval_minutes=s.get("interval_minutes"),
            tz_offset_minutes=s.get("tz_offset_minutes") or 0)
    await db_update_schedule(sched_id, **upd)
    return _public_schedule(await db_get_schedule(sched_id))


@router.post("/api/schedules/{sched_id}/run-now")
async def run_schedule_now(sched_id: int):
    s = await db_get_schedule(sched_id)
    if not s:
        raise HTTPException(404, "Mission programmee introuvable.")
    await _fire_schedule(s)
    return _public_schedule(await db_get_schedule(sched_id))


@router.delete("/api/schedules/{sched_id}")
async def delete_schedule(sched_id: int):
    s = await db_get_schedule(sched_id)
    if not s:
        raise HTTPException(404, "Mission programmee introuvable.")
    await db_delete_schedule(sched_id)
    return {"deleted": sched_id}


# ── Registre local -> vrais Agents geres Anthropic ───────────────────────────
class PromoteAgent(BaseModel):
    system: Optional[str] = None       # prompt systeme explicite (sinon synthetise du role)
    description: Optional[str] = None
    model: Optional[str] = None        # surclasse le modele de l'agent local


async def _promote_local_agent(task_id: int, agent: dict, *, system=None, description=None,
                               model=None) -> dict:
    """Cree un vrai agent gere Anthropic a partir d'un agent local, et lie les deux.

    Leve HTTPException si l'agent n'est pas promouvable (fournisseur non-Claude).
    """
    name = agent.get("name") or ""
    if (agent.get("provider") or "claude") != "claude":
        raise HTTPException(400, "Seuls les agents Claude peuvent devenir des agents geres "
                            "Anthropic (l'agent '" + name + "' est " + str(agent.get("provider")) + ").")
    sys_prompt = (system or "").strip() or managed_agents._system_from_role(name, agent.get("role") or "")
    use_model = (model or "").strip() or (agent.get("model") or "").strip() or DEFAULT_WORKER_CLAUDE
    desc = (description or "").strip() or (("Role : " + agent["role"]) if agent.get("role") else None)
    created = await managed_agents.create_agent(name, model=use_model, system=sys_prompt,
                                                description=desc)
    await db_set_agent_managed(task_id, name, created.get("id"))
    await emit(task_id, 0, "systeme", "info",
               {"msg": "Agent local '" + name + "' promu en agent gere Anthropic "
                + str(created.get("id")) + " (persistant, reutilisable par les futures taches)."})
    return created


@router.post("/api/tasks/{task_id}/agents/{name}/promote")
async def promote_agent(task_id: int, name: str, body: Optional[PromoteAgent] = None):
    """Transforme un agent LOCAL (du registre de la tache) en vrai agent gere Anthropic."""
    if not await db_get_task(task_id):
        raise HTTPException(404, "Tache introuvable.")
    agent = await db_get_agent(task_id, name)
    if not agent:
        raise HTTPException(404, "Agent local '" + name + "' introuvable dans cette tache.")
    if (agent.get("managed_agent_id") or "").strip():
        raise HTTPException(409, "Cet agent est deja lie a l'agent gere "
                            + agent["managed_agent_id"] + ".")
    b = body or PromoteAgent()
    try:
        created = await _promote_local_agent(task_id, agent, system=b.system,
                                             description=b.description, model=b.model)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, "Promotion impossible : " + str(e)[:200])
    return {"promoted": name, "managed_agent": created}


@router.post("/api/tasks/{task_id}/agents/sync")
async def sync_agents(task_id: int):
    """Promeut en agents geres TOUS les agents Claude locaux pas encore synchronises."""
    if not await db_get_task(task_id):
        raise HTTPException(404, "Tache introuvable.")
    agents = await db_list_agents(task_id)
    created, skipped = [], []
    for agent in agents:
        nm = agent.get("name") or ""
        if (agent.get("managed_agent_id") or "").strip():
            skipped.append({"name": nm, "reason": "deja synchronise", "agent_id": agent["managed_agent_id"]})
            continue
        if (agent.get("provider") or "claude") != "claude":
            skipped.append({"name": nm, "reason": "fournisseur non-Claude (" + str(agent.get("provider")) + ")"})
            continue
        try:
            mc = await _promote_local_agent(task_id, agent)
            created.append({"name": nm, "managed_agent": mc})
        except Exception as e:
            skipped.append({"name": nm, "reason": str(e)[:160]})
    return {"created": created, "skipped": skipped,
            "summary": str(len(created)) + " agent(s) gere(s) cree(s), " + str(len(skipped)) + " ignore(s)."}


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
    # _public_task : le jeton GitHub d'un projet n'est jamais expose par l'API.
    return [_public_task(t) for t in await db_list_tasks()]


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    t = _public_task(t)
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
