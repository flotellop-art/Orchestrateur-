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
import hashlib
import io
import json
import logging
import os
import re
import shlex
import shutil
import signal
import socket
import sys
import uuid
import zipfile
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

from app_paths import APP_ROOT, DATA_ROOT, DB_PATH, PROJECTS_ROOT, STATIC_ROOT
import agent_skills
from automations import AutomationStore
from durable_queue import DurableQueue, JobLease, JobState, LeaseLost
import managed_agents  # pont vers les Agents geres Anthropic (delegation depuis le chef)
from messaging import (
    MAX_MESSAGE_CHARACTERS,
    MessageRequest,
    MessagingGateway,
    MessagingConfigurationError,
    ServerChannelRegistry,
)
from install_permissions import (
    AccessLevel,
    DecisionOutcome,
    InstallValidationError,
    build_install_argv,
    evaluate_install,
    make_install_plan,
)
from security_policy import (
    build_child_environment,
    resolve_allowed_target_path,
    validate_github_repo_url,
)
from sandbox_runtime import (
    DockerSandboxRuntime,
    SandboxConfig,
    SandboxError,
    SandboxUnavailableError,
)

load_dotenv(APP_ROOT / ".env", override=False)
load_dotenv(DATA_ROOT / ".env", override=False)
log = logging.getLogger(__name__)

# Configuration
STATIC = STATIC_ROOT
PROJECTS = PROJECTS_ROOT
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
EXECUTION_MODES = frozenset({"docker", "local"})
DEFAULT_EXECUTION_MODE = (os.getenv("ORCHESTRATOR_DEFAULT_EXECUTION") or "docker").strip().lower()
if DEFAULT_EXECUTION_MODE not in EXECUTION_MODES:
    DEFAULT_EXECUTION_MODE = "docker"

COMMAND_WHITELIST = {
    "python", "python3", "pytest",
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
# request_id -> Future resolue par l'endpoint de decision.
install_waiters: dict[str, asyncio.Future] = {}
install_waiter_tasks: dict[str, int] = {}
_KNOWLEDGE_FTS = True  # FTS5 disponible pour la memoire long terme ?
# Cache des recherches web (requete normalisee -> resultat). Borne pour ne pas grossir sans fin.
_web_cache: dict[str, str] = {}
_WEB_CACHE_MAX = 256

# Services durables, initialises par ``start_runtime_services`` dans le lifespan.
task_queue: Optional[DurableQueue] = None
automation_store: Optional[AutomationStore] = None
messaging_gateway: Optional[MessagingGateway] = None
sandbox_runtime: Optional[DockerSandboxRuntime] = None
_runtime_background_tasks: list[asyncio.Task] = []
_runtime_shutdown: Optional[asyncio.Event] = None
_QUEUE_LEASE_SECONDS = 90.0


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "oui"}


def _env_number(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if minimum <= value <= maximum else default


def _make_sandbox_runtime() -> DockerSandboxRuntime:
    """Construit l'unique frontiere Docker depuis la configuration serveur."""
    return DockerSandboxRuntime(
        SandboxConfig(
            allowed_workspace_roots=(PROJECTS.resolve(),),
            cpus=_env_number("ORCHESTRATOR_SANDBOX_CPUS", 1.0, minimum=0.1, maximum=64),
            memory=(os.getenv("ORCHESTRATOR_SANDBOX_MEMORY") or "1g").strip(),
            pids_limit=int(_env_number(
                "ORCHESTRATOR_SANDBOX_PIDS", 128, minimum=16, maximum=4096
            )),
            # Le plafond inclut les installations approuvees ; les commandes
            # ordinaires gardent leur propre delai, plus court.
            default_timeout=_env_number(
                "ORCHESTRATOR_SANDBOX_TIMEOUT", 300, minimum=60, maximum=900
            ),
            # Double opt-in : le runtime l'autorise, mais seuls les appels
            # serveur d'installation/lancement passent network_enabled=True.
            allow_network=(
                _env_bool("ORCHESTRATOR_SANDBOX_ALLOW_INSTALL_NETWORK", False)
                or _env_bool("ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK", False)
            ),
            allowed_env_keys=(
                "CI", "NODE_ENV", "NO_COLOR", "PORT", "PYTHONPATH",
                "PYTHONUNBUFFERED", "NODE_PATH",
            ),
        )
    )


def get_sandbox_runtime() -> DockerSandboxRuntime:
    global sandbox_runtime
    if sandbox_runtime is None:
        sandbox_runtime = _make_sandbox_runtime()
    return sandbox_runtime


def sandbox_install_network_allowed() -> bool:
    return _env_bool("ORCHESTRATOR_SANDBOX_ALLOW_INSTALL_NETWORK", False)


def sandbox_app_network_allowed() -> bool:
    return _env_bool("ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK", False)

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
                install_policy TEXT DEFAULT 'ask',
                execution_mode TEXT DEFAULT 'docker',
                queue_job_id  TEXT,
                run_number    INTEGER DEFAULT 0,
                source_job_id TEXT,
                source_ready  INTEGER DEFAULT 1,
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
                    "ALTER TABLE tasks ADD COLUMN install_policy TEXT DEFAULT 'ask'",
                    "ALTER TABLE tasks ADD COLUMN execution_mode TEXT DEFAULT 'docker'",
                    "ALTER TABLE tasks ADD COLUMN queue_job_id TEXT",
                    "ALTER TABLE tasks ADD COLUMN run_number INTEGER DEFAULT 0",
                    "ALTER TABLE tasks ADD COLUMN source_job_id TEXT",
                    "ALTER TABLE tasks ADD COLUMN source_ready INTEGER DEFAULT 1"):
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
        # Demandes d'installation + accords lies au plan exact et a la tache.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS install_requests (
                id            TEXT PRIMARY KEY,
                task_id       INTEGER NOT NULL,
                iteration     INTEGER DEFAULT 0,
                agent         TEXT DEFAULT '',
                plan_hash     TEXT NOT NULL,
                manager       TEXT NOT NULL,
                package       TEXT NOT NULL,
                version       TEXT NOT NULL,
                scope         TEXT NOT NULL,
                access_level  TEXT NOT NULL,
                reason        TEXT DEFAULT '',
                source        TEXT,
                allow_scripts INTEGER DEFAULT 0,
                status        TEXT NOT NULL,
                decision      TEXT,
                result        TEXT,
                created_at    TEXT NOT NULL,
                expires_at    TEXT,
                resolved_at   TEXT
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_install_requests_task_status "
            "ON install_requests(task_id, status)"
        )
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_source_job "
            "ON tasks(source_job_id) WHERE source_job_id IS NOT NULL"
        )
        try:
            await db.execute("ALTER TABLE install_requests ADD COLUMN source TEXT")
        except Exception:
            pass
        # Memoire long terme partagee (inter-taches). FTS5 si dispo, sinon table simple.
        global _KNOWLEDGE_FTS
        try:
            await db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS knowledge USING fts5(content, created_at UNINDEXED)")
            _KNOWLEDGE_FTS = True
        except Exception:
            await db.execute("CREATE TABLE IF NOT EXISTS knowledge "
                             "(id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, created_at TEXT)")
            _KNOWLEDGE_FTS = False
        # Les taches en cours sont reprises par la file durable. On ne les
        # transforme plus silencieusement en taches arretees au redemarrage.
        await db.execute("UPDATE tasks SET status='queued' WHERE status='running'")
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id,task_id,iteration,agent FROM install_requests WHERE status='pending'"
        ) as cursor:
            pending_on_restart = [dict(row) for row in await cursor.fetchall()]
        for row in pending_on_restart:
            await db.execute(
                "INSERT INTO messages (task_id,iteration,agent,kind,content,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    row["task_id"], row.get("iteration", 0), row.get("agent") or "Agent",
                    "permission_resolved",
                    json.dumps({"request_id": row["id"], "decision": "server_restart"}),
                    datetime.utcnow().isoformat(),
                ),
            )
        await db.execute(
            "UPDATE install_requests SET status='expired', decision='server_restart', resolved_at=? "
            "WHERE status='pending'",
            (datetime.utcnow().isoformat(),),
        )
        await db.commit()


async def db_create_task(objective, folder, max_iterations, max_agents, web_enabled, chef_model,
                         company_mode=False, target_path=None, max_cost_usd=0.0,
                         install_policy="ask", execution_mode="local",
                         source_job_id=None, source_ready=True):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "INSERT INTO tasks (objective,folder,status,max_iterations,max_agents,web_enabled,chef_model,"
            "company_mode,target_path,max_cost_usd,install_policy,execution_mode,source_job_id,source_ready,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (objective, folder, "idle", max_iterations, max_agents,
             1 if web_enabled else 0, chef_model,
             1 if company_mode else 0, target_path, max_cost_usd, install_policy, execution_mode,
             source_job_id, 1 if source_ready else 0,
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
    "total_cost_usd", "max_cost_usd", "install_policy", "execution_mode",
    "queue_job_id", "run_number",
    "source_job_id", "source_ready",
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
        # Les competences sont liees a leur tache source. Elles sont retirees
        # avant la tache afin de ne pas conserver de contenu sans provenance.
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_skills'"
        ) as cursor:
            if await cursor.fetchone():
                await db.execute("DELETE FROM agent_skills WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        await db.execute("DELETE FROM task_agents WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM messages WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM prompts WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM builds WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM install_requests WHERE task_id=?", (task_id,))
        await db.commit()


async def db_transition_task_status(
    task_id: int, new_status: str, allowed_current: set[str] | frozenset[str]
) -> bool:
    if not allowed_current or any(not isinstance(item, str) for item in allowed_current):
        raise ValueError("Etats sources invalides.")
    placeholders = ",".join("?" for _ in allowed_current)
    values = [new_status, task_id, *sorted(allowed_current)]
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cursor = await db.execute(
            f"UPDATE tasks SET status=? WHERE id=? AND status IN ({placeholders})",
            values,
        )
        await db.commit()
        return cursor.rowcount == 1


# -- File durable, reprise et planifications ---------------------------------
async def _configured_channels() -> tuple[str, ...]:
    if messaging_gateway is None:
        return ()
    return messaging_gateway._registry.channel_names


async def enqueue_task_run(
    task_id: int,
    *,
    resume: bool,
    notification_channel: Optional[str] = None,
    source: str = "user",
):
    """Enregistre un lancement avant de l'executer.

    Le numero de lancement persiste avec la tache. Une seconde requete pendant
    qu'un travail est deja actif renvoie le meme travail au lieu de le doubler.
    """
    if task_queue is None:
        raise RuntimeError("La file durable n'est pas initialisee.")
    task = await db_get_task(task_id)
    if not task:
        raise ValueError("Tache introuvable.")
    existing_id = task.get("queue_job_id")
    if existing_id:
        existing = await task_queue.get(existing_id)
        if existing and existing.state in {
            JobState.QUEUED, JobState.RUNNING, JobState.PAUSED
        }:
            return existing

    if notification_channel:
        channels = await _configured_channels()
        if notification_channel not in channels:
            raise ValueError("Canal de notification non configure.")
    run_number = int(task.get("run_number") or 0) + 1
    job = await task_queue.enqueue(
        kind="run_task",
        payload={
            "task_id": task_id,
            "resume": bool(resume),
            "notification_channel": notification_channel,
            "source": source,
        },
        idempotency_key=f"task:{task_id}:run:{run_number}",
        max_attempts=3,
        base_backoff_seconds=5,
    )
    await db_update_task(
        task_id,
        queue_job_id=job.id,
        run_number=run_number,
        status="queued",
    )
    await emit(task_id, int(task.get("iteration") or 0), "systeme", "queued", {
        "job_id": job.id,
        "source": source,
    })
    return job


async def enqueue_notification(
    *, task_id: int, iteration: int, agent: str, channel: str, text: str
) -> str:
    if task_queue is None:
        raise RuntimeError("La file durable n'est pas initialisee.")
    clean_text = (text or "").strip()
    if not clean_text or len(clean_text) > MAX_MESSAGE_CHARACTERS:
        raise ValueError(
            f"Le message doit contenir entre 1 et {MAX_MESSAGE_CHARACTERS} caracteres."
        )
    MessageRequest(channel=channel, text=clean_text)
    if channel not in await _configured_channels():
        raise ValueError("Canal de notification non configure.")
    digest = hashlib.sha256(clean_text.encode("utf-8")).hexdigest()[:20]
    job = await task_queue.enqueue(
        kind="notify",
        payload={"channel": channel, "text": clean_text, "task_id": task_id},
        idempotency_key=(
            f"notify:{task_id}:{max(0, int(iteration))}:"
            f"{hashlib.sha256((agent or 'agent').encode()).hexdigest()[:10]}:{digest}"
        ),
        max_attempts=4,
        base_backoff_seconds=10,
    )
    return job.id


def _job_payload(job) -> dict:
    payload = job.payload if isinstance(job.payload, dict) else {}
    # Une occurrence planifiee enveloppe le payload de l'automatisation.
    nested = payload.get("payload")
    return nested if isinstance(nested, dict) else payload


async def _complete_automation_execution_clone(
    execution_id: int, template_id: int
) -> bool:
    """Finalise idempotemment la copie avant d'autoriser son execution."""
    execution = await db_get_task(execution_id)
    if not execution:
        raise ValueError("L'execution planifiee n'existe plus.")
    if bool(execution.get("source_ready")):
        return False
    template = await db_get_task(template_id)
    if not template:
        raise ValueError("Le modele de tache planifiee n'existe plus.")

    agents = await db_list_agents(template_id)
    image_path = None
    source_image = template.get("image_path")
    if source_image:
        source = Path(source_image)
        if source.is_file() and not source.is_symlink():
            destination = Path(execution["folder"]) / (
                "design_reference" + source.suffix.lower()
            )
            staging = destination.with_name(destination.name + ".pending")
            await asyncio.to_thread(shutil.copy2, source, staging)
            await asyncio.to_thread(os.replace, staging, destination)
            image_path = str(destination)

    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute(
            "SELECT source_ready FROM tasks WHERE id=?", (execution_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            await db.rollback()
            raise ValueError("L'execution planifiee n'existe plus.")
        if bool(row["source_ready"]):
            await db.commit()
            return False
        await db.execute("DELETE FROM task_agents WHERE task_id=?", (execution_id,))
        if agents:
            await db.executemany(
                "INSERT INTO task_agents "
                "(task_id,name,role,provider,model,created_by,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        execution_id,
                        agent["name"],
                        agent.get("role") or "",
                        agent.get("provider") or "claude",
                        agent.get("model") or "",
                        "automation",
                        datetime.utcnow().isoformat(),
                    )
                    for agent in agents
                ],
            )
        await db.execute(
            "UPDATE tasks SET image_path=?, source_ready=1 WHERE id=?",
            (image_path, execution_id),
        )
        await db.commit()
    return True


async def _automation_execution_task(job, template_id: int) -> tuple[int, bool]:
    """Cree une execution distincte et idempotente pour une occurrence."""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id FROM tasks WHERE source_job_id=?", (job.id,)
        ) as cursor:
            existing = await cursor.fetchone()
    if existing:
        execution_id = int(existing["id"])
        initialized = await _complete_automation_execution_clone(
            execution_id, template_id
        )
        return execution_id, initialized

    template = await db_get_task(template_id)
    if not template:
        raise ValueError("Le modele de tache planifiee n'existe plus.")
    if template.get("execution_mode") != "docker":
        raise ValueError("Les planifications exigent une tache Docker isolee.")
    folder = str(PROJECTS / (
        "scheduled_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        + "_" + uuid.uuid4().hex[:6]
    ))
    Path(folder).mkdir(parents=True, exist_ok=True)
    if template.get("company_mode"):
        (Path(folder) / "delivery").mkdir(exist_ok=True)
    try:
        task_id = await db_create_task(
            template["objective"], folder,
            int(template.get("max_iterations") or 15),
            int(template.get("max_agents") or 4),
            bool(template.get("web_enabled")),
            template.get("chef_model") or DEFAULT_CLAUDE,
            company_mode=bool(template.get("company_mode")),
            target_path=template.get("target_path"),
            max_cost_usd=float(template.get("max_cost_usd") or 0),
            install_policy=template.get("install_policy") or "ask",
            execution_mode="docker",
            source_job_id=job.id,
            source_ready=False,
        )
    except Exception:
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id FROM tasks WHERE source_job_id=?", (job.id,)
            ) as cursor:
                raced = await cursor.fetchone()
        if raced:
            await asyncio.to_thread(_rmtree_force, Path(folder))
            execution_id = int(raced["id"])
            initialized = await _complete_automation_execution_clone(
                execution_id, template_id
            )
            return execution_id, initialized
        await asyncio.to_thread(_rmtree_force, Path(folder))
        raise
    await _complete_automation_execution_clone(task_id, template_id)
    await emit(task_id, 0, "systeme", "info", {
        "msg": f"Execution planifiee issue de la tache modele #{template_id}.",
        "automation_id": (job.payload or {}).get("automation_id") if isinstance(job.payload, dict) else None,
    })
    return task_id, True


async def _task_completion_text(task_id: int, status: str) -> str:
    summary = ""
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT content FROM messages WHERE task_id=? AND kind='done' "
            "ORDER BY id DESC LIMIT 1",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
    if row:
        try:
            summary = str(json.loads(row["content"]).get("summary") or "").strip()
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = ""
    label = "terminee" if status == "done" else "en echec"
    text = f"Orchestrator : la tache planifiee #{task_id} est {label}."
    if summary:
        text += " " + summary
    return text[:1900]


async def _execute_run_job(job) -> dict:
    payload = _job_payload(job)
    template_id = payload.get("task_id")
    if not isinstance(template_id, int) or isinstance(template_id, bool):
        raise ValueError("Travail sans identifiant de tache valide.")
    scheduled = isinstance(job.payload, dict) and bool(job.payload.get("automation_id"))
    if scheduled:
        task_id, created = await _automation_execution_task(job, template_id)
    else:
        task_id, created = template_id, False
    task = await db_get_task(task_id)
    if not task:
        return {"status": "missing", "task_id": task_id}
    if task_id in running_tasks:
        await running_tasks[task_id]["task"]
        finished = await db_get_task(task_id)
    else:
        if task.get("execution_mode") == "docker":
            await get_sandbox_runtime().ensure_available()

        await db_update_task(task_id, queue_job_id=job.id, status="queued")
        resume = (not created) and (
            bool(payload.get("resume")) or task.get("status") not in {"idle", "queued"}
        )
        runner = _spawn_loop(task_id, resume=resume)
        await runner
        finished = await db_get_task(task_id)
    status = finished.get("status") if finished else "missing"

    channel = payload.get("notification_channel")
    if channel and status in {"done", "failed"}:
        await enqueue_notification(
            task_id=task_id,
            iteration=int((finished or {}).get("iteration") or 0),
            agent="systeme",
            channel=channel,
            text=await _task_completion_text(task_id, status),
        )
    return {
        "status": status,
        "task_id": task_id,
        "template_task_id": template_id if scheduled else None,
    }


async def _execute_queue_job(job):
    if job.kind == "run_task":
        return await _execute_run_job(job)
    if job.kind == "notify":
        if messaging_gateway is None:
            raise RuntimeError("messaging_unavailable")
        payload = job.payload if isinstance(job.payload, dict) else {}
        receipt = await messaging_gateway.send(MessageRequest(
            channel=payload.get("channel", ""), text=payload.get("text", "")
        ))
        return {"channel": receipt.channel, "status": receipt.status_code}
    raise ValueError("Type de travail inconnu.")


async def _run_with_heartbeat(lease: JobLease):
    if task_queue is None:
        raise RuntimeError("queue_unavailable")
    work = asyncio.create_task(_execute_queue_job(lease.job))
    current = lease
    try:
        while True:
            done, _ = await asyncio.wait({work}, timeout=_QUEUE_LEASE_SECONDS / 3)
            if work in done:
                return await work, current
            current = await task_queue.heartbeat(
                current, lease_seconds=_QUEUE_LEASE_SECONDS
            )
    except BaseException:
        if not work.done():
            work.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await work
        raise


async def _queue_worker(worker_number: int) -> None:
    assert task_queue is not None
    assert _runtime_shutdown is not None
    worker_id = f"local:{os.getpid()}:{worker_number}"
    while not _runtime_shutdown.is_set():
        try:
            lease = await task_queue.claim(
                worker_id=worker_id, lease_seconds=_QUEUE_LEASE_SECONDS
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("File durable momentanement indisponible (%s).", type(exc).__name__)
            try:
                await asyncio.wait_for(_runtime_shutdown.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            continue
        if lease is None:
            try:
                await asyncio.wait_for(_runtime_shutdown.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            result, current = await _run_with_heartbeat(lease)
            await task_queue.complete(current, result=result)
        except LeaseLost:
            # Pause, annulation ou reprise par un autre worker : aucun ancien
            # worker ne peut enregistrer un resultat avec un bail retire.
            continue
        except asyncio.CancelledError:
            # Une pause/annulation peut interrompre le travail imbrique sans
            # signifier l'arret du worker permanent.
            if _runtime_shutdown.is_set():
                with suppress(Exception):
                    await task_queue.release(lease)
                raise
            continue
        except Exception as exc:
            log.warning("Travail durable %s echoue (%s).", lease.job.id, type(exc).__name__)
            try:
                updated = await task_queue.fail(lease, error_code="worker_error")
                if lease.job.kind == "run_task":
                    payload = _job_payload(lease.job)
                    if isinstance(payload.get("task_id"), int):
                        failed_task_id = payload["task_id"]
                        execution = None
                        if (
                            isinstance(lease.job.payload, dict)
                            and lease.job.payload.get("automation_id")
                        ):
                            async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
                                db.row_factory = aiosqlite.Row
                                async with db.execute(
                                    "SELECT id,iteration FROM tasks WHERE source_job_id=?",
                                    (lease.job.id,),
                                ) as cursor:
                                    execution = await cursor.fetchone()
                            if execution:
                                failed_task_id = int(execution["id"])
                        next_status = (
                            "queued" if updated.state is JobState.QUEUED else "failed"
                        )
                        await db_update_task(failed_task_id, status=next_status)
                        if updated.state is JobState.FAILED:
                            await emit(
                                failed_task_id, 0, "systeme", "error",
                                {"msg": "La tache a echoue apres plusieurs tentatives."},
                            )
                            channel = payload.get("notification_channel")
                            if channel and execution:
                                with suppress(Exception):
                                    await enqueue_notification(
                                        task_id=failed_task_id,
                                        iteration=int(execution["iteration"] or 0),
                                        agent="systeme",
                                        channel=channel,
                                        text=(
                                            "Orchestrator : la tache planifiee "
                                            f"#{failed_task_id} a echoue apres plusieurs tentatives."
                                        ),
                                    )
            except LeaseLost:
                pass
            except Exception as queue_exc:
                log.warning(
                    "Echec d'enregistrement du travail %s (%s).",
                    lease.job.id, type(queue_exc).__name__,
                )


async def _scheduler_loop() -> None:
    assert automation_store is not None
    assert task_queue is not None
    assert _runtime_shutdown is not None
    while not _runtime_shutdown.is_set():
        try:
            await automation_store.dispatch_due(task_queue, limit=100)
        except Exception as exc:
            log.warning("Planificateur indisponible (%s).", type(exc).__name__)
        try:
            await asyncio.wait_for(_runtime_shutdown.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


async def _reconcile_durable_tasks() -> None:
    if task_queue is None:
        return
    for task in await db_list_tasks():
        if task.get("status") not in {"queued", "paused"}:
            continue
        job = await task_queue.get(task.get("queue_job_id")) if task.get("queue_job_id") else None
        if job and job.state in {JobState.QUEUED, JobState.RUNNING, JobState.PAUSED}:
            if task.get("status") == "paused" and job.state is not JobState.PAUSED:
                await task_queue.pause(job.id)
            elif task.get("status") != "paused" and job.state is JobState.PAUSED:
                await task_queue.resume(job.id)
            continue
        job = await enqueue_task_run(task["id"], resume=True, source="restart")
        if task.get("status") == "paused":
            await task_queue.pause(job.id)
            await db_update_task(task["id"], status="paused")


async def start_runtime_services() -> None:
    global task_queue, automation_store, messaging_gateway
    global _runtime_shutdown, _runtime_background_tasks
    task_queue = DurableQueue(DB_PATH)
    automation_store = AutomationStore(DB_PATH)
    await task_queue.init()
    await automation_store.init()
    await agent_skills.init_agent_skills_db(db_path=DB_PATH)
    await task_queue.recover_expired_leases()

    try:
        registry = ServerChannelRegistry.from_environment(os.environ)
    except MessagingConfigurationError as exc:
        log.error("Notifications desactivees : %s", exc)
        registry = ServerChannelRegistry.from_environment({})
    messaging_gateway = MessagingGateway(registry)
    runtime = get_sandbox_runtime()
    cleanup = await runtime.stop_all_managed(strict=False)
    if cleanup.failed:
        details = "; ".join(f"{item}: {reason}" for item, reason in cleanup.failed)
        log.error("Nettoyage Docker initial incomplet : %s", details)
        runtime.block_execution(
            "Le bac a sable Docker est bloque car d'anciens conteneurs n'ont "
            "pas pu etre verifies ou supprimes. Redemarrez Docker Desktop puis "
            "l'Orchestrateur."
        )
    else:
        runtime.unblock_execution()
    await _reconcile_durable_tasks()

    # L'API de planification est separee pour rester testable sans lancer les
    # workers. Sa configuration ne contient aucun secret client.
    try:
        import automation_api
        automation_api.configure(
            store=automation_store,
            queue=task_queue,
            channel_names=registry.channel_names,
            task_lookup=db_get_task,
        )
    except (ImportError, AttributeError):
        pass

    _runtime_shutdown = asyncio.Event()
    workers = int(_env_number(
        "ORCHESTRATOR_QUEUE_WORKERS", 2, minimum=1, maximum=8
    ))
    _runtime_background_tasks = [
        asyncio.create_task(_queue_worker(index), name=f"queue-worker-{index}")
        for index in range(1, workers + 1)
    ]
    _runtime_background_tasks.append(
        asyncio.create_task(_scheduler_loop(), name="automation-scheduler")
    )


async def stop_runtime_services() -> None:
    global _runtime_background_tasks, _runtime_shutdown
    if _runtime_shutdown is not None:
        _runtime_shutdown.set()
    for background in _runtime_background_tasks:
        background.cancel()
    if _runtime_background_tasks:
        await asyncio.gather(*_runtime_background_tasks, return_exceptions=True)
    _runtime_background_tasks = []
    # Les baux restent en base et expireront ; les taches ne sont pas perdues.
    runtime = sandbox_runtime
    if runtime is not None:
        cleanup = await runtime.stop_all_managed(strict=False)
        if cleanup.failed:
            details = "; ".join(f"{item}: {reason}" for item, reason in cleanup.failed)
            log.error("Nettoyage Docker final incomplet : %s", details)
            runtime.block_execution(
                "Le nettoyage Docker est incomplet ; aucune nouvelle execution "
                "Docker n'est autorisee avant un redemarrage propre."
            )
    launched_apps.clear()
    _runtime_shutdown = None


async def db_create_install_request(record: dict):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "INSERT INTO install_requests "
            "(id,task_id,iteration,agent,plan_hash,manager,package,version,scope,access_level,"
            "reason,source,allow_scripts,status,decision,result,created_at,expires_at,resolved_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record["id"], record["task_id"], record.get("iteration", 0),
                record.get("agent", ""), record["plan_hash"], record["manager"],
                record["package"], record["version"], record["scope"],
                record["access_level"], record.get("reason", ""),
                record.get("source"), 1 if record.get("allow_scripts") else 0, record["status"],
                record.get("decision"), record.get("result"), record["created_at"],
                record.get("expires_at"), record.get("resolved_at"),
            ),
        )
        await db.commit()


async def db_get_install_request(request_id: str):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM install_requests WHERE id=?", (request_id,)
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def db_list_install_requests(task_id: int, status: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        if status:
            query = "SELECT * FROM install_requests WHERE task_id=? AND status=? ORDER BY created_at DESC"
            params = (task_id, status)
        else:
            query = "SELECT * FROM install_requests WHERE task_id=? ORDER BY created_at DESC LIMIT 100"
            params = (task_id,)
        async with db.execute(query, params) as cur:
            return [dict(row) for row in await cur.fetchall()]


async def db_resolve_install_request(task_id: int, request_id: str, decision: str) -> bool:
    status = "denied" if decision == "deny" else "approved"
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "UPDATE install_requests SET status=?, decision=?, resolved_at=? "
            "WHERE id=? AND task_id=? AND status='pending'",
            (status, decision, datetime.utcnow().isoformat(), request_id, task_id),
        )
        await db.commit()
        return cur.rowcount == 1


async def db_finish_install_request(request_id: str, ok: bool, result: str):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "UPDATE install_requests SET status=?, result=?, resolved_at=? WHERE id=?",
            (
                "succeeded" if ok else "failed",
                (result or "")[:2000],
                datetime.utcnow().isoformat(),
                request_id,
            ),
        )
        await db.commit()


async def db_cancel_install_request(request_id: str, result: str):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute(
            "UPDATE install_requests SET status='cancelled', result=?, resolved_at=? WHERE id=?",
            ((result or "")[:2000], datetime.utcnow().isoformat(), request_id),
        )
        await db.commit()


async def db_expire_install_request(request_id: str, decision: str = "expired"):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "UPDATE install_requests SET status='expired', decision=?, resolved_at=? "
            "WHERE id=? AND status='pending'",
            (decision, datetime.utcnow().isoformat(), request_id),
        )
        await db.commit()
        return cur.rowcount == 1


async def db_cancel_pending_install_requests(task_id: int, decision: str = "task_stopped"):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        cur = await db.execute(
            "UPDATE install_requests SET status='cancelled', decision=?, resolved_at=? "
            "WHERE task_id=? AND status='pending'",
            (decision, datetime.utcnow().isoformat(), task_id),
        )
        await db.commit()
        return cur.rowcount


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
    ".venv", "venv", ".orchestrator", "node_modules", "__pycache__", ".pytest_cache", ".git")


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
_WORKSPACE_IGNORE_DIRS = {
    ".git", ".venv", "venv", ".orchestrator", "node_modules",
    "__pycache__", ".pytest_cache",
}
_NPM_MUTATING_COMMANDS = {
    "install", "i", "add", "uninstall", "remove", "rm", "update", "upgrade",
    "ci", "install-ci-test", "install-test", "rebuild", "dedupe", "prune",
    "link", "config", "exec", "pkg", "init",
}
INSTALL_POLICY_VALUES = frozenset({"blocked", "ask", "project", "user", "admin"})


def _task_venv_python(folder: str | Path) -> Path:
    venv = Path(folder) / ".orchestrator" / "python"
    subdir = "Scripts" if os.name == "nt" else "bin"
    return venv / subdir / ("python.exe" if os.name == "nt" else "python")


def _local_python_executable(folder: str | Path) -> Optional[str]:
    task_python = _task_venv_python(folder)
    if task_python.exists():
        return str(task_python)
    if getattr(sys, "frozen", False):
        return None
    return sys.executable


def _task_npm_root(folder: str | Path) -> Path:
    return Path(folder) / ".orchestrator" / "npm"


def _ensure_task_private_directory(folder: str | Path, name: str) -> Path:
    base = Path(folder).resolve()
    private = base / ".orchestrator"
    if private.exists() and (
        private.is_symlink() or private.resolve().parent != base
    ):
        raise ValueError("Dossier prive de la tache non sur.")
    private.mkdir(exist_ok=True)
    target = private / name
    if target.exists() and (
        target.is_symlink() or target.resolve().parent != private.resolve()
    ):
        raise ValueError("Destination privee de la tache non sure.")
    target.mkdir(exist_ok=True)
    return target


def _task_child_environment(folder: str | Path) -> dict[str, str]:
    """Ajoute les paquets npm isoles de la tache sans exposer les secrets."""
    env = build_child_environment()
    root = _task_npm_root(folder)
    node_paths = []
    bin_paths = []
    if root.is_dir():
        base = Path(folder).resolve()
        if root.is_symlink() or base not in root.resolve().parents:
            return env
        root_resolved = root.resolve()
        for candidate in sorted(root.iterdir()):
            if not candidate.is_dir() or candidate.name.startswith(".tmp-"):
                continue
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            if root_resolved not in resolved.parents:
                continue
            modules = resolved / "node_modules"
            if modules.is_dir():
                node_paths.append(str(modules))
                binaries = modules / ".bin"
                if binaries.is_dir():
                    bin_paths.append(str(binaries))
    if node_paths:
        env["NODE_PATH"] = os.pathsep.join(node_paths)
    if bin_paths:
        path_key = next((key for key in env if key.upper() == "PATH"), "PATH")
        env[path_key] = os.pathsep.join([env.get(path_key, "")] + bin_paths)
    return env


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
        rel = p.relative_to(base)
        if p.is_file() and not any(part in _WORKSPACE_IGNORE_DIRS for part in rel.parts):
            out.append(str(rel))
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

    path = Path(path)
    if not path.exists():
        return True

    def _onerror(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass
    shutil.rmtree(path, onerror=_onerror)
    return not path.exists()


async def _clone_repo(url, token=None):
    """Clone un depot GitHub en LECTURE SEULE dans un cache local, et renvoie son chemin.
    Le token (depot prive) sert au clone puis est scrubbe (remote supprime) ; jamais stocke/log."""
    url = validate_github_repo_url(url)
    m = re.search(r"github\.com/([^/]+)/([^/]+?)(?:\.git)?$", url)
    name = (m.group(1) + "_" + m.group(2)) if m else re.sub(r"[^A-Za-z0-9_.-]", "_", url)[-40:]
    dest = PROJECTS / ".repos" / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        ok = await asyncio.to_thread(_rmtree_force, str(dest))  # re-clone propre (gere .git read-only Windows)
        if not ok and dest.exists():
            # dernier recours : cloner dans un dossier voisin unique
            dest = PROJECTS / ".repos" / (name + "_" + uuid.uuid4().hex[:6])
    clone_env = build_child_environment()
    clone_env["GIT_TERMINAL_PROMPT"] = "0"
    credentials = None
    if token:
        # Git reads this one-shot config from the child environment. The token
        # is therefore absent from both the process command line and remote URL.
        credentials = base64.b64encode(
            ("x-access-token:" + token).encode("utf-8")
        ).decode("ascii")
        clone_env.update({
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.extraHeader",
            "GIT_CONFIG_VALUE_0": "Authorization: Basic " + credentials,
        })
    proc = await asyncio.create_subprocess_exec(
        "git", "clone", "--depth", "1", url, str(dest),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env=clone_env)
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
        if credentials:
            msg = msg.replace(credentials, "***")
        raise ValueError(msg.strip()[-300:] or "echec inconnu")
    # Scrubber le remote : aucune trace du token dans .git/config.
    if (dest / ".git").exists():
        try:
            p2 = await asyncio.create_subprocess_exec(
                "git", "-C", str(dest), "remote", "remove", "origin",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                env=build_child_environment())
            await p2.communicate()
        except Exception:
            pass
    return str(dest.resolve())


def _sandbox_task_environment(folder: str | Path) -> dict[str, str]:
    base = Path(folder)
    env = {
        "PYTHONPATH": "/workspace/.orchestrator/python-packages",
        "PYTHONUNBUFFERED": "1",
        "NO_COLOR": "1",
    }
    npm_root = base / ".orchestrator" / "npm"
    node_paths = []
    if npm_root.is_dir():
        for child in sorted(npm_root.iterdir(), key=lambda item: item.name):
            if child.is_dir() and (child / "node_modules").is_dir():
                node_paths.append(f"/workspace/.orchestrator/npm/{child.name}/node_modules")
    if node_paths:
        env["NODE_PATH"] = ":".join(node_paths)[:4096]
    return env


async def _task_uses_docker(task_id: Optional[int]) -> bool:
    if task_id is None:
        return False
    task = await db_get_task(task_id)
    return bool(task and task.get("execution_mode") == "docker")


async def _run_cmd(
    folder: str, cmd: str, timeout: int = 60, *, task_id: Optional[int] = None
) -> str:
    try:
        parts = shlex.split(cmd or "")
    except ValueError:
        return "Commande invalide (guillemets non fermes)."
    if not parts:
        return "Commande vide."
    executable = Path(parts[0]).name.lower()
    for suffix in (".exe", ".cmd", ".bat"):
        if executable.endswith(suffix):
            executable = executable[:-len(suffix)]
            break
    if executable in {"pip", "pip3"}:
        return "Installation directe refusee. Utilise l'outil install_package."
    normalized_args = [str(part).lower() for part in parts[1:]]
    if executable in {"python", "python3"} and any(
        normalized_args[index:index + 2] in (["-m", "pip"], ["-m", "ensurepip"])
        for index in range(max(0, len(normalized_args) - 1))
    ):
        return "Installation directe refusee. Utilise l'outil install_package."
    if executable == "npm" and any(
        argument in _NPM_MUTATING_COMMANDS for argument in normalized_args
    ):
        return "Modification npm directe refusee. Utilise l'outil install_package."
    if executable not in COMMAND_WHITELIST:
        return "Commande refusee (hors liste blanche): " + parts[0]
    if await _task_uses_docker(task_id):
        if executable == "pytest":
            parts = ["python", "-m", "pytest"] + parts[1:]
        elif executable in {"python", "python3"}:
            parts[0] = "python"
        else:
            parts[0] = executable
        try:
            result = await get_sandbox_runtime().run_command(
                task_id,
                folder,
                tuple(parts),
                timeout=timeout,
                env=_sandbox_task_environment(folder),
            )
        except SandboxError as exc:
            return "Execution isolee refusee : " + str(exc)[:1200]
        suffix = "\n(code de sortie " + str(result.exit_code) + ")" if not result.ok else ""
        return ((result.output or "(vide)") + suffix)[:2500]
    local_python = _local_python_executable(folder)
    if executable in {"python", "python3"}:
        if not local_python:
            return "Mode local indisponible : aucun interpreteur Python externe n'est configure."
        parts[0] = local_python
    elif executable == "pytest":
        if not local_python:
            return "Tests locaux indisponibles : aucun interpreteur Python externe n'est configure."
        parts = [local_python, "-m", "pytest"] + parts[1:]
    else:
        trusted_executable = shutil.which(executable)
        if not trusted_executable:
            return "Programme introuvable: " + executable
        parts[0] = trusted_executable
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *parts, cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=_task_child_environment(folder),
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


async def _run_pytest(
    folder: str, timeout: int = 120, *, task_id: Optional[int] = None
) -> str:
    if await _task_uses_docker(task_id):
        try:
            result = await get_sandbox_runtime().run_tests(
                task_id,
                folder,
                ("python", "-m", "pytest", "-q"),
                timeout=timeout,
                env=_sandbox_task_environment(folder),
            )
        except SandboxError as exc:
            return "Tests isoles refuses : " + str(exc)[:1200]
        suffix = "\n(code de sortie " + str(result.exit_code) + ")" if not result.ok else ""
        return ((result.output or "(vide)") + suffix)[:2500]
    proc = None
    try:
        python_exe = _local_python_executable(folder)
        if not python_exe:
            return "Tests locaux indisponibles : aucun interpreteur Python externe n'est configure."
        proc = await asyncio.create_subprocess_exec(
            python_exe, "-m", "pytest", "-q", cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=build_child_environment(),
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


INSTALL_REQUEST_TIMEOUT_SECONDS = 300
INSTALL_PAYLOAD_FIELDS = (
    "manager", "package", "version", "scope", "allow_scripts", "source",
)


def _install_destination(folder: str, plan, execution_mode: str = "local") -> str:
    if plan.manager.value == "python":
        if execution_mode == "docker":
            return str(Path(folder) / ".orchestrator" / "python-packages")
        return str(Path(folder) / ".orchestrator" / "python")
    if plan.manager.value == "npm":
        return str(_task_npm_root(folder) / plan.plan_hash)
    return "votre compte Windows" if plan.access is AccessLevel.USER else "tout l'ordinateur"


def _plan_from_install_row(row: dict):
    payload = {
        "manager": row["manager"],
        "package": row["package"],
        "version": row["version"],
        "scope": row["scope"],
        "allow_scripts": bool(row.get("allow_scripts")),
    }
    if row.get("source"):
        payload["source"] = row["source"]
    return make_install_plan(payload)


def _install_request_payload(row: dict) -> dict:
    plan = _plan_from_install_row(row)
    return {
        "request_id": row["id"],
        "task_id": row["task_id"],
        "iteration": row.get("iteration", 0),
        "agent": row.get("agent") or "Agent",
        "manager": plan.manager.value,
        "package": plan.package,
        "version": plan.version,
        "scope": plan.scope,
        "source": plan.source or (
            "PyPI (sauf politique globale de la machine)"
            if plan.manager.value == "python" else "npm officiel"
        ),
        "access_level": plan.access.value,
        "reason": row.get("reason") or "Cette installation est necessaire a la tache.",
        "risk_reason": plan.reason,
        "allow_scripts": plan.allow_scripts,
        "destination": None,
        "command_preview": list(plan.command_preview),
        "status": row.get("status"),
        "expires_at": row.get("expires_at"),
    }


async def _install_payload_for_task(row: dict) -> dict:
    payload = _install_request_payload(row)
    task = await db_get_task(row["task_id"])
    payload["destination"] = _install_destination(
        task["folder"] if task else "",
        _plan_from_install_row(row),
        task.get("execution_mode", "local") if task else "local",
    )
    return payload


async def _terminate_install_process(proc) -> None:
    """Arrete au mieux le processus d'installation et ses enfants."""
    if not proc or proc.returncode is not None:
        return
    if os.name == "nt":
        system_root = Path(os.environ.get("SystemRoot", r"C:\Windows"))
        taskkill = system_root / "System32" / "taskkill.exe"
        if taskkill.is_file():
            try:
                killer = await asyncio.create_subprocess_exec(
                    str(taskkill), "/PID", str(proc.pid), "/T", "/F",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=build_child_environment(),
                )
                await asyncio.wait_for(killer.wait(), timeout=10)
            except Exception:
                pass
    else:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    if proc.returncode is None:
        try:
            proc.kill()
        except (ProcessLookupError, PermissionError):
            pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except (asyncio.TimeoutError, ProcessLookupError):
        pass


async def _run_install_process(task_id: int, argv: tuple[str, ...], folder: str,
                               timeout: int, env_overrides: Optional[dict] = None
                               ) -> tuple[bool, str]:
    proc = None
    ctrl = running_tasks.get(task_id)
    if ctrl is None:
        return False, "La tache n'est plus active."
    control_lock = ctrl["permission_lock"]
    tail = bytearray()

    async def _read_bounded_output():
        while True:
            chunk = await proc.stdout.read(4096)
            if not chunk:
                break
            tail.extend(chunk)
            if len(tail) > 65536:
                del tail[:-65536]
        await proc.wait()

    try:
        async with control_lock:
            current = await db_get_task(task_id)
            status = current.get("status") if current else None
            if status not in {"running", "paused"} or (
                status == "paused" and not ctrl["pause"].is_set()
            ):
                return False, "La tache a ete arretee ou mise en pause avant l'installation."
            child_env = build_child_environment()
            child_env.update(env_overrides or {})
            process_options = {"start_new_session": True} if os.name != "nt" else {}
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=folder,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                env=child_env, **process_options,
            )
            ctrl["install_proc"] = proc
        await asyncio.wait_for(_read_bounded_output(), timeout=timeout)
        rendered = tail.decode("utf-8", errors="replace")
        return proc.returncode == 0, (rendered or "(aucune sortie)")[-4000:]
    except FileNotFoundError:
        return False, "Programme d'installation introuvable : " + str(argv[0])
    except asyncio.TimeoutError:
        await _terminate_install_process(proc)
        return False, "Delai d'installation depasse."
    except asyncio.CancelledError:
        await _terminate_install_process(proc)
        raise
    finally:
        async with control_lock:
            if ctrl.get("install_proc") is proc:
                ctrl.pop("install_proc", None)


async def _ensure_task_python_venv(task_id: int, folder: str) -> tuple[bool, str]:
    try:
        private = _ensure_task_private_directory(folder, "python")
    except ValueError as exc:
        return False, str(exc)
    python_exe = _task_venv_python(folder)
    if python_exe.exists():
        return True, str(python_exe)
    if getattr(sys, "frozen", False):
        return False, (
            "Le mode local du paquet desktop necessite un Python externe. "
            "Utilisez le mode Docker recommande."
        )
    venv_dir = private
    ok, output = await _run_install_process(
        task_id,
        (sys.executable, "-I", "-m", "venv", str(venv_dir)),
        folder,
        120,
    )
    if not ok or not python_exe.exists():
        return False, "Creation de l'environnement Python impossible : " + output[-1200:]
    return True, str(python_exe)


async def _execute_install_plan(task_id: int, folder: str, plan) -> tuple[bool, str]:
    task = await db_get_task(task_id)
    if task and task.get("execution_mode") == "docker" and plan.manager.value in {"python", "npm"}:
        if not sandbox_install_network_allowed():
            return False, (
                "Le reseau d'installation Docker est desactive par l'administrateur "
                "(ORCHESTRATOR_SANDBOX_ALLOW_INSTALL_NETWORK)."
            )
        runtime = get_sandbox_runtime()
        base = Path(folder).resolve()
        try:
            if plan.manager.value == "python":
                destination = _ensure_task_private_directory(base, "python-packages")
                argv = (
                    "python", "-m", "pip", "install", "--isolated",
                    "--only-binary=:all:", "--no-input",
                    "--disable-pip-version-check",
                    "--index-url", "https://pypi.org/simple",
                    "--target", "/workspace/.orchestrator/python-packages",
                    f"{plan.package}=={plan.version}",
                )
                result = await runtime.run_command(
                    task_id, base, argv, timeout=300,
                    env={"NO_COLOR": "1"}, network_enabled=True,
                )
                return result.ok, (result.output or "(aucune sortie)")[-4000:]

            npm_root = _ensure_task_private_directory(base, "npm")
            staging = npm_root / (".tmp-" + uuid.uuid4().hex)
            destination = npm_root / plan.plan_hash
            staging.mkdir()
            (staging / "package.json").write_text(
                json.dumps({"name": "orchestrator-task-package", "private": True,
                            "version": "1.0.0"}),
                encoding="utf-8",
            )
            container_prefix = "/workspace/.orchestrator/npm/" + staging.name
            argv = build_install_argv(
                plan, npm_executable="npm", npm_prefix=container_prefix,
            )
            result = await runtime.run_command(
                task_id, base, argv, timeout=300,
                env={"NO_COLOR": "1"}, network_enabled=True,
            )
            if result.ok:
                if destination.exists():
                    await asyncio.to_thread(_rmtree_force, destination)
                staging.replace(destination)
            elif staging.exists():
                await asyncio.to_thread(_rmtree_force, staging)
            return result.ok, (result.output or "(aucune sortie)")[-4000:]
        except SandboxError as exc:
            return False, "Installation isolee refusee : " + str(exc)[:1200]
        except (OSError, ValueError) as exc:
            return False, "Installation isolee impossible : " + str(exc)[:1200]

    if plan.manager.value == "python":
        ok, python_or_error = await _ensure_task_python_venv(task_id, folder)
        if not ok:
            return False, python_or_error
        argv = build_install_argv(plan, venv_python=python_or_error)
        timeout = 300
    elif plan.manager.value == "npm":
        npm = shutil.which("npm") or shutil.which("npm.cmd")
        if not npm:
            return False, "npm est absent de cet ordinateur."
        try:
            npm_root = _ensure_task_private_directory(folder, "npm")
        except ValueError as exc:
            return False, str(exc)
        staging = npm_root / (".tmp-" + uuid.uuid4().hex)
        staging.mkdir()
        (staging / "package.json").write_text(
            json.dumps({"name": "orchestrator-task-package", "private": True,
                        "version": "1.0.0"}),
            encoding="utf-8",
        )
        (staging / ".npmrc").write_text("", encoding="utf-8")
        argv = build_install_argv(plan, npm_executable=npm, npm_prefix=staging)
        if os.name == "nt" or Path(npm).suffix.lower() in {".cmd", ".bat"}:
            node = shutil.which("node") or shutil.which("node.exe")
            npm_cli = Path(npm).resolve().parent / "node_modules" / "npm" / "bin" / "npm-cli.js"
            if not node or not npm_cli.is_file():
                await asyncio.to_thread(shutil.rmtree, staging)
                return False, "L'executable npm securise est introuvable."
            argv = (str(Path(node).resolve()), str(npm_cli)) + argv[1:]
        npm_env = {
            "NPM_CONFIG_USERCONFIG": os.devnull,
            "NPM_CONFIG_GLOBALCONFIG": os.devnull,
            "NPM_CONFIG_REGISTRY": "https://registry.npmjs.org/",
            "NPM_CONFIG_GLOBAL": "false",
            "NPM_CONFIG_IGNORE_SCRIPTS": "true",
            "NODE_OPTIONS": "",
            "NODE_PATH": "",
        }
        ok, output = await _run_install_process(
            task_id, argv, str(staging), 300, env_overrides=npm_env
        )
        destination = npm_root / plan.plan_hash

        def _replace_environment():
            root_resolved = npm_root.resolve()
            if staging.resolve().parent != root_resolved:
                raise ValueError("Dossier npm temporaire invalide.")
            if destination.exists():
                if destination.is_symlink() or root_resolved not in destination.resolve().parents:
                    raise ValueError("Destination npm non sure.")
                shutil.rmtree(destination)
            if ok:
                staging.replace(destination)
            elif staging.exists():
                shutil.rmtree(staging)

        try:
            await asyncio.to_thread(_replace_environment)
        except Exception as exc:
            return False, output + NL + "Finalisation npm impossible : " + str(exc)[:300]
        return ok, output
    else:
        winget = shutil.which("winget") or shutil.which("winget.exe")
        if not winget:
            return False, "Le gestionnaire d'applications Windows (winget) est absent."
        argv = build_install_argv(plan, winget_executable=winget)
        timeout = 900
    return await _run_install_process(task_id, argv, folder, timeout)


def _read_installed_dependency_manifest(folder: str) -> dict:
    base = Path(folder).resolve()
    path = base / "orchestrator-dependencies.json"
    if path.exists() and (path.is_symlink() or base not in path.resolve().parents):
        return {}
    manifest = {"schema": 1, "python": {}, "npm": {}, "applications": {}}
    if path.is_file():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing, dict) and existing.get("schema") == 1:
                for key in ("python", "npm", "applications"):
                    if isinstance(existing.get(key), dict):
                        manifest[key] = existing[key]
        except (OSError, json.JSONDecodeError):
            pass
    return manifest


def _record_installed_dependency(folder: str, plan) -> None:
    """Ecrit un manifeste reproductible des demandes directes installees."""
    base = Path(folder).resolve()
    path = base / "orchestrator-dependencies.json"
    manifest = _read_installed_dependency_manifest(folder)
    if not manifest:
        raise ValueError("Manifeste de dependances non sur.")
    if plan.manager.value in {"python", "npm"}:
        manifest[plan.manager.value][plan.package] = plan.version
    else:
        manifest["applications"][plan.package] = {
            "version": plan.version,
            "scope": plan.scope,
            "source": plan.source,
        }
    temporary = path.with_suffix(".json.tmp")
    if temporary.exists() and (
        temporary.is_symlink() or base not in temporary.resolve().parents
    ):
        raise ValueError("Fichier temporaire de dependances non sur.")
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + NL,
        encoding="utf-8",
    )
    temporary.replace(path)


async def _tool_install_package(task_id: int, iteration: int, folder: str,
                                act: dict, agent_name: Optional[str]) -> str:
    task = await db_get_task(task_id)
    if not task:
        return "Tache introuvable."
    raw_payload = {key: act[key] for key in INSTALL_PAYLOAD_FIELDS if key in act}
    try:
        plan = make_install_plan(raw_payload)
    except InstallValidationError as exc:
        return "Demande d'installation refusee : " + str(exc)

    agent_reason = str(act.get("reason") or "").strip()[:500]
    decision = evaluate_install(plan, task.get("install_policy") or "ask")
    request_id = uuid.uuid4().hex
    created_at = datetime.utcnow()
    expires_at = created_at + timedelta(seconds=INSTALL_REQUEST_TIMEOUT_SECONDS)
    record = {
        "id": request_id,
        "task_id": task_id,
        "iteration": iteration,
        "agent": agent_name or "Agent",
        "plan_hash": plan.plan_hash,
        "manager": plan.manager.value,
        "package": plan.package,
        "version": plan.version,
        "scope": plan.scope,
        "access_level": plan.access.value,
        "reason": agent_reason,
        "source": plan.source,
        "allow_scripts": plan.allow_scripts,
        "status": (
            "denied" if decision.outcome is DecisionOutcome.DENIED
            else "approved" if decision.outcome is DecisionOutcome.AUTOMATIC
            else "pending"
        ),
        "decision": decision.outcome.value if decision.outcome is not DecisionOutcome.PROMPT else None,
        "result": decision.reason if decision.outcome is DecisionOutcome.DENIED else None,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat() if decision.outcome is DecisionOutcome.PROMPT else None,
        "resolved_at": created_at.isoformat() if decision.outcome is not DecisionOutcome.PROMPT else None,
    }
    if decision.outcome is DecisionOutcome.DENIED:
        await db_create_install_request(record)
        await emit(task_id, iteration, agent_name or "Agent", "permission_resolved", {
            "request_id": request_id, "decision": "denied", "message": decision.reason,
        })
        return "Installation refusee : " + decision.reason

    if decision.outcome is DecisionOutcome.PROMPT:
        ctrl = running_tasks.get(task_id)
        if ctrl is None:
            return "Installation annulee : la tache n'est plus active."
        control_lock = ctrl["permission_lock"]
        future = asyncio.get_running_loop().create_future()
        async with control_lock:
            current = await db_get_task(task_id)
            if not current or current.get("status") == "stopped":
                return "Installation annulee : la tache est arretee."
            pending = await db_list_install_requests(task_id, status="pending")
            if len(pending) >= 10:
                return "Installation refusee : trop de demandes sont deja en attente."
            if any(row.get("plan_hash") == plan.plan_hash for row in pending):
                return "Une demande identique est deja en attente."
            install_waiters[request_id] = future
            install_waiter_tasks[request_id] = task_id
            try:
                await db_create_install_request(record)
                request_payload = await _install_payload_for_task(record)
                await emit(
                    task_id, iteration, agent_name or "Agent",
                    "permission_requested", request_payload,
                )
            except Exception:
                install_waiters.pop(request_id, None)
                install_waiter_tasks.pop(request_id, None)
                await db_expire_install_request(request_id, decision="display_failed")
                raise
        try:
            user_decision = await asyncio.wait_for(
                asyncio.shield(future), timeout=INSTALL_REQUEST_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            async with control_lock:
                changed = await db_expire_install_request(request_id)
                if changed:
                    if not future.done():
                        future.set_result("expired")
                    try:
                        await emit(task_id, iteration, agent_name or "Agent",
                                   "permission_resolved", {
                                       "request_id": request_id,
                                       "decision": "expired",
                                       "message": "La demande a expire.",
                                   })
                    except Exception as exc:
                        log.warning("Expiration d'installation non emise: %s", exc)
                    user_decision = "expired"
                else:
                    user_decision = future.result() if future.done() else "expired"
        finally:
            async with control_lock:
                install_waiters.pop(request_id, None)
                install_waiter_tasks.pop(request_id, None)
        if user_decision != "allow_once":
            return "Installation refusee par l'utilisateur."
    else:
        await db_create_install_request(record)

    ctrl = running_tasks.get(task_id)
    if ctrl is None:
        await db_cancel_install_request(request_id, "Tache inactive.")
        return "Installation annulee : la tache n'est plus active."
    await _wait_if_paused(task_id)
    if await _is_stopped(task_id):
        await db_cancel_install_request(request_id, "Tache arretee avant l'installation.")
        return "Installation annulee : la tache est arretee."
    install_lock = ctrl["install_lock"]
    async with install_lock:
        try:
            ok, output = await _execute_install_plan(task_id, folder, plan)
        except asyncio.CancelledError:
            await db_finish_install_request(request_id, False, "Installation annulee.")
            raise
        except Exception as exc:
            ok, output = False, "Erreur d'installation : " + str(exc)[:1000]

    if ok:
        try:
            await asyncio.to_thread(_record_installed_dependency, folder, plan)
        except Exception as exc:
            log.warning("Manifeste de dependances non ecrit: %s", exc)

    await db_finish_install_request(request_id, ok, output)
    message = (
        "Installation terminee : " + plan.display
        if ok else "Installation echouee : " + plan.display
    )
    await emit(task_id, iteration, agent_name or "Agent", "installation_result", {
        "request_id": request_id, "ok": ok, "message": message,
    })
    return message + NL + output[-1800:]


async def web_search(query: str) -> str:
    # Cache : une meme requete (a la casse/espaces pres) ne repaye pas un appel SDK.
    key = " ".join((query or "").lower().split())
    if key and key in _web_cache:
        return _web_cache[key] + NL + "(resultat en cache)"
    try:
        from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions, ResultMessage
    except ImportError:
        return "Recherche web indisponible (claude-agent-sdk non installe)."
    out = []
    try:
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
    except Exception as e:
        return "Erreur recherche web: " + str(e)[:200]
    result = (NL.join(out) or "Aucun resultat.")[:3000]
    if key and not result.startswith("Erreur"):
        if len(_web_cache) >= _WEB_CACHE_MAX:
            _web_cache.pop(next(iter(_web_cache)), None)  # evince la plus ancienne entree
        _web_cache[key] = result
    return result


# ── MCP : serveurs OFFICIELS uniquement (liste blanche) ──────────────────────
# Verifie cote serveur : un agent ne peut activer qu'un serveur de cette liste
# (serveurs de reference du projet Model Context Protocol).
OFFICIAL_MCP = {
    # Les paquets doivent etre installes explicitement via install_package.
    "fetch": {"kind": "python", "pip": "mcp-server-fetch", "module": "mcp_server_fetch"},
    "git": {"kind": "python", "pip": "mcp-server-git", "module": "mcp_server_git",
            "args": ["--repository", "{folder}"]},
    "time": {"kind": "python", "pip": "mcp-server-time", "module": "mcp_server_time"},
    # Node : lance uniquement les executables deja presents dans l'environnement isole.
    "filesystem": {"kind": "npx", "package": "@modelcontextprotocol/server-filesystem",
                   "bin": "mcp-server-filesystem", "args": ["{folder}"]},
    "memory": {"kind": "npx", "package": "@modelcontextprotocol/server-memory",
               "bin": "mcp-server-memory"},
    "sequentialthinking": {"kind": "npx", "package": "@modelcontextprotocol/server-sequential-thinking",
                           "bin": "mcp-server-sequential-thinking"},
    "everything": {"kind": "npx", "package": "@modelcontextprotocol/server-everything",
                   "bin": "mcp-server-everything"},
}


async def _ensure_mcp_command(name, folder):
    """Construit la commande sans jamais telecharger silencieusement un serveur."""
    spec = OFFICIAL_MCP[name]
    args = [a.replace("{folder}", folder) for a in spec.get("args", [])]
    installed = _read_installed_dependency_manifest(folder)
    if spec["kind"] == "python":
        task_version = (installed.get("python") or {}).get(spec["pip"])
        candidates = []
        if task_version:
            candidates.append((_task_venv_python(folder), task_version))
        if not getattr(sys, "frozen", False):
            candidates.append((Path(sys.executable), None))
        for candidate, expected_version in candidates:
            if not candidate.exists():
                continue
            proc = await asyncio.create_subprocess_exec(
                str(candidate), "-c",
                "import importlib.metadata as m,importlib.util,sys;"
                "v=m.version(sys.argv[1]);"
                "sys.exit(0 if importlib.util.find_spec(sys.argv[3]) and "
                "(not sys.argv[2] or v==sys.argv[2]) else 1)",
                spec["pip"], expected_version or "", spec["module"],
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL, env=build_child_environment(),
                cwd=str(APP_ROOT),
            )
            await proc.communicate()
            if proc.returncode == 0:
                return [str(candidate), "-m", spec["module"]] + args
        raise RuntimeError(
            "Le serveur MCP '" + name + "' n'est pas installe. Utilise install_package "
            "avec manager=python, package=" + spec["pip"] + " et une version exacte."
        )
    expected_version = (installed.get("npm") or {}).get(spec["package"])
    if expected_version:
        plan = make_install_plan({
            "manager": "npm", "package": spec["package"], "version": expected_version,
        })
        environment = _task_npm_root(folder) / plan.plan_hash
        package_dir = environment / "node_modules" / Path(*spec["package"].split("/"))
        package_json = package_dir / "package.json"
        try:
            metadata = json.loads(package_json.read_text(encoding="utf-8"))
            binary = metadata.get("bin")
            if isinstance(binary, dict):
                binary = binary.get(spec["bin"])
            if (metadata.get("name") == spec["package"]
                    and metadata.get("version") == expected_version
                    and isinstance(binary, str)):
                executable = (package_dir / binary).resolve()
                if package_dir.resolve() in executable.parents and executable.is_file():
                    node = shutil.which("node") or shutil.which("node.exe")
                    if node:
                        return [str(Path(node).resolve()), str(executable)] + args
        except (OSError, json.JSONDecodeError):
            pass
    raise RuntimeError(
        "Le serveur MCP '" + name + "' n'est pas installe. Utilise install_package "
        "avec manager=npm, package=" + spec["package"] + " et une version exacte."
    )


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
            env=build_child_environment(),
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
        return ("Impossible de lancer le serveur MCP '" + server + "'. "
                "Installe d'abord son paquet exact avec install_package.")
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
                       agent_name: Optional[str] = None, iteration: int = 0) -> str:
    tool = act.get("tool")
    try:
        current_task = await db_get_task(task_id)
        if not current_task or current_task.get("status") != "running":
            return "Action refusee : la tache n'est plus en cours."
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
            return await _run_cmd(folder, act.get("cmd", ""), task_id=task_id)
        if tool == "run_tests":
            return await _run_pytest(folder, task_id=task_id)
        if tool == "install_package":
            return await _tool_install_package(
                task_id, iteration, folder, act, agent_name
            )
        if tool == "web_search":
            if not web_enabled:
                return "Recherche web desactivee pour cette tache."
            return await web_search(act.get("query", ""))
        if tool == "add_mcp":
            if current_task.get("execution_mode") == "docker":
                return (
                    "MCP est desactive en mode Docker tant que ses processus "
                    "ne peuvent pas etre executes dans le meme bac a sable."
                )
            return await _tool_add_mcp(task_id, folder, act.get("server", ""))
        if tool == "mcp_call":
            if current_task.get("execution_mode") == "docker":
                return "Appel MCP refuse en mode Docker."
            return await _tool_mcp_call(task_id, act.get("server", ""),
                                        act.get("name") or act.get("tool_name") or "",
                                        act.get("arguments") or {})
        if tool == "save_knowledge":
            return await _save_knowledge(act.get("content", ""), agent_name=agent_name)
        if tool == "search_knowledge":
            return await _search_knowledge(act.get("query", ""), agent_name=agent_name)
        if tool == "propose_skill":
            if not agent_name:
                return "Proposition refusee : identite d'agent absente."
            skill = await agent_skills.propose_skill(
                task_id=task_id,
                agent=agent_name,
                name=act.get("name", ""),
                summary=act.get("summary", ""),
                instructions=act.get("instructions", ""),
                tags=act.get("tags") or [],
                db_path=DB_PATH,
            )
            await emit(task_id, iteration, agent_name, "skill_proposed", {
                "skill_id": skill.id,
                "name": skill.name,
                "summary": skill.summary,
                "status": skill.status.value,
            })
            return (
                f"Competence proposee (#{skill.id}). Elle restera inactive "
                "jusqu'a la validation d'un humain."
            )
        if tool == "send_notification":
            if not agent_name:
                return "Notification refusee : identite d'agent absente."
            job_id = await enqueue_notification(
                task_id=task_id,
                iteration=iteration,
                agent=agent_name,
                channel=act.get("channel", ""),
                text=act.get("text", ""),
            )
            return "Notification ajoutee a la file (travail " + job_id + ")."
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
    {{"tool":"install_package","manager":"python","package":"requests","version":"2.32.3","reason":"appeler l'API demandee"}},
    {{"tool":"install_package","manager":"npm","package":"vite","version":"5.4.8","allow_scripts":false,"reason":"construire l'interface"}},
    {{"tool":"install_package","manager":"winget","package":"Microsoft.VisualStudioCode","version":"1.95.3","scope":"user","reason":"mettre l'editeur a disposition de l'utilisateur"}},
    {{"tool":"web_search","query":"..."}},
    {{"tool":"add_mcp","server":"filesystem"}},
    {{"tool":"mcp_call","server":"filesystem","name":"list_directory","arguments":{{}}}},
    {{"tool":"search_knowledge","query":"astuce dependance windows"}},
    {{"tool":"save_knowledge","content":"Lecon apprise reutilisable pour les futures taches"}},
    {{"tool":"propose_skill","name":"Nom clair","summary":"Quand l'utiliser","instructions":"Methode documentaire reutilisable...","tags":["python"]}},
    {{"tool":"send_notification","channel":"canal-configure","text":"Le travail est termine."}}
  ],
  "done": false,
  "report":"(quand done=true) resume clair de ce que tu as produit"}}

Regles :
- Ecris tes livrables dans des fichiers (write_file), chemins relatifs au dossier de travail.
- Commandes autorisees (run_command) : {whitelist}.
- Pour installer, utilise UNIQUEMENT install_package avec un nom et une version exacts. N'utilise jamais
  pip install, npm install, un lien de telechargement ou un script d'installation dans run_command.
- manager=python installe dans l'environnement prive de la tache. manager=npm installe dans un espace
  npm prive, sans scripts (les scripts npm sont refuses). Pour une application Windows, utilise
  manager=winget et scope=user ou system ; l'application est mise a disposition de l'utilisateur,
  elle n'est pas lancee automatiquement par l'agent.
  Orchestrator calcule le niveau d'acces et peut demander l'accord de l'utilisateur.
- Outils MCP OFFICIELS uniquement : active un serveur avec add_mcp (autorises : {mcp_servers}),
  puis utilise ses outils via mcp_call. Tout serveur non officiel est refuse automatiquement.
  Si le paquet du serveur manque, installe-le d'abord avec install_package, puis rappelle add_mcp.
- Memoire persistante : search_knowledge(query) cherche dans (TON namespace de role + lecons
  partagees + notes utilisateur). save_knowledge(content) ENREGISTRE une astuce dans TON namespace
  de role -- elle te servira (ainsi qu'aux futurs agents du meme role) sur les taches a venir.
  Consulte la memoire AU DEBUT de chaque mission, et enregistre une lecon AVANT de finir si tu as
  appris quelque chose de generalisable.
- Competences reutilisables : propose_skill soumet uniquement une methode DOCUMENTAIRE. Elle ne
  devient jamais active automatiquement : un humain doit la relire et l'approuver dans /skills.
- Notifications sortantes : send_notification accepte seulement un nom configure par le serveur.
  Canaux disponibles : {channels}. Tu ne fournis jamais d'URL, de webhook ni de secret.
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
        channels=", ".join(await _configured_channels()) or "(aucun)",
    )
    ctrl = running_tasks.get(task_id)
    if ctrl and ctrl.get("skills_block"):
        system += NL + ctrl["skills_block"]
    # Mode entreprise : lire le depot cible (lecture seule), ecrire UNIQUEMENT dans delivery/.
    _t = await db_get_task(task_id)
    company = bool(_t and _t.get("company_mode"))
    if _t and _t.get("execution_mode") == "docker":
        system += (
            NL + "MODE DOCKER ISOLE : add_mcp et mcp_call sont indisponibles. "
            "N'essaie pas de les utiliser dans cette mission."
        )
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
        await _wait_if_paused(task_id)
        if await _is_stopped(task_id):
            break
        hist.append({"role": "assistant", "content": raw})
        data = _extract_json(raw) or {}

        thought = data.get("thought", "")
        if thought:
            await emit(task_id, iteration, name, "agent_message", {"kind": "thought", "content": thought})

        async def _exec_one(act):
            tool = act.get("tool")
            await emit(task_id, iteration, name, "tool_call",
                       {"tool": tool, "input": {k: v for k, v in act.items() if k != "tool"}})
            out = await execute_tool(
                task_id, folder, web_enabled, act, agent_name=name, iteration=iteration
            )
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
            if tool in ("write_file", "run_command", "run_tests", "install_package"):
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

        # Seules les competences explicitement approuvees par un humain sont
        # reutilisees. Elles restent du texte documentaire, jamais du code.
        skills_block = ""
        try:
            skill_hits = await agent_skills.search_skills(
                objective, limit=5, db_path=DB_PATH
            )
            if skill_hits:
                skill_lines = []
                for hit in skill_hits:
                    skill = hit.skill
                    skill_lines.append(
                        "## " + skill.name + NL + skill.summary + NL + skill.instructions
                    )
                skills_block = (
                    "COMPETENCES REUTILISABLES VALIDEES PAR L'UTILISATEUR :" + NL
                    + (NL + NL).join(skill_lines)
                )
                start += NL + NL + skills_block
        except Exception as exc:
            log.info("[skills] recherche indisponible (%s)", type(exc).__name__)
        if ctrl0 is not None:
            ctrl0["skills_block"] = skills_block

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
            chef_messages = [{"role": "user", "content": await _resume_context(task_id, folder)}]
            await emit(task_id, iteration, "systeme", "info",
                       {"msg": "Reprise de la tache au tour " + str(iteration) + " (contexte reconstruit)."})
        else:
            chef_messages = [{"role": "user", "content": start + managed_block}]
            iteration = 0

        if not await db_transition_task_status(
            task_id, "running", {"queued", "running"}
        ):
            return
        while iteration < max_iter:
            await _wait_if_paused(task_id)
            if await _is_stopped(task_id):
                break

            ctrl = running_tasks.get(task_id)
            if ctrl and ctrl.get("over_budget"):
                msg = ("Budget maximal atteint (%.2f $ depenses). Arret de la tache. "
                       "Augmente le plafond puis reprends si besoin." % ctrl.get("cost", 0.0))
                if await db_transition_task_status(task_id, "stopped", {"running"}):
                    await emit(task_id, iteration, "systeme", "info", {"msg": msg})
                    await emit(task_id, iteration, "chef", "done", {"summary": msg})
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
                if await db_transition_task_status(task_id, "done", {"running"}):
                    await emit(task_id, iteration, "chef", "done", {"summary": final})
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
            if await db_transition_task_status(task_id, "failed", {"running"}):
                await emit(task_id, iteration, "chef", "done",
                           {"summary": "Limite de " + str(max_iter) + " iterations atteinte."})
    except asyncio.CancelledError:
        current = await db_get_task(task_id)
        # Une fermeture de l'application ou la perte du bail n'est pas un
        # ordre d'arret humain. La file durable reprendra la tache.
        if current and current.get("status") not in {"paused", "stopped", "done"}:
            await db_transition_task_status(task_id, "queued", {"running"})
        raise
    except Exception as e:
        log.exception("Task %s crashed", task_id)
        if await db_transition_task_status(task_id, "failed", {"running", "queued"}):
            await emit(task_id, 0, "systeme", "error", {"msg": str(e)[:300]})
        raise
    finally:
        await _stop_mcp(task_id)
        await _cancel_install_waiters(task_id, reason="task_finished")
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
    install_policy: str = "ask"  # blocked|ask|project|user|admin
    execution_mode: str = DEFAULT_EXECUTION_MODE  # docker|local


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
    install_policy = (body.install_policy or "ask").strip().lower()
    if install_policy not in INSTALL_POLICY_VALUES:
        raise HTTPException(400, "Niveau d'installation invalide.")
    execution_mode = (body.execution_mode or DEFAULT_EXECUTION_MODE).strip().lower()
    if execution_mode not in EXECUTION_MODES:
        raise HTTPException(400, "Mode d'execution invalide.")
    target_path = None
    if company:
        if body.target_repo_url:
            try:
                target_path = await _clone_repo(body.target_repo_url, body.github_token)
            except Exception as e:
                raise HTTPException(400, "Connexion au depot GitHub impossible : " + str(e)[:300])
        elif body.target_path:
            try:
                target_path = str(resolve_allowed_target_path(body.target_path))
            except (OSError, ValueError) as e:
                raise HTTPException(400, str(e)[:300])
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
                                   install_policy=install_policy,
                                   execution_mode=execution_mode)
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


async def _cancel_install_waiters_locked(task_id: int, reason: str):
    rows = await db_list_install_requests(task_id, status="pending")
    await db_cancel_pending_install_requests(task_id, decision=reason)
    for row in rows:
        request_id = row["id"]
        future = install_waiters.pop(request_id, None)
        install_waiter_tasks.pop(request_id, None)
        if future and not future.done():
            future.set_result("cancelled")
        try:
            await emit(task_id, row.get("iteration", 0), row.get("agent") or "Agent",
                       "permission_resolved", {
                           "request_id": request_id,
                           "decision": "cancelled",
                           "message": "La demande a ete annulee.",
                       })
        except Exception as exc:
            log.warning("Resolution d'installation non emise: %s", exc)


async def _cancel_install_waiters(task_id: int, reason: str = "task_stopped"):
    ctrl = running_tasks.get(task_id)
    if ctrl:
        async with ctrl["permission_lock"]:
            await _cancel_install_waiters_locked(task_id, reason)
    else:
        await _cancel_install_waiters_locked(task_id, reason)


def _spawn_loop(task_id, resume=False):
    existing = running_tasks.get(task_id)
    if existing and existing.get("task"):
        return existing["task"]
    pause = asyncio.Event()
    pause.set()  # set = en marche ; clear = en pause
    running_tasks[task_id] = {
        "pause": pause,
        "step": False,
        "inbox": [],
        "mcp": {},
        "install_lock": asyncio.Lock(),
        "permission_lock": asyncio.Lock(),
    }
    running_tasks[task_id]["task"] = asyncio.create_task(
        _run_task(task_id, resume=resume), name=f"agent-task-{task_id}"
    )
    return running_tasks[task_id]["task"]


@router.post("/api/tasks/{task_id}/start")
async def start_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    if task_id in running_tasks:
        return {"status": "already_running"}
    if task_queue is None:
        raise HTTPException(503, "La file durable n'est pas encore prete.")
    if t.get("execution_mode") == "docker":
        try:
            await get_sandbox_runtime().ensure_available()
        except SandboxUnavailableError as exc:
            raise HTTPException(409, str(exc))
    job = await enqueue_task_run(task_id, resume=False)
    return {"status": "queued", "job_id": job.id}


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
    if task_queue is None:
        raise HTTPException(503, "La file durable n'est pas encore prete.")
    if t.get("execution_mode") == "docker":
        try:
            await get_sandbox_runtime().ensure_available()
        except SandboxUnavailableError as exc:
            raise HTTPException(409, str(exc))
    job = await enqueue_task_run(task_id, resume=True)
    return {"status": "queued", "resumed": True, "job_id": job.id}


@router.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: int):
    task = await db_get_task(task_id)
    if not task:
        raise HTTPException(404, "Tache introuvable")
    if task.get("status") not in {"running", "queued"}:
        raise HTTPException(409, "Seule une tache en cours ou en attente peut etre mise en pause.")
    ctrl = running_tasks.get(task_id)
    if ctrl is None and not task.get("queue_job_id"):
        raise HTTPException(409, "Cette tache n'a pas de lancement actif.")
    worker_task = ctrl.get("task") if ctrl else None
    if ctrl:
        async with ctrl["permission_lock"]:
            ctrl["pause"].clear()
            await db_update_task(task_id, status="paused")
    else:
        await db_update_task(task_id, status="paused")
    if task_queue is not None and task.get("queue_job_id"):
        paused = await task_queue.pause(task["queue_job_id"])
        if paused is None or paused.state is not JobState.PAUSED:
            await db_update_task(task_id, status=task.get("status"))
            raise HTTPException(409, "Ce lancement ne peut plus etre mis en pause.")
    if worker_task and not worker_task.done():
        worker_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await worker_task
    await emit(task_id, 0, "systeme", "paused", {})
    return {"status": "paused"}


@router.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: int):
    task = await db_get_task(task_id)
    if not task:
        raise HTTPException(404, "Tache introuvable")
    if task.get("status") != "paused":
        raise HTTPException(409, "Seule une tache en pause peut etre reprise.")
    ctrl = running_tasks.get(task_id)
    if ctrl:
        async with ctrl["permission_lock"]:
            ctrl["pause"].set()
        if task_queue is None or not task.get("queue_job_id"):
            await db_update_task(task_id, status="running")
            await emit(task_id, 0, "systeme", "resumed", {})
            return {"status": "running"}
    if task_queue is not None and task.get("queue_job_id"):
        resumed = await task_queue.resume(task["queue_job_id"])
        if resumed is None or resumed.state is not JobState.QUEUED:
            raise HTTPException(409, "Le lancement durable est introuvable.")
        await db_update_task(task_id, status="queued")
    else:
        raise HTTPException(409, "Cette tache n'a pas de lancement a reprendre.")
    await emit(task_id, 0, "systeme", "resumed", {})
    return {"status": "queued"}


@router.post("/api/tasks/{task_id}/step")
async def step_task(task_id: int):
    """Le pas-a-pas durable est desactive tant que son etat n'est pas persiste."""
    task = await db_get_task(task_id)
    if not task:
        raise HTTPException(404, "Tache introuvable")
    if task.get("status") != "paused":
        raise HTTPException(409, "La tache doit etre en pause.")
    raise HTTPException(
        409,
        "Le mode Etape est temporairement indisponible avec la reprise durable. "
        "Utilisez Reprendre puis Pause.",
    )


@router.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: int):
    task = await db_get_task(task_id)
    if not task:
        raise HTTPException(404, "Tache introuvable")
    ctrl = running_tasks.get(task_id)
    worker_task = ctrl.get("task") if ctrl else None
    if ctrl:
        async with ctrl["permission_lock"]:
            await db_update_task(task_id, status="stopped")
            ctrl["pause"].set()  # debloque la boucle pour qu'elle constate l'arret
            await _terminate_install_process(ctrl.get("install_proc"))
            await _cancel_install_waiters_locked(task_id, "task_stopped")
    else:
        await db_update_task(task_id, status="stopped")
        await _cancel_install_waiters(task_id)
    if task_queue is not None and task.get("queue_job_id"):
        await task_queue.cancel(task["queue_job_id"])
    if worker_task and not worker_task.done():
        worker_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await worker_task
    await _stop_launched(task_id)
    if task.get("execution_mode") == "docker":
        runtime = get_sandbox_runtime()
        try:
            cleanup = await runtime.stop(task_id, strict=False)
        except SandboxError as exc:
            runtime.block_execution(
                "Le nettoyage d'une tache Docker n'a pas pu etre verifie. "
                "Redemarrez Docker Desktop puis l'Orchestrateur."
            )
            raise HTTPException(503, str(exc)[:500]) from None
        if cleanup.failed:
            runtime.block_execution(
                "Un conteneur de tache a resiste au nettoyage. Aucune nouvelle "
                "execution Docker n'est autorisee avant intervention."
            )
            raise HTTPException(
                500, "La tache est arretee, mais son conteneur n'a pas pu etre nettoye."
            )
    await _stop_mcp(task_id)
    await emit(task_id, 0, "systeme", "stopped", {})
    return {"status": "stopped"}


class InstallDecisionBody(BaseModel):
    decision: str


@router.get("/api/tasks/{task_id}/install-requests")
async def list_install_requests(task_id: int, status: Optional[str] = Query(default=None)):
    if not await db_get_task(task_id):
        raise HTTPException(404, "Tache introuvable")
    allowed_statuses = {
        "pending", "approved", "denied", "succeeded", "failed", "expired", "cancelled",
    }
    if status is not None and status not in allowed_statuses:
        raise HTTPException(400, "Etat de demande invalide.")
    rows = await db_list_install_requests(task_id, status=status)
    payloads = []
    for row in rows:
        try:
            payloads.append(await _install_payload_for_task(row))
        except InstallValidationError:
            log.warning("Demande d'installation invalide en base : %s", row.get("id"))
    return {"requests": payloads}


@router.post("/api/tasks/{task_id}/install-requests/{request_id}/decision")
async def decide_install_request(task_id: int, request_id: str, body: InstallDecisionBody):
    if body.decision not in {"allow_once", "deny"}:
        raise HTTPException(400, "Decision invalide.")
    if not await db_get_task(task_id):
        raise HTTPException(404, "Tache introuvable")
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(409, "La tache n'est plus active.")
    async with ctrl["permission_lock"]:
        row = await db_get_install_request(request_id)
        if not row or row["task_id"] != task_id:
            raise HTTPException(404, "Demande d'installation introuvable.")
        if row["status"] != "pending":
            raise HTTPException(409, "Cette demande a deja ete traitee.")
        future = install_waiters.get(request_id)
        if future is None or future.done() or install_waiter_tasks.get(request_id) != task_id:
            raise HTTPException(409, "La tache n'attend plus cette decision.")

        expires_at = row.get("expires_at")
        if expires_at:
            try:
                expired = datetime.fromisoformat(expires_at) <= datetime.utcnow()
            except ValueError:
                expired = True
            if expired:
                await db_expire_install_request(request_id)
                future.set_result("expired")
                try:
                    await emit(task_id, row.get("iteration", 0), row.get("agent") or "Agent",
                               "permission_resolved", {
                                   "request_id": request_id,
                                   "decision": "expired",
                               })
                except Exception as exc:
                    log.warning("Expiration d'installation non emise: %s", exc)
                raise HTTPException(410, "Cette demande a expire.")

        changed = await db_resolve_install_request(task_id, request_id, body.decision)
        if not changed:
            raise HTTPException(409, "Cette demande a deja ete traitee.")
        future.set_result(body.decision)
        try:
            await emit(task_id, row.get("iteration", 0), row.get("agent") or "Agent",
                       "permission_resolved", {
                           "request_id": request_id,
                           "decision": body.decision,
                       })
        except Exception as exc:
            log.warning("Decision d'installation non emise: %s", exc)
    return {"request_id": request_id, "decision": body.decision}


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
    if info and info.get("sandbox"):
        with suppress(Exception):
            await get_sandbox_runtime().stop(task_id, strict=False)
        return
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
    task = await db_get_task(task_id)
    if task and task.get("execution_mode") == "docker":
        if not sandbox_app_network_allowed():
            raise HTTPException(
                409,
                "L'aperçu d'une application dynamique est desactive : le reseau "
                "Docker bridge donne aussi un acces sortant. Un administrateur "
                "peut l'activer explicitement avec ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK.",
            )
        source = (Path(base) / name).read_text(
            encoding="utf-8", errors="replace"
        )[:200_000]
        match = re.search(r"\bport\s*=\s*(\d{2,5})", source)
        container_port = int(match.group(1)) if match else 5000
        if not 1024 <= container_port <= 65535:
            container_port = 5000
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            host_port = int(probe.getsockname()[1])
        try:
            handle = await get_sandbox_runtime().launch(
                task_id,
                base,
                ("python", name),
                env={
                    **_sandbox_task_environment(base),
                    "PORT": str(container_port),
                },
                network_enabled=True,
                published_ports={host_port: container_port},
                lifetime_seconds=3600,
            )
        except SandboxError as exc:
            raise HTTPException(409, "Lancement isole refuse : " + str(exc)[:1200])
        url = f"http://127.0.0.1:{host_port}"
        launched_apps[task_id] = {
            "sandbox": handle,
            "url": url,
            "output": [],
        }
        return {
            "type": "python",
            "entry": name,
            "url": url,
            "running": True,
            "output": "Application lancee dans un conteneur isole (duree maximale : 1 h).",
        }
    python_exe = _local_python_executable(base)
    if not python_exe:
        raise HTTPException(
            409,
            "Le lancement local exige un Python externe. Utilisez le mode Docker.",
        )
    proc = await asyncio.create_subprocess_exec(
        python_exe, name, cwd=str(base),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env=build_child_environment())
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
    # Le livrable est non fiable. Une origine opaque et l'absence de connexion
    # l'empechent de lire localStorage, d'appeler l'API Orchestrator ou
    # d'exfiltrer la cle depuis le navigateur.
    return FileResponse(
        p,
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": (
                "sandbox allow-scripts; default-src 'none'; "
                "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                "img-src data: blob:; font-src data:; base-uri 'none'; "
                "form-action 'none'"
            ),
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )


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
            rel = p.relative_to(base)
            if p.is_file() and not any(part in _WORKSPACE_IGNORE_DIRS for part in rel.parts):
                z.write(p, rel.as_posix())
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
async def _proc_run(args, cwd, timeout, *, task_id: Optional[int] = None):
    """Lance une commande, renvoie (code, sortie tronquee)."""
    if await _task_uses_docker(task_id):
        docker_args = list(args)
        executable = Path(str(docker_args[0])).name.lower()
        if executable.startswith("python") or executable in {"python.exe", "python3"}:
            docker_args[0] = "python"
        try:
            result = await get_sandbox_runtime().run_command(
                task_id, cwd, tuple(str(arg) for arg in docker_args),
                timeout=timeout, env=_sandbox_task_environment(cwd),
            )
            return result.exit_code, (result.output or "(aucune sortie)")[:3000]
        except SandboxError as exc:
            return 1, "Execution isolee refusee : " + str(exc)[:1200]
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=build_child_environment())
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


async def _build_ping(folder, py_exe, *, task_id: Optional[int] = None):
    base = Path(folder)
    entry = next((n for n in WEB_ENTRYPOINTS if (base / n).exists()), None)
    if not entry:
        return {"name": "ping HTTP /", "ok": True, "log": "Pas d'app web Python (ignore)."}
    if await _task_uses_docker(task_id):
        return {
            "name": "ping HTTP /",
            "ok": True,
            "log": "Le code n'est pas execute sur l'hote pendant le build. "
                   "Utilisez Lancer pour le test HTTP isole.",
        }
    # PYTHONUNBUFFERED : stdout non bufferise -> on lit l'URL de demarrage en temps reel.
    # FLASK_DEBUG=0 : decourage le reloader. On NE met PAS WERKZEUG_RUN_MAIN (sinon Werkzeug
    # cherche WERKZEUG_SERVER_FD -> KeyError fatale au demarrage).
    env = build_child_environment()
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
                if not any(x in p.parts for x in (
                    ".ci", "venv", ".venv", ".orchestrator", "node_modules", "__pycache__"
                ))]
    errs = []
    for p in py_files:
        try:
            await asyncio.to_thread(py_compile.compile, str(p), None, str(p), True)
        except py_compile.PyCompileError as e:
            errs.append(str(e)[:300])
    steps.append({"name": "syntaxe", "ok": not errs,
                  "log": (str(len(py_files)) + " fichier(s) OK") if not errs else NL.join(errs)})

    # 2. Reutilise uniquement l'environnement alimente par install_package.
    # Le bouton Build ne doit jamais installer silencieusement un requirements.txt controle par un agent.
    docker_mode = await _task_uses_docker(task_id)
    task_python = _task_venv_python(base)
    py_exe = "python" if docker_mode else _local_python_executable(base)
    docker_packages = base / ".orchestrator" / "python-packages"
    dependencies_ready = docker_packages.is_dir() if docker_mode else task_python.exists()
    if (base / "requirements.txt").exists() and not dependencies_ready:
        steps.append({
            "name": "dependances autorisees",
            "ok": False,
            "log": "Aucune dependance n'a ete approuvee pour cette tache. "
                   "L'agent doit utiliser install_package avant le build.",
        })
    else:
        steps.append({
            "name": "dependances autorisees",
            "ok": True,
            "log": ("Dependances privees de la tache utilisees."
                    if dependencies_ready else "Aucune dependance Python declaree."),
        })

    # 3. Tests unitaires (pytest)
    if py_exe is None:
        code, tlog = 1, (
            "Tests locaux indisponibles dans le paquet autonome sans Python externe."
        )
    else:
        code, tlog = await _proc_run(
            [py_exe, "-m", "pytest", "-q"], base, 180, task_id=task_id
        )
    # pytest renvoie 5 quand aucun test collecte -> on ne compte pas ca comme un echec
    steps.append({"name": "pytest", "ok": code in (0, 5), "log": tlog or "(aucune sortie)"})

    # 4. Ping HTTP /
    if py_exe is None:
        steps.append({
            "name": "ping HTTP /", "ok": False,
            "log": "Lancement local indisponible sans Python externe.",
        })
    else:
        steps.append(await _build_ping(folder, py_exe, task_id=task_id))

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
    if automation_store is not None:
        await automation_store.cancel_for_task(task_id)
    if task_queue is not None:
        await task_queue.cancel_scheduled_for_task(task_id)
    await stop_task(task_id)
    t = await db_get_task(task_id)
    if t:
        folder = Path(t["folder"]).resolve(strict=False)
        projects_root = PROJECTS.resolve()
        if folder != projects_root and projects_root in folder.parents:
            if not await asyncio.to_thread(_rmtree_force, folder):
                raise HTTPException(500, "Le dossier de la tache n'a pas pu etre supprime.")
        else:
            log.error("Suppression du dossier de tache refusee : %s", folder)
            raise HTTPException(400, "Dossier de tache invalide ; suppression refusee.")
    # Nettoyer aussi le venv de CI et les snapshots (sinon orphelins, ~50 Mo+).
    for auxiliary in (
        PROJECTS / ".ci" / str(task_id),
        PROJECTS / ".snapshots" / str(task_id),
    ):
        if not await asyncio.to_thread(_rmtree_force, auxiliary):
            raise HTTPException(500, "Des fichiers annexes n'ont pas pu etre supprimes.")
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
async def stream_task(task_id: int, after_id: int = Query(default=0, ge=0)):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")

    async def gen() -> AsyncGenerator[str, None]:
        last_id = after_id
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
