"""api_control.py — Module FastAPI (APIRouter) pour le Centre de Contrôle.

Fournit les endpoints du dashboard unifié :
  GET  /api/overview          — KPIs agrégés du dashboard
  GET  /api/monitoring        — Métriques tokens/coûts/erreurs/latence
  GET  /api/agents            — Liste enrichie des agents
  GET  /api/tasks             — Tâches avec pagination
  GET  /api/events            — Flux SSE temps réel (broadcaster)
  GET  /api/activity          — Historique d'activité (24 h)
  POST /api/monitoring/reset  — Remise à zéro des compteurs
  POST /api/agents/{id}/stop  — Info arrêt agent (redirect tâche parente)

Contraintes respectées :
  - Même DB que orchestrator.py / team.py  (apps.db)
  - Patterns async aiosqlite identiques au code existant
  - Aucune dépendance supplémentaire hors celles déjà présentes
  - Tables nouvelles : token_usage, error_log  (CREATE IF NOT EXISTS)
  - Broadcaster SSE via asyncio.Queue (fanout sur N clients)
  - Heartbeat SSE toutes les 30 s
  - Format SSE : event: <type>\\ndata: {json}\\n\\n  (compatible addEventListener)
  - api_key accepté en query param pour EventSource (pas de header custom)

Intégration dans orchestrator.py — voir api_control_install.md
"""
import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, AsyncGenerator, Optional

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

log = logging.getLogger(__name__)

# ── Version du module ──────────────────────────────────────────────────────────
_VERSION = "1.1.0"

# ── Chemin de la base de données (même que orchestrator.py / team.py) ─────────
# Résolu dynamiquement depuis l'emplacement de *ce* fichier une fois monté.
# Peut être surchargé avant import si la DB est ailleurs.
DB_PATH = Path(__file__).parent / "apps.db"

# ── Table des prix approximatifs par modèle ($/1 000 tokens) ─────────────────
# Source : tarifs publics mai 2026 — à mettre à jour si nécessaire.
_PRICE_PER_1K: dict[str, dict[str, float]] = {
    # Claude — tarifs Anthropic verifies (mai 2026)
    "claude-opus-4-8":       {"input": 0.005,   "output": 0.025},   # NOUVEAU phare
    "claude-opus-4-7":       {"input": 0.005,   "output": 0.025},   # (etait 0.015/0.075 par erreur)
    "claude-opus-4-6":       {"input": 0.005,   "output": 0.025},
    "claude-opus-4-5":       {"input": 0.005,   "output": 0.025},   # (etait 0.015/0.075 par erreur)
    "claude-opus-4-1":       {"input": 0.015,   "output": 0.075},   # ancien tarif Opus haut
    "claude-sonnet-4-6":     {"input": 0.003,   "output": 0.015},
    "claude-sonnet-4-5":     {"input": 0.003,   "output": 0.015},
    "claude-haiku-4-5":      {"input": 0.001,   "output": 0.005},   # (etait 4x trop bas)
    "claude-3-5-sonnet":     {"input": 0.003,   "output": 0.015},
    "claude-3-haiku":        {"input": 0.00025, "output": 0.00125},
    # OpenAI
    "gpt-4o":                {"input": 0.005,   "output": 0.015},
    "gpt-4o-mini":           {"input": 0.00015, "output": 0.0006},
    "gpt-5.5":               {"input": 0.01,    "output": 0.03},
    "gpt-4-turbo":           {"input": 0.01,    "output": 0.03},
    # Gemini
    "gemini-3.5-flash":      {"input": 0.000075, "output": 0.0003},
    "gemini-2.0-flash":      {"input": 0.000075, "output": 0.0003},
    "gemini-1.5-pro":        {"input": 0.00125,  "output": 0.005},
}
_PRICE_DEFAULT = {"input": 0.003, "output": 0.015}  # fallback (= tarif Sonnet)


def _price(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calcule le coût estimé en USD pour un appel modèle donné."""
    prices = _PRICE_PER_1K.get(model, _PRICE_DEFAULT)
    return round(
        (input_tokens / 1000) * prices["input"]
        + (output_tokens / 1000) * prices["output"],
        6,
    )


def _now_iso() -> str:
    """Retourne l'horodatage UTC actuel au format ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


# ── Heure de démarrage du processus (uptime) ──────────────────────────────────
_START_TIME = datetime.now(timezone.utc)


def _uptime_seconds() -> float:
    """Durée en secondes depuis le démarrage du module."""
    return (datetime.now(timezone.utc) - _START_TIME).total_seconds()


# ── Broadcaster SSE — fanout vers N clients connectés ─────────────────────────
class _SSEBroadcaster:
    """Distribue chaque événement SSE à tous les clients abonnés.

    Chaque client reçoit sa propre asyncio.Queue. Les clients trop lents
    (queue pleine) sont décrochés automatiquement pour ne pas bloquer.
    """

    def __init__(self) -> None:
        self._queues: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        """Enregistre un nouveau client ; retourne sa Queue personnelle."""
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._queues.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        """Supprime un client de la liste de diffusion."""
        try:
            self._queues.remove(q)
        except ValueError:
            pass

    def publish(self, event: dict) -> None:
        """Envoie un événement à tous les clients (non-bloquant)."""
        dead: list[asyncio.Queue] = []
        for q in self._queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)  # client trop lent — on le décroche
        for q in dead:
            self.unsubscribe(q)

    @property
    def subscriber_count(self) -> int:
        """Nombre de clients SSE actuellement connectés."""
        return len(self._queues)


# Instance globale — importable depuis orchestrator.py / team.py
broadcaster = _SSEBroadcaster()


def _sse_frame(event_type: str, payload: dict) -> str:
    """Formate un message SSE avec champ `event:` pour addEventListener().

    Format retourné ::

        event: task_update\n
        data: {"type": "task_update", ...}\n
        \n

    Compatible avec `EventSource.addEventListener('task_update', ...)` ET
    `EventSource.onmessage` (car `data:` est toujours présent).
    """
    data = json.dumps(payload, ensure_ascii=False)
    return f"event: {event_type}\ndata: {data}\n\n"


def publish_event(event_type: str, **payload) -> None:
    """Publie un événement SSE depuis n'importe quel module.

    Exemple d'utilisation depuis team.py ou orchestrator.py ::

        from api_control import publish_event
        publish_event("task_update",  task_id=42, status="running")
        publish_event("agent_update", task_id=42, agent="DevAgent")
        publish_event("app_update",   app_id=3,   status="running")
        publish_event("notification", message="Tâche terminée", level="info")
    """
    data = {"type": event_type, "ts": _now_iso(), **payload}
    broadcaster.publish({"event_type": event_type, "payload": data})


# ── Initialisation des tables de tracking ─────────────────────────────────────
async def init_control_db(db_path: Optional[Path] = None) -> None:
    """Crée les tables token_usage et error_log si elles n'existent pas.

    Doit être appelée dans le lifespan de l'application
    (après init_db() et init_team_db()).
    """
    path = db_path or DB_PATH
    async with aiosqlite.connect(str(path), timeout=30.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")

        # ── Tracking de consommation de tokens ────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS token_usage (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
                task_id         INTEGER,
                agent           TEXT DEFAULT '',
                provider        TEXT NOT NULL DEFAULT 'claude',
                model           TEXT NOT NULL DEFAULT '',
                input_tokens    INTEGER NOT NULL DEFAULT 0,
                output_tokens   INTEGER NOT NULL DEFAULT 0,
                estimated_cost  REAL NOT NULL DEFAULT 0.0
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_token_usage_timestamp
            ON token_usage (timestamp)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_token_usage_model
            ON token_usage (provider, model)
        """)

        # ── Journal des erreurs ───────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS error_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL DEFAULT (datetime('now')),
                endpoint    TEXT DEFAULT '',
                error_type  TEXT DEFAULT '',
                message     TEXT DEFAULT '',
                stack_trace TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_log_timestamp
            ON error_log (timestamp)
        """)

        await db.commit()
    log.info("[api_control] Tables token_usage et error_log prêtes (v%s).", _VERSION)


# ── Helpers DB ────────────────────────────────────────────────────────────────
async def _log_error(
    endpoint: str,
    error_type: str,
    message: str,
    stack: str = "",
) -> None:
    """Insère une entrée dans error_log (best-effort, ne lève jamais)."""
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=10.0) as db:
            await db.execute(
                "INSERT INTO error_log "
                "(timestamp, endpoint, error_type, message, stack_trace) "
                "VALUES (?,?,?,?,?)",
                (_now_iso(), endpoint, error_type, message[:2000], stack[:4000]),
            )
            await db.commit()
    except Exception as exc:
        log.warning("[api_control] Impossible d'écrire dans error_log : %s", exc)


async def record_token_usage(
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    task_id: Optional[int] = None,
    agent: str = "",
) -> None:
    """Enregistre une consommation de tokens dans token_usage.

    Peut être importée et appelée depuis team.py après chaque appel LLM.
    Le coût est calculé automatiquement via la table de prix _PRICE_PER_1K.
    """
    cost = _price(model, input_tokens, output_tokens)
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=10.0) as db:
            await db.execute(
                "INSERT INTO token_usage "
                "(timestamp, task_id, agent, provider, model, "
                " input_tokens, output_tokens, estimated_cost) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (_now_iso(), task_id, agent, provider, model,
                 input_tokens, output_tokens, cost),
            )
            await db.commit()
    except Exception as exc:
        log.warning("[api_control] record_token_usage erreur : %s", exc)


# ── Router FastAPI ─────────────────────────────────────────────────────────────
router = APIRouter(tags=["control-center"])


# ── GET /api/overview ─────────────────────────────────────────────────────────
@router.get("/api/overview")
async def get_overview():
    """Données agrégées pour la vue d'atterrissage du dashboard.

    Champs retournés (alignés avec control.html) :
    - agents_active   : nombre d'agents dans des tâches en cours
    - tasks.running   : tâches en statut 'running'
    - tasks.total     : total de toutes les tâches
    - tasks.by_status : décompte par statut
    - apps.count      : nombre total d'apps
    - apps.by_status  : décompte par statut
    - tokens_24h.in   : tokens d'entrée (24 h)
    - tokens_24h.out  : tokens de sortie (24 h)
    - tokens_24h.cost : coût estimé USD (24 h)
    - recent_tasks    : 10 dernières tâches
    - version         : version du module api_control
    - started_at      : horodatage ISO du démarrage du serveur
    - uptime_seconds  : uptime en secondes
    - sse_clients     : nombre de clients SSE connectés
    """
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            db.row_factory = aiosqlite.Row

            # ── Apps ──────────────────────────────────────────────────────────
            async with db.execute(
                "SELECT status, COUNT(*) AS cnt FROM apps GROUP BY status"
            ) as cur:
                app_rows = await cur.fetchall()
            apps_by_status = {r["status"]: r["cnt"] for r in app_rows}
            total_apps = sum(apps_by_status.values())

            # ── Tâches ────────────────────────────────────────────────────────
            async with db.execute(
                "SELECT status, COUNT(*) AS cnt FROM tasks GROUP BY status"
            ) as cur:
                task_rows = await cur.fetchall()
            tasks_by_status = {r["status"]: r["cnt"] for r in task_rows}
            total_tasks = sum(tasks_by_status.values())
            running_tasks_count = tasks_by_status.get("running", 0)

            # ── Agents actifs (dans des tâches en cours) ──────────────────────
            async with db.execute(
                "SELECT COUNT(*) AS cnt FROM task_agents ta "
                "JOIN tasks t ON t.id = ta.task_id "
                "WHERE t.status = 'running'"
            ) as cur:
                row = await cur.fetchone()
            active_agents = row["cnt"] if row else 0

            # ── 10 dernières tâches ───────────────────────────────────────────
            async with db.execute(
                "SELECT id, objective, status, iteration, "
                "       max_iterations, created_at "
                "FROM tasks ORDER BY created_at DESC LIMIT 10"
            ) as cur:
                recent_tasks = [dict(r) for r in await cur.fetchall()]

            # ── Tokens 24 h ───────────────────────────────────────────────────
            cutoff_24h = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat()
            async with db.execute(
                "SELECT COALESCE(SUM(input_tokens),0)   AS ti, "
                "       COALESCE(SUM(output_tokens),0)  AS to_, "
                "       COALESCE(SUM(estimated_cost),0) AS cost "
                "FROM token_usage WHERE timestamp >= ?",
                (cutoff_24h,),
            ) as cur:
                tok = await cur.fetchone()

        tokens_in  = tok["ti"]   if tok else 0
        tokens_out = tok["to_"]  if tok else 0
        cost_24h   = round(tok["cost"] if tok else 0, 4)

        return {
            # ── Champs attendus par control.html ──────────────────────────────
            "agents_active":  active_agents,
            "tasks": {
                "running":    running_tasks_count,
                "total":      total_tasks,
                "by_status":  tasks_by_status,
            },
            "apps": {
                "count":      total_apps,
                "by_status":  apps_by_status,
            },
            "tokens_24h": {
                "in":         tokens_in,
                "out":        tokens_out,
                "cost":       cost_24h,
                "cost_usd":   cost_24h,   # alias
            },
            "recent_tasks":   recent_tasks,
            # ── Champs système ────────────────────────────────────────────────
            "version":        _VERSION,
            "started_at":     _START_TIME.isoformat(),
            "uptime_seconds": _uptime_seconds(),
            "sse_clients":    broadcaster.subscriber_count,
        }

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/overview", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/overview erreur : %s", exc)
        raise HTTPException(500, "Erreur interne overview : " + str(exc)[:200])


# ── GET /api/monitoring ───────────────────────────────────────────────────────
@router.get("/api/monitoring")
async def get_monitoring(
    period: Annotated[
        str,
        Query(description="Fenêtre : '1h', '24h' (défaut), '7d', '30d', 'all'"),
    ] = "24h",
):
    """Métriques de monitoring détaillées.

    Paramètre :
    - period : fenêtre temporelle — '1h', '24h' (défaut), '7d', '30d', 'all'

    Champs retournés (alignés avec control.html) :
    - providers  : [{name, tokens_in, tokens_out, cost, calls, models}]
    - requests   : nombre total d'appels LLM sur la période
    - hourly     : activité horaire [{label, at, tokens, value, cost}]
    - series     : alias de hourly
    - errors     : erreurs récentes [{at, provider, message, error_type}]
    - totals     : agrégats {input_tokens, output_tokens, cost_usd, calls}
    - errors_24h : nb d'erreurs sur 24 h
    - price_table: tarifs de référence par modèle
    """
    _PERIOD_MAP = {
        "1h":  timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d":  timedelta(days=7),
        "30d": timedelta(days=30),
    }
    if period == "all":
        cutoff = "1970-01-01T00:00:00+00:00"
    else:
        delta = _PERIOD_MAP.get(period, timedelta(hours=24))
        cutoff = (datetime.now(timezone.utc) - delta).isoformat()

    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            db.row_factory = aiosqlite.Row

            # ── Tokens par provider / modèle ──────────────────────────────────
            async with db.execute(
                "SELECT provider, model, "
                "       SUM(input_tokens)   AS total_in, "
                "       SUM(output_tokens)  AS total_out, "
                "       SUM(estimated_cost) AS total_cost, "
                "       COUNT(*)            AS calls "
                "FROM token_usage "
                "WHERE timestamp >= ? "
                "GROUP BY provider, model "
                "ORDER BY total_cost DESC",
                (cutoff,),
            ) as cur:
                model_rows = [dict(r) for r in await cur.fetchall()]

            # ── Agrégat global (période) ──────────────────────────────────────
            async with db.execute(
                "SELECT COALESCE(SUM(input_tokens),0)   AS ti, "
                "       COALESCE(SUM(output_tokens),0)  AS to_, "
                "       COALESCE(SUM(estimated_cost),0) AS cost, "
                "       COUNT(*) AS calls "
                "FROM token_usage WHERE timestamp >= ?",
                (cutoff,),
            ) as cur:
                agg = await cur.fetchone()

            # ── Série horaire (dernières 24 h, 24 buckets max) ────────────────
            cutoff_chart = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat()
            async with db.execute(
                "SELECT strftime('%H', timestamp) AS hr, "
                "       SUM(input_tokens + output_tokens) AS tokens, "
                "       SUM(estimated_cost) AS cost "
                "FROM token_usage "
                "WHERE timestamp >= ? "
                "GROUP BY hr ORDER BY hr",
                (cutoff_chart,),
            ) as cur:
                hourly_rows = [dict(r) for r in await cur.fetchall()]

            # ── Erreurs récentes (50) ─────────────────────────────────────────
            async with db.execute(
                "SELECT id, timestamp AS at, endpoint, error_type, message "
                "FROM error_log "
                "WHERE timestamp >= ? "
                "ORDER BY timestamp DESC LIMIT 50",
                (cutoff,),
            ) as cur:
                err_rows = [dict(r) for r in await cur.fetchall()]

            # ── Nombre d'erreurs sur 24 h ─────────────────────────────────────
            cutoff_24h = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat()
            async with db.execute(
                "SELECT COUNT(*) AS cnt FROM error_log WHERE timestamp >= ?",
                (cutoff_24h,),
            ) as cur:
                err_24h_count = (await cur.fetchone())["cnt"]

        # ── Construction de la liste providers (groupée par provider) ─────────
        provider_map: dict[str, dict] = {}
        for row in model_rows:
            pname = row["provider"]
            if pname not in provider_map:
                provider_map[pname] = {
                    "name":       pname,
                    "tokens_in":  0,
                    "tokens_out": 0,
                    "cost":       0.0,
                    "calls":      0,
                    "models":     [],
                }
            p = provider_map[pname]
            p["tokens_in"]  += row["total_in"]
            p["tokens_out"] += row["total_out"]
            p["cost"]        = round(p["cost"] + row["total_cost"], 6)
            p["calls"]      += row["calls"]
            p["models"].append({
                "model":      row["model"],
                "tokens_in":  row["total_in"],
                "tokens_out": row["total_out"],
                "cost":       round(row["total_cost"], 6),
                "calls":      row["calls"],
            })
        providers = sorted(
            provider_map.values(),
            key=lambda x: x["cost"],
            reverse=True,
        )

        # ── Format des erreurs (compatible frontend) ──────────────────────────
        errors_fmt = [
            {
                "at":         e["at"],
                "timestamp":  e["at"],
                "provider":   e["endpoint"],   # endpoint sert de source
                "source":     e["endpoint"],
                "error_type": e["error_type"],
                "message":    e["message"],
                "error":      e["message"],
            }
            for e in err_rows
        ]

        # ── Série horaire enrichie ────────────────────────────────────────────
        series = [
            {
                "label":  r["hr"] + "h",
                "at":     r["hr"],
                "tokens": r["tokens"] or 0,
                "value":  r["tokens"] or 0,
                "cost":   round(r["cost"] or 0, 6),
            }
            for r in hourly_rows
        ]

        # ── Table prix ────────────────────────────────────────────────────────
        price_table = {
            model: {
                "input_per_1k_usd":  prices["input"],
                "output_per_1k_usd": prices["output"],
            }
            for model, prices in _PRICE_PER_1K.items()
        }

        return {
            "providers":         providers,
            "requests":          agg["calls"] if agg else 0,
            "hourly":            series,
            "series":            series,
            "errors":            errors_fmt,
            "totals": {
                "input_tokens":  agg["ti"]   if agg else 0,
                "output_tokens": agg["to_"]  if agg else 0,
                "total_tokens":  (agg["ti"] + agg["to_"]) if agg else 0,
                "cost_usd":      round(agg["cost"] if agg else 0, 4),
                "calls":         agg["calls"] if agg else 0,
            },
            "errors_24h":        err_24h_count,
            "period":            period,
            "price_table":       price_table,
            "sse_clients":       broadcaster.subscriber_count,
        }

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/monitoring", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/monitoring erreur : %s", exc)
        raise HTTPException(500, "Erreur interne monitoring : " + str(exc)[:200])


# ── GET /api/agents ───────────────────────────────────────────────────────────
@router.get("/api/agents")
async def get_agents():
    """Liste enrichie de tous les agents avec statut, tâche courante et statistiques.

    Retourne un objet avec :
    - `agents` / `items` : liste des agents (les deux clés sont présentes)
    - `total`            : nombre total d'agents

    Chaque agent contient :
    - id, task_id, name, role, provider, model, created_by, created_at
    - task_status, task_objective, task_iteration
    - status      : synthétique ('active'|'paused'|'idle'|'failed'|'unknown')
    - msg_count   : messages envoyés
    - last_msg_at : horodatage du dernier message
    - last_message: extrait du dernier message (300 car. max)
    """
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                """
                SELECT
                    ta.id,
                    ta.task_id,
                    ta.name,
                    ta.role,
                    ta.provider,
                    ta.model,
                    ta.managed_agent_id,
                    ta.created_by,
                    ta.created_at,
                    t.status       AS task_status,
                    t.objective    AS task_objective,
                    t.iteration    AS task_iteration
                FROM task_agents ta
                LEFT JOIN tasks t ON t.id = ta.task_id
                ORDER BY ta.task_id DESC, ta.id
                """
            ) as cur:
                agents = [dict(r) for r in await cur.fetchall()]

            async with db.execute(
                "SELECT task_id, agent, COUNT(*) AS msg_count, "
                "       MAX(created_at) AS last_msg_at "
                "FROM messages GROUP BY task_id, agent"
            ) as cur:
                msg_stats: dict[tuple, dict] = {
                    (r["task_id"], r["agent"]): {
                        "msg_count":   r["msg_count"],
                        "last_msg_at": r["last_msg_at"],
                    }
                    for r in await cur.fetchall()
                }

            async with db.execute(
                "SELECT task_id, agent, content "
                "FROM messages "
                "WHERE id IN ("
                "    SELECT MAX(id) FROM messages "
                "    WHERE kind IN "
                "      ('tool_result','assistant','step','done','error') "
                "    GROUP BY task_id, agent"
                ")"
            ) as cur:
                last_msgs: dict[tuple, str] = {
                    (r["task_id"], r["agent"]): r["content"]
                    for r in await cur.fetchall()
                }

        for ag in agents:
            key = (ag["task_id"], ag["name"])
            stats = msg_stats.get(key, {})
            ag["msg_count"]   = stats.get("msg_count", 0)
            ag["last_msg_at"] = stats.get("last_msg_at")

            raw_last = last_msgs.get(key, "")
            try:
                parsed = json.loads(raw_last or "{}")
                ag["last_message"] = (
                    parsed.get("content")
                    or parsed.get("text")
                    or parsed.get("result")
                    or raw_last
                )[:300]
            except Exception:
                ag["last_message"] = (raw_last or "")[:300]

            ts = ag.get("task_status") or ""
            if ts == "running":
                ag["status"] = "active"
            elif ts == "paused":
                ag["status"] = "paused"
            elif ts in ("done", "stopped"):
                ag["status"] = "idle"
            elif ts == "failed":
                ag["status"] = "failed"
            else:
                ag["status"] = ts or "unknown"

        return {
            "agents": agents,
            "items":  agents,   # alias attendu par certains patterns frontend
            "total":  len(agents),
        }

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/agents", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/agents erreur : %s", exc)
        raise HTTPException(500, "Erreur interne agents : " + str(exc)[:200])


# ── GET /api/tasks ────────────────────────────────────────────────────────────
@router.get("/api/tasks")
async def get_tasks(
    page:     int = 1,
    per_page: int = 20,
    limit:    int = 0,
    status:   Optional[str] = None,
):
    """Liste paginée des tâches.

    Paramètres query :
    - page     : page courante (1-based, défaut 1)
    - per_page : taille de page (défaut 20, max 100)
    - limit    : alias simple — si > 0, retourne les N premières (prioritaire)
    - status   : filtre sur le statut (running|idle|done|failed|paused)

    Retourne :
    - items       : liste des tâches
    - total       : nombre total (avec filtre)
    - page, pages : info pagination
    """
    # Bornes
    page     = max(1, page)
    per_page = max(1, min(per_page, 100))

    # `limit` prend le dessus sur la pagination si spécifié
    if limit > 0:
        per_page = min(limit, 200)
        page = 1

    offset = (page - 1) * per_page

    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            db.row_factory = aiosqlite.Row

            if status:
                async with db.execute(
                    "SELECT COUNT(*) AS cnt FROM tasks WHERE status = ?",
                    (status,),
                ) as cur:
                    total = (await cur.fetchone())["cnt"]
                async with db.execute(
                    "SELECT id, objective, status, iteration, max_iterations, "
                    "       max_agents, web_enabled, chef_model, company_mode, "
                    "       target_path, created_at "
                    "FROM tasks WHERE status = ? "
                    "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (status, per_page, offset),
                ) as cur:
                    items = [dict(r) for r in await cur.fetchall()]
            else:
                async with db.execute(
                    "SELECT COUNT(*) AS cnt FROM tasks"
                ) as cur:
                    total = (await cur.fetchone())["cnt"]
                async with db.execute(
                    "SELECT id, objective, status, iteration, max_iterations, "
                    "       max_agents, web_enabled, chef_model, company_mode, "
                    "       target_path, created_at "
                    "FROM tasks "
                    "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (per_page, offset),
                ) as cur:
                    items = [dict(r) for r in await cur.fetchall()]

        pages = max(1, (total + per_page - 1) // per_page)
        return {
            "items":    items,
            "total":    total,
            "page":     page,
            "per_page": per_page,
            "pages":    pages,
        }

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/tasks", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/tasks erreur : %s", exc)
        raise HTTPException(500, "Erreur interne tasks : " + str(exc)[:200])


# ── GET /api/activity ─────────────────────────────────────────────────────────
@router.get("/api/activity")
async def get_activity(hours: int = 24):
    """Historique d'activité sur les dernières N heures (défaut 24, max 168).

    Retourne une timeline fusionnée de :
    - Créations de tâches
    - Messages agents importants (step, done, error, tool_call)
    - Erreurs système (error_log)
    Triée par horodatage décroissant, limitée à 200 entrées.
    """
    hours = max(1, min(hours, 168))
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).isoformat()

        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                "SELECT id, objective, status, created_at "
                "FROM tasks WHERE created_at >= ? "
                "ORDER BY created_at DESC LIMIT 100",
                (cutoff,),
            ) as cur:
                task_rows = await cur.fetchall()

            async with db.execute(
                "SELECT m.id, m.task_id, m.agent, m.kind, "
                "       m.content, m.created_at "
                "FROM messages m "
                "WHERE m.created_at >= ? "
                "  AND m.kind IN ('step','done','error','tool_call','assistant') "
                "ORDER BY m.created_at DESC LIMIT 150",
                (cutoff,),
            ) as cur:
                msg_rows = await cur.fetchall()

            async with db.execute(
                "SELECT id, timestamp AS created_at, endpoint, "
                "       error_type, message "
                "FROM error_log WHERE timestamp >= ? "
                "ORDER BY timestamp DESC LIMIT 50",
                (cutoff,),
            ) as cur:
                err_rows = await cur.fetchall()

        timeline = []

        for r in task_rows:
            timeline.append({
                "ts":      r["created_at"],
                "kind":    "task_created",
                "task_id": r["id"],
                "label":   f"Tâche #{r['id']} créée : {str(r['objective'])[:80]}",
                "status":  r["status"],
            })

        for r in msg_rows:
            text = ""
            try:
                payload = json.loads(r["content"] or "{}")
                text = (
                    payload.get("content")
                    or payload.get("text")
                    or payload.get("result")
                    or r["content"]
                    or ""
                )[:120]
            except Exception:
                text = (r["content"] or "")[:120]

            timeline.append({
                "ts":      r["created_at"],
                "kind":    r["kind"],
                "task_id": r["task_id"],
                "agent":   r["agent"],
                "label":   text,
            })

        for r in err_rows:
            timeline.append({
                "ts":       r["created_at"],
                "kind":     "system_error",
                "endpoint": r["endpoint"],
                "label":    (
                    f"Erreur {r['error_type']} — "
                    f"{str(r['message'])[:100]}"
                ),
            })

        timeline.sort(key=lambda x: x.get("ts") or "", reverse=True)
        timeline = timeline[:200]

        return {
            "window_hours": hours,
            "count":        len(timeline),
            "events":       timeline,
        }

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/activity", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/activity erreur : %s", exc)
        raise HTTPException(500, "Erreur interne activity : " + str(exc)[:200])


# ── POST /api/monitoring/reset ────────────────────────────────────────────────
@router.post("/api/monitoring/reset")
async def reset_monitoring():
    """Remet à zéro les compteurs de tokens et vide le journal d'erreurs.

    Publie un événement SSE 'notification' visible comme toast dans le frontend.
    """
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=15.0) as db:
            await db.execute("DELETE FROM token_usage")
            await db.execute("DELETE FROM error_log")
            await db.commit()

        publish_event(
            "notification",
            message="Compteurs de monitoring réinitialisés",
            level="info",
        )
        log.info("[api_control] Monitoring réinitialisé.")
        return {"status": "ok", "message": "Compteurs réinitialisés", "ts": _now_iso()}

    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error("/api/monitoring/reset", type(exc).__name__, str(exc), tb)
        log.error("[api_control] /api/monitoring/reset erreur : %s", exc)
        raise HTTPException(500, "Erreur interne reset : " + str(exc)[:200])


# ── POST /api/agents/{agent_id}/stop ──────────────────────────────────────────
# NOTE ARCHITECTURALE : team.py gère le cycle de vie des tâches (pause/stop).
# Un agent individuel ne peut pas être arrêté indépendamment de sa tâche :
# les agents s'exécutent dans la coroutine _run_task() et ne sont pas des
# processus indépendants.
# Cet endpoint répond avec les infos nécessaires pour rediriger vers
# POST /api/tasks/{task_id}/stop (endpoint team.py).
@router.post("/api/agents/{agent_id}/stop")
async def stop_agent(agent_id: int):
    """Informations pour l'arrêt d'un agent individuel.

    Les agents s'exécutent dans le contexte de leur tâche parente et ne
    peuvent pas être arrêtés indépendamment. Cet endpoint retourne les
    informations nécessaires pour arrêter la tâche parente.

    Retourne status='not_supported' avec l'URL de l'action correcte :
        POST /api/tasks/{task_id}/stop
    """
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=10.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT ta.id, ta.task_id, ta.name, t.status AS task_status "
                "FROM task_agents ta "
                "LEFT JOIN tasks t ON t.id = ta.task_id "
                "WHERE ta.id = ?",
                (agent_id,),
            ) as cur:
                row = await cur.fetchone()

        if not row:
            raise HTTPException(404, f"Agent {agent_id} introuvable")

        return {
            "status":      "not_supported",
            "agent_id":    agent_id,
            "agent_name":  row["name"],
            "task_id":     row["task_id"],
            "task_status": row["task_status"],
            "message":     (
                f"Les agents ne peuvent pas être arrêtés individuellement. "
                f"Pour arrêter l'agent '{row['name']}', utilisez : "
                f"POST /api/tasks/{row['task_id']}/stop"
            ),
        }

    except HTTPException:
        raise
    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error(
            f"/api/agents/{agent_id}/stop",
            type(exc).__name__, str(exc), tb,
        )
        raise HTTPException(500, "Erreur interne : " + str(exc)[:200])


# ── GET /api/events (SSE) ─────────────────────────────────────────────────────
@router.get("/api/events")
async def sse_events(
    request: Request,
    api_key: Annotated[
        Optional[str],
        Query(
            description=(
                "Clé API (alternative au header X-API-Key). "
                "Nécessaire car EventSource JS ne supporte pas les headers custom. "
                "Le middleware d'auth doit accepter ce paramètre — voir install.md."
            )
        ),
    ] = None,
):
    """Flux Server-Sent Events unifié pour le temps réel.

    Authentification :
        Header `X-API-Key` pour les requêtes fetch ordinaires.
        Query param `?api_key=` pour EventSource (pas de header custom possible).
        Le middleware d'auth (middleware_auth.py) doit lire le query param
        `api_key` en priorité 3 — voir api_control_install.md section 4.

    Événements émis (format `event: <type>\\ndata: {json}\\n\\n`) :
        connected             — confirmation de connexion
        heartbeat             — keepalive toutes les 30 s
        task_update           — cycle de vie tâche (+ variant task.update)
        agent_update          — activité agent (+ variant agent.update)
        app_update            — changement app (+ variant app.update)
        notification          — toast {message, level}
        monitoring_reset      — compteurs réinitialisés

    Utilisation JS ::

        const key = localStorage.getItem('ORCH_API_KEY') || '';
        const url = '/api/events' + (key ? '?api_key=' + key : '');
        const es  = new EventSource(url);

        es.addEventListener('task_update', e => {
            const d = JSON.parse(e.data);
            console.log('task:', d.task_id, d.status);
        });
        es.onmessage = e => {   // reçoit TOUS les events
            const d = JSON.parse(e.data);
            if (d.type === 'heartbeat') return;
        };
    """
    queue = broadcaster.subscribe()

    async def _generate() -> AsyncGenerator[str, None]:
        """Génère le flux SSE pour un client unique."""
        welcome = {
            "type":    "connected",
            "ts":      _now_iso(),
            "message": "Connecté au flux d'événements",
            "version": _VERSION,
        }
        yield _sse_frame("connected", welcome)

        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=30.0)
                    event_type = item.get("event_type", "message")
                    payload    = item.get("payload", item)

                    # Frame principal avec `event:` pour addEventListener()
                    yield _sse_frame(event_type, payload)

                    # Variant avec point (task_update -> task.update)
                    # pour compatibilité avec les deux conventions du frontend
                    if "_" in event_type:
                        dot_variant = event_type.replace("_", ".", 1)
                        yield _sse_frame(dot_variant, payload)

                except asyncio.TimeoutError:
                    # Heartbeat — maintient la connexion vivante (30 s)
                    hb = {"type": "heartbeat", "ts": _now_iso()}
                    yield _sse_frame("heartbeat", hb)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.warning("[api_control] SSE client déconnecté : %s", exc)
        finally:
            broadcaster.unsubscribe(queue)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ── Middleware optionnel : log automatique des erreurs 5xx ─────────────────────
async def error_logging_middleware(request: Request, call_next):
    """Middleware FastAPI qui enregistre les erreurs 5xx dans error_log.

    Optionnel — à ajouter dans orchestrator.py ::

        from api_control import error_logging_middleware
        app.middleware('http')(error_logging_middleware)
    """
    try:
        response = await call_next(request)
        if response.status_code >= 500:
            await _log_error(
                str(request.url.path),
                "HTTP_5xx",
                f"Statut {response.status_code} sur "
                f"{request.method} {request.url.path}",
            )
        return response
    except Exception as exc:
        tb = traceback.format_exc()
        await _log_error(
            str(request.url.path),
            type(exc).__name__,
            str(exc),
            tb,
        )
        raise
