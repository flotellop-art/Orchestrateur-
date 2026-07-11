"""API humaine pour les planifications persistantes.

Le routeur est volontairement sans dependance directe vers ``team.py``. Le
serveur injecte les services au demarrage avec :func:`configure`. Seul le type
de travail ``run_task`` est accepte et le payload public est reduit a un
``task_id`` existant et, eventuellement, un nom de canal deja configure.
"""
from __future__ import annotations

import inspect
import json
import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse

from automations import (
    Automation,
    AutomationConflict,
    AutomationStore,
    AutomationValidationError,
    Schedule,
)
from durable_queue import DurableQueue
from messaging import MessagingGateway


STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_CREATE_BODY_BYTES = 32 * 1024
_IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}\Z", re.ASCII)
_AUTOMATION_ID_RE = re.compile(r"[a-f0-9]{32}\Z", re.ASCII)
_CHANNEL_RE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z", re.ASCII)

TaskExists = Callable[[int], bool | Awaitable[bool] | Mapping[str, Any] | Awaitable[Mapping[str, Any] | None] | None]


@dataclass(frozen=True, slots=True)
class _Services:
    store: AutomationStore
    queue: DurableQueue
    messaging: MessagingGateway | None
    task_exists: TaskExists | None
    channel_names: tuple[str, ...]


_services: _Services | None = None
_services_error: str | None = None


def _configured_channel_names(gateway: MessagingGateway | None) -> tuple[str, ...]:
    if gateway is None:
        return ()
    # MessagingGateway conserve volontairement son registre comme detail
    # serveur. Cette API n'en extrait que les noms publics, jamais les endpoints.
    registry = getattr(gateway, "_registry", None)
    names = getattr(registry, "channel_names", ())
    if not isinstance(names, tuple) or any(not isinstance(name, str) for name in names):
        raise TypeError("Le registre de messagerie ne fournit pas de canaux valides.")
    return tuple(sorted(names))


def configure(
    store: AutomationStore,
    queue: DurableQueue,
    messaging: MessagingGateway | None = None,
    *,
    task_exists: TaskExists | None = None,
    channel_names: tuple[str, ...] | list[str] | None = None,
    task_lookup: TaskExists | None = None,
) -> None:
    """Injecte les services serveur utilises par le routeur.

    ``task_exists`` (alias d'integration : ``task_lookup``) peut etre une
    fonction synchrone ou asynchrone. Si elle est absente, la table ``tasks``
    de la base de ``AutomationStore`` est consultee en lecture seule.
    ``channel_names`` permet de reutiliser la liste deja extraite du registre
    serveur ; si une passerelle est aussi fournie, cette liste doit en etre un
    sous-ensemble.
    """
    if not isinstance(store, AutomationStore):
        raise TypeError("store doit etre une AutomationStore.")
    if not isinstance(queue, DurableQueue):
        raise TypeError("queue doit etre une DurableQueue.")
    if messaging is not None and not isinstance(messaging, MessagingGateway):
        raise TypeError("messaging doit etre une MessagingGateway ou None.")
    if task_exists is not None and task_lookup is not None:
        raise TypeError("Utiliser task_exists ou task_lookup, pas les deux.")
    checker = task_exists if task_exists is not None else task_lookup
    if checker is not None and not callable(checker):
        raise TypeError("task_exists doit etre appelable.")
    gateway_names = _configured_channel_names(messaging)
    if channel_names is None:
        public_channel_names = gateway_names
    else:
        if (
            not isinstance(channel_names, (tuple, list))
            or len(channel_names) > 64
            or any(
                not isinstance(name, str) or not _CHANNEL_RE.fullmatch(name)
                for name in channel_names
            )
        ):
            raise TypeError("channel_names contient un canal invalide.")
        public_channel_names = tuple(sorted(set(channel_names)))
        if messaging is not None and not set(public_channel_names).issubset(gateway_names):
            raise TypeError("channel_names contient un canal absent de la messagerie.")
    global _services, _services_error
    _services = _Services(
        store=store,
        queue=queue,
        messaging=messaging,
        task_exists=checker,
        channel_names=public_channel_names,
    )
    _services_error = None


def disable(reason: str) -> None:
    """Ferme l'API quand le planificateur n'est plus réellement actif."""
    message = " ".join(str(reason or "").split())
    if not message:
        message = "Les services de planification sont indisponibles."
    global _services, _services_error
    _services = None
    _services_error = message[:500]


def _get_services() -> _Services:
    if _services is None:
        raise HTTPException(
            503,
            _services_error or "Les planifications ne sont pas encore initialisees.",
        )
    return _services


async def _default_task_exists(store: AutomationStore, task_id: int):
    try:
        async with aiosqlite.connect(str(store.db_path), timeout=5.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id,execution_mode FROM tasks WHERE id=? LIMIT 1", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    except (sqlite3.Error, OSError):
        return False


async def _task_exists(services: _Services, task_id: int):
    if services.task_exists is None:
        return await _default_task_exists(services.store, task_id)
    try:
        result = services.task_exists(task_id)
        if inspect.isawaitable(result):
            result = await result
    except Exception:
        raise HTTPException(503, "La verification de la tache est indisponible.") from None
    if isinstance(result, Mapping):
        return result
    return True if result is True else None


def _automation_payload(automation: Automation) -> dict[str, Any]:
    payload = automation.payload if isinstance(automation.payload, dict) else {}
    public_payload: dict[str, Any] = {"task_id": payload.get("task_id")}
    if payload.get("notification_channel") is not None:
        public_payload["notification_channel"] = payload.get("notification_channel")
    return {
        "id": automation.id,
        "name": automation.name,
        "schedule": automation.schedule.to_payload(),
        "job_kind": automation.job_kind,
        "payload": public_payload,
        "state": automation.state.value,
        "next_run_at": automation.next_run_at.isoformat().replace("+00:00", "Z"),
        "created_at": automation.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": automation.updated_at.isoformat().replace("+00:00", "Z"),
    }


async def _read_json_object(request: Request) -> dict[str, Any]:
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            parsed_length = int(content_length)
            if parsed_length < 0:
                raise HTTPException(400, "Content-Length invalide.")
            if parsed_length > MAX_CREATE_BODY_BYTES:
                raise HTTPException(413, "La demande est trop volumineuse.")
        except ValueError:
            raise HTTPException(400, "Content-Length invalide.") from None
    raw_buffer = bytearray()
    async for chunk in request.stream():
        if len(raw_buffer) + len(chunk) > MAX_CREATE_BODY_BYTES:
            raise HTTPException(413, "La demande est trop volumineuse.")
        raw_buffer.extend(chunk)
    raw = bytes(raw_buffer)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(400, "Le corps JSON est invalide.") from None
    if not isinstance(value, dict):
        raise HTTPException(400, "Le corps doit etre un objet JSON.")
    return value


def _parse_create_body(body: dict[str, Any], channel_names: tuple[str, ...]) -> tuple[str, Schedule, dict[str, Any], str]:
    allowed_fields = {"name", "schedule", "job_kind", "payload", "idempotency_key"}
    if set(body) - allowed_fields:
        raise HTTPException(400, "La demande contient des champs interdits.")

    name = body.get("name")
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 120:
        raise HTTPException(400, "Le nom doit contenir entre 1 et 120 caracteres.")

    if body.get("job_kind") != "run_task":
        raise HTTPException(400, "Seul le type run_task peut etre planifie.")

    payload = body.get("payload")
    if not isinstance(payload, dict) or set(payload) - {"task_id", "notification_channel"}:
        raise HTTPException(400, "Le contenu de la tache planifiee est invalide.")
    task_id = payload.get("task_id")
    if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
        raise HTTPException(400, "task_id doit etre un entier positif.")

    notification_channel = payload.get("notification_channel")
    if notification_channel in (None, ""):
        notification_channel = None
    elif not isinstance(notification_channel, str) or notification_channel not in channel_names:
        raise HTTPException(400, "Le canal de notification n'est pas autorise.")

    schedule_payload = body.get("schedule")
    try:
        schedule = Schedule.from_payload(schedule_payload)
    except AutomationValidationError as exc:
        raise HTTPException(400, str(exc)) from None

    idempotency_key = body.get("idempotency_key")
    if idempotency_key is None:
        idempotency_key = "ui:" + uuid.uuid4().hex
    if not isinstance(idempotency_key, str) or not _IDEMPOTENCY_RE.fullmatch(idempotency_key):
        raise HTTPException(400, "idempotency_key est invalide.")

    stored_payload: dict[str, Any] = {"task_id": task_id}
    if notification_channel is not None:
        stored_payload["notification_channel"] = notification_channel
    return name.strip(), schedule, stored_payload, idempotency_key


def _automation_id(value: str) -> str:
    if not _AUTOMATION_ID_RE.fullmatch(value):
        raise HTTPException(404, "Planification introuvable.")
    return value


router = APIRouter(tags=["automations"])

try:
    from auth_middleware import PUBLIC_PATHS
    PUBLIC_PATHS.add("/automations")
except ImportError:  # pragma: no cover - utilisation autonome du routeur
    pass


@router.get("/automations", response_class=FileResponse)
async def automations_page() -> FileResponse:
    path = STATIC_DIR / "automations.html"
    if not path.is_file():
        raise HTTPException(404, "Page des planifications introuvable.")
    return FileResponse(
        path,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": (
                "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                "style-src 'self' 'unsafe-inline'; connect-src 'self'; "
                "img-src 'self' data:; object-src 'none'; base-uri 'none'; "
                "frame-ancestors 'none'; form-action 'self'"
            ),
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )


@router.get("/api/automations/channels")
async def list_notification_channels(response: Response) -> dict[str, list[str]]:
    response.headers["Cache-Control"] = "no-store"
    return {"channels": list(_get_services().channel_names)}


@router.get("/api/automations")
async def list_automations(
    response: Response,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    response.headers["Cache-Control"] = "no-store"
    services = _get_services()
    items = await services.store.list(limit=limit)
    return {"automations": [_automation_payload(item) for item in items]}


@router.post("/api/automations", status_code=201)
async def create_automation(request: Request, response: Response) -> dict[str, Any]:
    services = _get_services()
    body = await _read_json_object(request)
    name, schedule, payload, idempotency_key = _parse_create_body(
        body, services.channel_names
    )
    task_record = await _task_exists(services, payload["task_id"])
    if not task_record:
        raise HTTPException(404, "La tache choisie n'existe pas.")
    if isinstance(task_record, Mapping) and task_record.get("execution_mode") not in {None, "docker"}:
        raise HTTPException(
            400, "Seules les taches Docker isolees peuvent etre programmees."
        )
    try:
        automation = await services.store.create(
            name=name,
            schedule=schedule,
            job_kind="run_task",
            payload=payload,
            idempotency_key=idempotency_key,
        )
    except AutomationConflict as exc:
        raise HTTPException(409, str(exc)) from None
    except AutomationValidationError as exc:
        raise HTTPException(400, str(exc)) from None
    response.headers["Cache-Control"] = "no-store"
    return _automation_payload(automation)


async def _change_state(automation_id: str, action: str) -> dict[str, Any]:
    services = _get_services()
    automation_id = _automation_id(automation_id)
    if action == "pause":
        automation = await services.store.pause(automation_id)
    elif action == "resume":
        automation = await services.store.resume(automation_id)
    else:
        automation = await services.store.cancel(automation_id)
    if automation is None:
        raise HTTPException(404, "Planification introuvable.")
    return _automation_payload(automation)


@router.post("/api/automations/{automation_id}/pause")
async def pause_automation(automation_id: str, response: Response) -> dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    return await _change_state(automation_id, "pause")


@router.post("/api/automations/{automation_id}/resume")
async def resume_automation(automation_id: str, response: Response) -> dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    return await _change_state(automation_id, "resume")


@router.post("/api/automations/{automation_id}/cancel")
async def cancel_automation(automation_id: str, response: Response) -> dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    return await _change_state(automation_id, "cancel")


__all__ = ["configure", "router"]
