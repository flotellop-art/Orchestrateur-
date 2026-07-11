"""Planifications UTC persistantes branchees sur :mod:`durable_queue`.

Trois rythmes sans dependance externe sont proposes : intervalle, quotidien et
hebdomadaire. Le calcul est pur et strictement en UTC. Chaque occurrence est
envoyee dans la file avec une cle idempotente stable ; un redemarrage entre
l'envoi et l'avancement du calendrier ne cree donc pas de doublon.
"""
from __future__ import annotations

import json
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

import aiosqlite

from durable_queue import (
    DurableQueue,
    IdempotencyConflict as QueueIdempotencyConflict,
    Job,
    MAX_PAYLOAD_BYTES as MAX_QUEUE_PAYLOAD_BYTES,
    ValidationError as QueueValidationError,
    datetime_from_text,
    datetime_to_text,
    normalize_utc,
    utc_now,
)


MAX_AUTOMATION_PAYLOAD_BYTES = 64 * 1024
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}\Z", re.ASCII)


class AutomationError(RuntimeError):
    pass


class AutomationValidationError(AutomationError, ValueError):
    pass


class AutomationConflict(AutomationError):
    pass


class ScheduleKind(str, Enum):
    INTERVAL = "interval"
    DAILY = "daily"
    WEEKLY = "weekly"


class AutomationState(str, Enum):
    ENABLED = "enabled"
    PAUSED = "paused"
    CANCELLED = "cancelled"


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise AutomationValidationError(
            f"{label} doit etre un identifiant ASCII de 1 a 192 caracteres."
        )
    return value


def _json_text(
    value: Any,
    label: str,
    *,
    limit: int = MAX_AUTOMATION_PAYLOAD_BYTES,
) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise AutomationValidationError(f"{label} doit etre du JSON valide.") from exc
    if len(encoded.encode("utf-8")) > limit:
        raise AutomationValidationError(f"{label} est trop volumineux.")
    return encoded


def _occurrence_payload(
    automation_id: str, occurrence_text: str, payload: Any
) -> dict[str, Any]:
    return {
        "automation_id": automation_id,
        "scheduled_for": occurrence_text,
        "payload": payload,
    }


def _validate_occurrence_payload(
    automation_id: str, occurrence_text: str, payload: Any
) -> None:
    # La file borne le JSON apres ajout de cette enveloppe. Valider seulement
    # le payload utilisateur laisserait passer une valeur qui echoue ensuite
    # a chaque tour du planificateur.
    _json_text(
        _occurrence_payload(automation_id, occurrence_text, payload),
        "payload final de l'automatisation",
        limit=MAX_QUEUE_PAYLOAD_BYTES,
    )


def _utc(value: datetime, label: str) -> datetime:
    try:
        return normalize_utc(value, label)
    except QueueValidationError as exc:
        raise AutomationValidationError(str(exc)) from exc


def _clock_part(value: object, label: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= maximum:
        raise AutomationValidationError(f"{label} doit etre compris entre 0 et {maximum}.")
    return value


@dataclass(frozen=True, slots=True)
class Schedule:
    kind: ScheduleKind
    interval_seconds: int | None = None
    anchor: datetime | None = None
    weekday: int | None = None
    hour: int = 0
    minute: int = 0
    second: int = 0

    def __post_init__(self) -> None:
        try:
            kind = self.kind if isinstance(self.kind, ScheduleKind) else ScheduleKind(self.kind)
        except ValueError as exc:
            raise AutomationValidationError("Type de planification invalide.") from exc
        object.__setattr__(self, "kind", kind)

        if kind is ScheduleKind.INTERVAL:
            if (
                not isinstance(self.interval_seconds, int)
                or isinstance(self.interval_seconds, bool)
                or not 1 <= self.interval_seconds <= 31_536_000
            ):
                raise AutomationValidationError(
                    "interval_seconds doit etre compris entre 1 et 31536000."
                )
            if self.anchor is None:
                raise AutomationValidationError("Une ancre UTC est obligatoire.")
            try:
                anchor = _utc(self.anchor, "anchor")
            except QueueValidationError as exc:
                raise AutomationValidationError(str(exc)) from exc
            object.__setattr__(self, "anchor", anchor)
            if self.weekday is not None or any((self.hour, self.minute, self.second)):
                raise AutomationValidationError(
                    "Un intervalle n'accepte ni jour de semaine ni heure fixe."
                )
            return

        if self.interval_seconds is not None or self.anchor is not None:
            raise AutomationValidationError(
                "Les champs d'intervalle ne sont pas permis pour ce rythme."
            )
        object.__setattr__(self, "hour", _clock_part(self.hour, "hour", 23))
        object.__setattr__(self, "minute", _clock_part(self.minute, "minute", 59))
        object.__setattr__(self, "second", _clock_part(self.second, "second", 59))
        if kind is ScheduleKind.DAILY:
            if self.weekday is not None:
                raise AutomationValidationError("Un rythme quotidien n'accepte pas weekday.")
        else:
            if (
                not isinstance(self.weekday, int)
                or isinstance(self.weekday, bool)
                or not 0 <= self.weekday <= 6
            ):
                raise AutomationValidationError(
                    "weekday doit etre compris entre 0 (lundi) et 6 (dimanche)."
                )

    @classmethod
    def interval(cls, seconds: int, *, anchor: datetime) -> "Schedule":
        return cls(ScheduleKind.INTERVAL, interval_seconds=seconds, anchor=anchor)

    @classmethod
    def daily(cls, *, hour: int, minute: int = 0, second: int = 0) -> "Schedule":
        return cls(ScheduleKind.DAILY, hour=hour, minute=minute, second=second)

    @classmethod
    def weekly(
        cls,
        *,
        weekday: int,
        hour: int,
        minute: int = 0,
        second: int = 0,
    ) -> "Schedule":
        return cls(
            ScheduleKind.WEEKLY,
            weekday=weekday,
            hour=hour,
            minute=minute,
            second=second,
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "Schedule":
        if not isinstance(payload, Mapping):
            raise AutomationValidationError("schedule doit etre un objet.")
        schedule_type = payload.get("type")
        try:
            kind = ScheduleKind(schedule_type)
        except (TypeError, ValueError) as exc:
            raise AutomationValidationError("schedule.type invalide.") from exc
        allowed = {
            ScheduleKind.INTERVAL: {"type", "seconds", "anchor"},
            ScheduleKind.DAILY: {"type", "hour", "minute", "second"},
            ScheduleKind.WEEKLY: {"type", "weekday", "hour", "minute", "second"},
        }[kind]
        unknown = set(payload) - allowed
        if unknown:
            raise AutomationValidationError(
                "Champs de planification inconnus : " + ", ".join(sorted(unknown))
            )
        if kind is ScheduleKind.INTERVAL:
            anchor_value = payload.get("anchor")
            if not isinstance(anchor_value, str):
                raise AutomationValidationError("schedule.anchor doit etre une date UTC ISO 8601.")
            try:
                anchor = datetime_from_text(anchor_value)
            except (TypeError, ValueError) as exc:
                raise AutomationValidationError("schedule.anchor est invalide.") from exc
            if anchor is None:
                raise AutomationValidationError("schedule.anchor est obligatoire.")
            return cls.interval(payload.get("seconds"), anchor=anchor)  # type: ignore[arg-type]
        if kind is ScheduleKind.DAILY:
            return cls.daily(
                hour=payload.get("hour"),  # type: ignore[arg-type]
                minute=payload.get("minute", 0),
                second=payload.get("second", 0),
            )
        return cls.weekly(
            weekday=payload.get("weekday"),  # type: ignore[arg-type]
            hour=payload.get("hour"),  # type: ignore[arg-type]
            minute=payload.get("minute", 0),
            second=payload.get("second", 0),
        )

    def to_payload(self) -> dict[str, Any]:
        if self.kind is ScheduleKind.INTERVAL:
            return {
                "type": self.kind.value,
                "seconds": self.interval_seconds,
                "anchor": datetime_to_text(self.anchor),  # type: ignore[arg-type]
            }
        result: dict[str, Any] = {
            "type": self.kind.value,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }
        if self.kind is ScheduleKind.WEEKLY:
            result["weekday"] = self.weekday
        return result

    def next_after(self, after: datetime) -> datetime:
        """Renvoie la premiere occurrence strictement posterieure a ``after``."""
        try:
            reference = _utc(after, "after")
        except QueueValidationError as exc:
            raise AutomationValidationError(str(exc)) from exc
        if self.kind is ScheduleKind.INTERVAL:
            anchor = self.anchor  # valide dans __post_init__
            assert anchor is not None and self.interval_seconds is not None
            if reference < anchor:
                return anchor
            delta = reference - anchor
            delta_microseconds = (
                delta.days * 86_400_000_000
                + delta.seconds * 1_000_000
                + delta.microseconds
            )
            interval_microseconds = self.interval_seconds * 1_000_000
            steps = delta_microseconds // interval_microseconds + 1
            return anchor + timedelta(seconds=steps * self.interval_seconds)

        candidate_time = time(self.hour, self.minute, self.second, tzinfo=timezone.utc)
        if self.kind is ScheduleKind.DAILY:
            candidate = datetime.combine(reference.date(), candidate_time)
            if candidate <= reference:
                candidate += timedelta(days=1)
            return candidate

        assert self.weekday is not None
        days_ahead = (self.weekday - reference.weekday()) % 7
        candidate = datetime.combine(reference.date() + timedelta(days=days_ahead), candidate_time)
        if candidate <= reference:
            candidate += timedelta(days=7)
        return candidate


@dataclass(frozen=True, slots=True)
class Automation:
    id: str
    idempotency_key: str
    name: str
    schedule: Schedule
    job_kind: str
    payload: Any = field(repr=False)
    state: AutomationState
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime


def _row_to_automation(row: aiosqlite.Row) -> Automation:
    automation_id = _identifier(row["id"], "automation.id")
    idempotency_key = _identifier(
        row["idempotency_key"], "automation.idempotency_key"
    )
    job_kind = _identifier(row["job_kind"], "automation.job_kind")
    name = row["name"]
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 120:
        raise AutomationValidationError("automation.name est invalide.")
    return Automation(
        id=automation_id,
        idempotency_key=idempotency_key,
        name=name.strip(),
        schedule=Schedule.from_payload(json.loads(row["schedule_json"])),
        job_kind=job_kind,
        payload=json.loads(row["payload_json"]),
        state=AutomationState(row["state"]),
        next_run_at=datetime_from_text(row["next_run_at"]),  # type: ignore[arg-type]
        created_at=datetime_from_text(row["created_at"]),  # type: ignore[arg-type]
        updated_at=datetime_from_text(row["updated_at"]),  # type: ignore[arg-type]
    )


class AutomationStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    @asynccontextmanager
    async def _db(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(str(self.db_path), timeout=30.0)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA busy_timeout=30000")
            yield db
        finally:
            await db.close()

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with self._db() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS automations (
                    id                TEXT PRIMARY KEY,
                    idempotency_key   TEXT NOT NULL UNIQUE,
                    name              TEXT NOT NULL,
                    schedule_json     TEXT NOT NULL,
                    job_kind          TEXT NOT NULL,
                    payload_json      TEXT NOT NULL,
                    state             TEXT NOT NULL CHECK (state IN ('enabled','paused','cancelled')),
                    next_run_at       TEXT NOT NULL,
                    created_at        TEXT NOT NULL,
                    updated_at        TEXT NOT NULL
                )
                """
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_automations_due "
                "ON automations(state, next_run_at)"
            )
            await db.commit()

    async def create(
        self,
        *,
        name: str,
        schedule: Schedule,
        job_kind: str,
        payload: Any,
        idempotency_key: str,
        now: datetime | None = None,
    ) -> Automation:
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 120:
            raise AutomationValidationError("name doit contenir entre 1 et 120 caracteres.")
        name = name.strip()
        if not isinstance(schedule, Schedule):
            raise AutomationValidationError("schedule invalide.")
        job_kind = _identifier(job_kind, "job_kind")
        idempotency_key = _identifier(idempotency_key, "idempotency_key")
        schedule_json = _json_text(schedule.to_payload(), "schedule")
        current = _utc(now or utc_now(), "now")
        next_run = schedule.next_after(current)
        current_text = datetime_to_text(current)
        automation_id = uuid.uuid4().hex
        payload_json = _json_text(payload, "payload")
        _validate_occurrence_payload(
            automation_id, datetime_to_text(next_run), payload
        )
        async with self._db() as db:
            try:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """
                    INSERT INTO automations (
                        id, idempotency_key, name, schedule_json, job_kind,
                        payload_json, state, next_run_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'enabled', ?, ?, ?)
                    """,
                    (
                        automation_id,
                        idempotency_key,
                        name,
                        schedule_json,
                        job_kind,
                        payload_json,
                        datetime_to_text(next_run),
                        current_text,
                        current_text,
                    ),
                )
                await db.commit()
            except sqlite3.IntegrityError:
                await db.rollback()
                async with db.execute(
                    "SELECT * FROM automations WHERE idempotency_key=?",
                    (idempotency_key,),
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    raise
                if (
                    row["name"] != name
                    or row["schedule_json"] != schedule_json
                    or row["job_kind"] != job_kind
                    or row["payload_json"] != payload_json
                ):
                    raise AutomationConflict(
                        "Cette cle idempotente designe deja une autre planification."
                    ) from None
                return _row_to_automation(row)
            async with db.execute(
                "SELECT * FROM automations WHERE id=?", (automation_id,)
            ) as cursor:
                row = await cursor.fetchone()
            return _row_to_automation(row)

    async def get(self, automation_id: str) -> Automation | None:
        _identifier(automation_id, "automation_id")
        async with self._db() as db:
            async with db.execute(
                "SELECT * FROM automations WHERE id=?", (automation_id,)
            ) as cursor:
                row = await cursor.fetchone()
        return _row_to_automation(row) if row else None

    async def list(self, *, limit: int = 100) -> list[Automation]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise AutomationValidationError("limit doit etre compris entre 1 et 1000.")
        async with self._db() as db:
            async with db.execute(
                "SELECT * FROM automations ORDER BY created_at DESC LIMIT ?", (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
        return [_row_to_automation(row) for row in rows]

    async def cancel_for_task(
        self, task_id: int, *, now: datetime | None = None
    ) -> int:
        """Annule toutes les planifications d'une tache, sans limite de page."""
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
            raise AutomationValidationError("task_id doit etre un entier positif.")
        current_text = datetime_to_text(_utc(now or utc_now(), "now"))
        matched: list[int] = []
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT rowid AS _storage_rowid,payload_json FROM automations "
                "WHERE state!='cancelled'"
            ) as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    # Une ancienne ligne corrompue ne doit pas empêcher la
                    # suppression d'une tâche sans rapport. La mettre en
                    # quarantaine dans la même transaction évite aussi de
                    # reproduire l'échec au prochain appel.
                    await db.execute(
                        "UPDATE automations SET state='cancelled', updated_at=? "
                        "WHERE rowid=? AND state!='cancelled'",
                        (current_text, int(row["_storage_rowid"])),
                    )
                    continue
                if not isinstance(payload, dict):
                    # AutomationStore reste générique : un payload JSON
                    # scalaire ou tableau peut être valide pour un autre type
                    # de travail et n'est simplement pas lié à une tâche.
                    continue
                payload_task_id = payload.get("task_id")
                if (
                    isinstance(payload_task_id, int)
                    and not isinstance(payload_task_id, bool)
                    and payload_task_id == task_id
                ):
                    matched.append(int(row["_storage_rowid"]))
            for row_id in matched:
                await db.execute(
                    "UPDATE automations SET state='cancelled', updated_at=? "
                    "WHERE rowid=? AND state!='cancelled'",
                    (current_text, row_id),
                )
            await db.commit()
        return len(matched)

    async def pause(self, automation_id: str, *, now: datetime | None = None) -> Automation | None:
        return await self._set_state(
            automation_id, AutomationState.PAUSED, now=now, recompute=False
        )

    async def cancel(self, automation_id: str, *, now: datetime | None = None) -> Automation | None:
        return await self._set_state(
            automation_id, AutomationState.CANCELLED, now=now, recompute=False
        )

    async def resume(self, automation_id: str, *, now: datetime | None = None) -> Automation | None:
        return await self._set_state(
            automation_id, AutomationState.ENABLED, now=now, recompute=True
        )

    async def _set_state(
        self,
        automation_id: str,
        state: AutomationState,
        *,
        now: datetime | None,
        recompute: bool,
    ) -> Automation | None:
        _identifier(automation_id, "automation_id")
        current = _utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT * FROM automations WHERE id=?", (automation_id,)
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                await db.commit()
                return None
            current_state = AutomationState(row["state"])
            allowed_transition = (
                current_state is state
                or (state is AutomationState.PAUSED and current_state is AutomationState.ENABLED)
                or (
                    state is AutomationState.CANCELLED
                    and current_state in {AutomationState.ENABLED, AutomationState.PAUSED}
                )
                or (
                    state is AutomationState.ENABLED
                    and current_state is AutomationState.PAUSED
                )
            )
            if not allowed_transition or current_state is AutomationState.CANCELLED:
                await db.commit()
                return _row_to_automation(row)
            if current_state is state:
                await db.commit()
                return _row_to_automation(row)
            next_run_text = row["next_run_at"]
            if recompute:
                schedule = Schedule.from_payload(json.loads(row["schedule_json"]))
                next_run_text = datetime_to_text(schedule.next_after(current))
            await db.execute(
                "UPDATE automations SET state=?, next_run_at=?, updated_at=? WHERE id=?",
                (state.value, next_run_text, current_text, automation_id),
            )
            async with db.execute(
                "SELECT * FROM automations WHERE id=?", (automation_id,)
            ) as cursor:
                updated = await cursor.fetchone()
            await db.commit()
        return _row_to_automation(updated)

    async def _quarantine_invalid(
        self, row_id: int, *, now_text: str
    ) -> None:
        """Ecarte une ligne invalide afin que les suivantes restent executables."""
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                "UPDATE automations SET state='cancelled', updated_at=? "
                "WHERE rowid=? AND state='enabled'",
                (now_text, row_id),
            )
            await db.commit()

    async def dispatch_due(
        self,
        queue: DurableQueue,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """Enfile les occurrences dues, avec rattrapage borne par ``limit``.

        L'enfilage precede l'avancement du calendrier. Si le processus s'arrete
        entre les deux, la meme cle d'occurrence sera rejouee sans creer un
        second travail.
        """
        if not isinstance(queue, DurableQueue):
            raise AutomationValidationError("queue invalide.")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise AutomationValidationError("limit doit etre compris entre 1 et 1000.")
        current = _utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        dispatched: list[Job] = []

        while len(dispatched) < limit:
            async with self._db() as db:
                async with db.execute(
                    """
                    SELECT rowid AS _storage_rowid, * FROM automations
                    WHERE state='enabled' AND next_run_at<=?
                    ORDER BY next_run_at ASC, created_at ASC
                    LIMIT 1
                    """,
                    (current_text,),
                ) as cursor:
                    row = await cursor.fetchone()
            if row is None:
                break

            try:
                automation = _row_to_automation(row)
            except (
                AutomationValidationError,
                QueueValidationError,
                TypeError,
                ValueError,
                KeyError,
            ):
                await self._quarantine_invalid(
                    int(row["_storage_rowid"]), now_text=current_text
                )
                continue
            occurrence = automation.next_run_at
            occurrence_text = datetime_to_text(occurrence)
            occurrence_key = f"automation:{automation.id}:{occurrence_text}"
            job_payload = _occurrence_payload(
                automation.id, occurrence_text, automation.payload
            )
            try:
                _validate_occurrence_payload(
                    automation.id, occurrence_text, automation.payload
                )
                job = await queue.enqueue(
                    kind=automation.job_kind,
                    payload=job_payload,
                    idempotency_key=occurrence_key,
                    available_at=occurrence,
                    now=current,
                )
            except (
                AutomationValidationError,
                QueueValidationError,
                QueueIdempotencyConflict,
            ):
                await self._quarantine_invalid(
                    int(row["_storage_rowid"]), now_text=current_text
                )
                continue
            # Une remise en route apres une longue absence declenche au plus
            # une execution de rattrapage par planification. Les occurrences
            # intermediaires sont regroupees pour eviter une tempete de couts.
            next_run = automation.schedule.next_after(current)

            async with self._db() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    UPDATE automations
                    SET next_run_at=?, updated_at=?
                    WHERE id=? AND state='enabled' AND next_run_at=?
                    """,
                    (
                        datetime_to_text(next_run),
                        current_text,
                        automation.id,
                        occurrence_text,
                    ),
                )
                advanced = cursor.rowcount == 1
                await db.commit()
            if advanced:
                dispatched.append(job)

        return dispatched


__all__ = [
    "Automation",
    "AutomationConflict",
    "AutomationError",
    "AutomationState",
    "AutomationStore",
    "AutomationValidationError",
    "MAX_AUTOMATION_PAYLOAD_BYTES",
    "Schedule",
    "ScheduleKind",
]
