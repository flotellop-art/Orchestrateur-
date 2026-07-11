"""File de travaux SQLite durable, idempotente et recuperable apres crash.

Le module ne lance pas les travaux lui-meme. Un worker reclame un travail pour
une duree limitee (un *lease*), renouvelle ce bail pendant l'execution, puis
termine ou reporte le travail. Un bail expire est remis en file, ce qui permet
une reprise apres l'arret brutal du processus.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite


MAX_PAYLOAD_BYTES = 64 * 1024
MAX_RESULT_BYTES = 64 * 1024
MAX_ERROR_CODE_LENGTH = 96
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}\Z", re.ASCII)
_ERROR_CODE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z", re.ASCII)


class QueueError(RuntimeError):
    """Erreur de contrat de la file."""


class ValidationError(QueueError, ValueError):
    """Un argument ne respecte pas le contrat public."""


class IdempotencyConflict(QueueError):
    """Une cle idempotente existe deja pour un autre travail."""


class TaskReservationConflict(QueueError):
    """La tache a change d'etat avant la reservation de son travail."""


class LeaseLost(QueueError):
    """Le worker ne possede plus le bail associe au travail."""


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class Job:
    id: str
    idempotency_key: str
    kind: str
    payload: Any = field(repr=False)
    state: JobState
    priority: int
    attempts: int
    max_attempts: int
    base_backoff_seconds: float
    available_at: datetime
    lease_owner: str | None
    lease_until: datetime | None
    last_error_code: str | None
    result: Any | None = field(repr=False)
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class JobLease:
    """Capacite temporaire necessaire pour modifier un travail en cours."""

    job: Job
    token: str = field(repr=False)
    worker_id: str
    lease_until: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_utc(value: datetime, label: str = "datetime") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValidationError(f"{label} doit inclure un fuseau horaire.")
    return value.astimezone(timezone.utc)


def datetime_to_text(value: datetime) -> str:
    normalized = normalize_utc(value)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def datetime_from_text(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValidationError("La date doit inclure un fuseau horaire.")
    return parsed.astimezone(timezone.utc)


def _name(value: object, label: str) -> str:
    if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
        raise ValidationError(
            f"{label} doit etre un identifiant ASCII de 1 a 192 caracteres."
        )
    return value


def _json_text(value: Any, *, limit: int, label: str) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} doit etre du JSON valide.") from exc
    if len(encoded.encode("utf-8")) > limit:
        raise ValidationError(f"{label} depasse la taille maximale de {limit} octets.")
    return encoded


def _error_code(value: object) -> str:
    if not isinstance(value, str) or not _ERROR_CODE_RE.fullmatch(value):
        raise ValidationError(
            "error_code doit etre un code ASCII court, pas un message d'exception."
        )
    return value


def _row_to_job(row: aiosqlite.Row) -> Job:
    return Job(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        kind=row["kind"],
        payload=json.loads(row["payload_json"]),
        state=JobState(row["state"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        base_backoff_seconds=row["base_backoff_seconds"],
        available_at=datetime_from_text(row["available_at"]),  # type: ignore[arg-type]
        lease_owner=row["lease_owner"],
        lease_until=datetime_from_text(row["lease_until"]),
        last_error_code=row["last_error_code"],
        result=json.loads(row["result_json"]) if row["result_json"] is not None else None,
        created_at=datetime_from_text(row["created_at"]),  # type: ignore[arg-type]
        updated_at=datetime_from_text(row["updated_at"]),  # type: ignore[arg-type]
        started_at=datetime_from_text(row["started_at"]),
        finished_at=datetime_from_text(row["finished_at"]),
    )


class DurableQueue:
    """File multi-workers stockee dans un fichier SQLite.

    Les methodes sont asynchrones pour s'integrer a l'orchestrateur. Chaque
    operation ouvre une connexion courte et utilise une transaction immediate
    pour les transitions concurrentes.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    @asynccontextmanager
    async def _db(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(str(self.db_path), timeout=30.0)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA foreign_keys=ON")
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
                CREATE TABLE IF NOT EXISTS durable_jobs (
                    id                    TEXT PRIMARY KEY,
                    idempotency_key       TEXT NOT NULL UNIQUE,
                    kind                  TEXT NOT NULL,
                    payload_json          TEXT NOT NULL,
                    state                 TEXT NOT NULL CHECK (state IN (
                        'queued','running','succeeded','failed','paused','cancelled'
                    )),
                    priority              INTEGER NOT NULL DEFAULT 0,
                    attempts              INTEGER NOT NULL DEFAULT 0,
                    max_attempts          INTEGER NOT NULL,
                    base_backoff_seconds  REAL NOT NULL,
                    available_at          TEXT NOT NULL,
                    lease_owner           TEXT,
                    lease_token           TEXT,
                    lease_until           TEXT,
                    last_error_code       TEXT,
                    result_json           TEXT,
                    created_at            TEXT NOT NULL,
                    updated_at            TEXT NOT NULL,
                    started_at            TEXT,
                    finished_at           TEXT
                )
                """
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_durable_jobs_ready "
                "ON durable_jobs(state, available_at, priority, created_at)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_durable_jobs_lease "
                "ON durable_jobs(state, lease_until)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_durable_jobs_created_at "
                "ON durable_jobs(created_at)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_durable_jobs_terminal_retention "
                "ON durable_jobs(state, finished_at)"
            )
            await db.commit()

    async def enqueue(
        self,
        *,
        kind: str,
        payload: Any,
        idempotency_key: str,
        available_at: datetime | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        base_backoff_seconds: float = 5.0,
        now: datetime | None = None,
    ) -> Job:
        kind = _name(kind, "kind")
        idempotency_key = _name(idempotency_key, "idempotency_key")
        payload_json = _json_text(payload, limit=MAX_PAYLOAD_BYTES, label="payload")
        if not isinstance(priority, int) or isinstance(priority, bool) or not -1000 <= priority <= 1000:
            raise ValidationError("priority doit etre un entier entre -1000 et 1000.")
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or not 1 <= max_attempts <= 100:
            raise ValidationError("max_attempts doit etre un entier entre 1 et 100.")
        if (
            not isinstance(base_backoff_seconds, (int, float))
            or isinstance(base_backoff_seconds, bool)
            or not math.isfinite(float(base_backoff_seconds))
            or not 0 <= float(base_backoff_seconds) <= 86400
        ):
            raise ValidationError("base_backoff_seconds doit etre compris entre 0 et 86400.")

        current = normalize_utc(now or utc_now(), "now")
        ready = normalize_utc(available_at or current, "available_at")
        current_text = datetime_to_text(current)
        job_id = uuid.uuid4().hex
        async with self._db() as db:
            try:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """
                    INSERT INTO durable_jobs (
                        id, idempotency_key, kind, payload_json, state, priority,
                        attempts, max_attempts, base_backoff_seconds, available_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        idempotency_key,
                        kind,
                        payload_json,
                        priority,
                        max_attempts,
                        float(base_backoff_seconds),
                        datetime_to_text(ready),
                        current_text,
                        current_text,
                    ),
                )
                await db.commit()
            except sqlite3.IntegrityError:
                await db.rollback()
                async with db.execute(
                    "SELECT * FROM durable_jobs WHERE idempotency_key=?",
                    (idempotency_key,),
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    raise
                if (
                    row["kind"] != kind
                    or row["payload_json"] != payload_json
                    or row["priority"] != priority
                    or row["max_attempts"] != max_attempts
                    or row["base_backoff_seconds"] != float(base_backoff_seconds)
                ):
                    raise IdempotencyConflict(
                        "Cette cle idempotente designe deja un travail different."
                    ) from None
                return _row_to_job(row)

            async with db.execute("SELECT * FROM durable_jobs WHERE id=?", (job_id,)) as cursor:
                row = await cursor.fetchone()
            return _row_to_job(row)

    async def get(self, job_id: str) -> Job | None:
        _name(job_id, "job_id")
        async with self._db() as db:
            async with db.execute("SELECT * FROM durable_jobs WHERE id=?", (job_id,)) as cursor:
                row = await cursor.fetchone()
        return _row_to_job(row) if row else None

    async def reserve_task_run(
        self,
        *,
        task_id: int,
        payload: Any,
        allowed_task_states: set[str] | frozenset[str],
        now: datetime | None = None,
    ) -> Job:
        """Cree un lancement et reserve sa tache dans une seule transaction.

        ``durable_jobs`` et ``tasks`` partagent le meme fichier SQLite dans
        l'Orchestrateur. Cette operation evite donc qu'un worker voie le job
        avant que ``queue_job_id``, ``run_number`` et ``status`` soient poses.
        Le travail deja actif reste idempotent ; une ancienne occurrence
        terminale orpheline fait avancer le numero au lieu d'etre rejouee.
        """
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
            raise ValidationError("task_id doit etre un entier positif.")
        if (
            not isinstance(allowed_task_states, (set, frozenset))
            or not allowed_task_states
            or any(not isinstance(state, str) or not state for state in allowed_task_states)
        ):
            raise ValidationError("allowed_task_states est invalide.")
        kind = "run_task"
        payload_json = _json_text(payload, limit=MAX_PAYLOAD_BYTES, label="payload")
        current = normalize_utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        active_states = {
            JobState.QUEUED.value,
            JobState.RUNNING.value,
            JobState.PAUSED.value,
        }

        async with self._db() as db:
            try:
                await db.execute("BEGIN IMMEDIATE")
                async with db.execute(
                    "SELECT status,queue_job_id,COALESCE(run_number,0) AS run_number, "
                    "source_job_id "
                    "FROM tasks WHERE id=?",
                    (task_id,),
                ) as cursor:
                    task = await cursor.fetchone()
                if task is None:
                    raise TaskReservationConflict("Tache introuvable.")

                previous_status = str(task["status"])
                previous_job_id = task["queue_job_id"]
                if previous_job_id:
                    async with db.execute(
                        "SELECT * FROM durable_jobs WHERE id=?", (previous_job_id,)
                    ) as cursor:
                        previous_job = await cursor.fetchone()
                    if (
                        previous_job is not None
                        and previous_job["state"] in active_states
                    ):
                        if previous_status not in {"queued", "running", "paused"}:
                            raise TaskReservationConflict(
                                "Le lancement precedent termine encore son traitement."
                            )
                        try:
                            previous_payload = json.loads(previous_job["payload_json"])
                        except (TypeError, json.JSONDecodeError) as exc:
                            raise TaskReservationConflict(
                                "Le lancement actif contient un payload invalide."
                            ) from exc
                        source_owned = task["source_job_id"] == previous_job_id
                        direct_owned = (
                            isinstance(previous_payload, dict)
                            and isinstance(previous_payload.get("task_id"), int)
                            and not isinstance(previous_payload.get("task_id"), bool)
                            and previous_payload.get("task_id") == task_id
                            and previous_job["payload_json"] == payload_json
                            and previous_job["idempotency_key"]
                            == f"task:{task_id}:run:{int(task['run_number'] or 0)}"
                        )
                        scheduled_owned = (
                            source_owned
                            and isinstance(previous_payload, dict)
                            and bool(previous_payload.get("automation_id"))
                            and isinstance(previous_payload.get("payload"), dict)
                            and isinstance(
                                previous_payload["payload"].get("task_id"), int
                            )
                            and not isinstance(
                                previous_payload["payload"].get("task_id"), bool
                            )
                        )
                        if previous_job["kind"] != kind or not (
                            direct_owned or scheduled_owned
                        ):
                            raise TaskReservationConflict(
                                "Le lancement actif n'appartient pas a cette tache."
                            )
                        await db.commit()
                        return _row_to_job(previous_job)

                if previous_status not in allowed_task_states:
                    raise TaskReservationConflict(
                        "La tache a change d'etat avant son lancement."
                    )

                previous_run_number = int(task["run_number"] or 0)
                run_number = previous_run_number + 1
                job_row = None
                # Les trous ne se produisent qu'apres une ancienne ecriture
                # partielle. La borne protege une base volontairement corrompue.
                for _ in range(1000):
                    idempotency_key = f"task:{task_id}:run:{run_number}"
                    async with db.execute(
                        "SELECT * FROM durable_jobs WHERE idempotency_key=?",
                        (idempotency_key,),
                    ) as cursor:
                        existing = await cursor.fetchone()
                    if existing is not None:
                        if existing["state"] in active_states:
                            if (
                                existing["kind"] != kind
                                or existing["payload_json"] != payload_json
                                or existing["priority"] != 0
                                or existing["max_attempts"] != 3
                                or existing["base_backoff_seconds"] != 5.0
                            ):
                                raise IdempotencyConflict(
                                    "Cette occurrence active contient un autre lancement."
                                )
                            job_row = existing
                            break
                        run_number += 1
                        continue

                    job_id = uuid.uuid4().hex
                    try:
                        await db.execute(
                            """
                            INSERT INTO durable_jobs (
                                id, idempotency_key, kind, payload_json, state,
                                priority, attempts, max_attempts,
                                base_backoff_seconds, available_at, created_at,
                                updated_at
                            ) VALUES (?, ?, ?, ?, 'queued', 0, 0, 3, 5, ?, ?, ?)
                            """,
                            (
                                job_id,
                                idempotency_key,
                                kind,
                                payload_json,
                                current_text,
                                current_text,
                                current_text,
                            ),
                        )
                    except sqlite3.IntegrityError:
                        async with db.execute(
                            "SELECT * FROM durable_jobs WHERE idempotency_key=?",
                            (idempotency_key,),
                        ) as cursor:
                            existing = await cursor.fetchone()
                        if existing is None:
                            raise
                        if (
                            existing["kind"] != kind
                            or existing["payload_json"] != payload_json
                        ):
                            raise IdempotencyConflict(
                                "Cette cle idempotente designe un autre travail."
                            ) from None
                        job_row = existing
                    else:
                        async with db.execute(
                            "SELECT * FROM durable_jobs WHERE id=?", (job_id,)
                        ) as cursor:
                            job_row = await cursor.fetchone()
                    break
                if job_row is None:
                    raise TaskReservationConflict(
                        "Impossible de reserver un numero de lancement."
                    )

                cursor = await db.execute(
                    """
                    UPDATE tasks
                    SET status='queued', queue_job_id=?, run_number=?
                    WHERE id=? AND status=? AND COALESCE(run_number,0)=?
                        AND queue_job_id IS ?
                    """,
                    (
                        job_row["id"],
                        run_number,
                        task_id,
                        previous_status,
                        previous_run_number,
                        previous_job_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise TaskReservationConflict(
                        "La tache a change d'etat avant sa reservation."
                    )
                await db.commit()
                return _row_to_job(job_row)
            except BaseException:
                await db.rollback()
                raise

    async def list(self, *, state: JobState | str | None = None, limit: int = 100) -> list[Job]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise ValidationError("limit doit etre compris entre 1 et 1000.")
        params: tuple[object, ...]
        query = "SELECT * FROM durable_jobs"
        if state is not None:
            try:
                normalized_state = JobState(state)
            except ValueError as exc:
                raise ValidationError("state invalide.") from exc
            query += " WHERE state=?"
            params = (normalized_state.value, limit)
        else:
            params = (limit,)
        query += " ORDER BY created_at DESC LIMIT ?"
        async with self._db() as db:
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
        return [_row_to_job(row) for row in rows]

    async def purge_terminal(
        self,
        *,
        older_than: datetime,
        limit: int = 1000,
    ) -> int:
        """Supprime un lot de travaux termines apres leur duree de retention.

        La date de fin, et non la date de creation, porte la retention : un
        travail ancien qui vient seulement de finir garde donc son resultat.
        Les travaux en attente, actifs ou en pause ne sont jamais concernes.
        Le lot borne evite de monopoliser SQLite lors d'un gros nettoyage.
        """
        cutoff = datetime_to_text(normalize_utc(older_than, "older_than"))
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 10_000:
            raise ValidationError("limit doit etre compris entre 1 et 10000.")
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                """
                SELECT id FROM durable_jobs
                WHERE state IN ('succeeded','failed','cancelled')
                    AND finished_at IS NOT NULL AND finished_at<=?
                ORDER BY finished_at ASC, created_at ASC
                LIMIT ?
                """,
                (cutoff, limit),
            ) as cursor:
                rows = await cursor.fetchall()
            if rows:
                await db.executemany(
                    "DELETE FROM durable_jobs WHERE id=?",
                    [(row["id"],) for row in rows],
                )
            await db.commit()
        return len(rows)

    async def cancel_scheduled_for_task(
        self, task_id: int, *, now: datetime | None = None
    ) -> int:
        """Annule les occurrences d'automatisation encore non terminales."""
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
            raise ValidationError("task_id doit etre un entier positif.")
        current_text = datetime_to_text(normalize_utc(now or utc_now(), "now"))
        matched: list[str] = []
        quarantined: list[str] = []
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT id,payload_json FROM durable_jobs "
                "WHERE kind='run_task' AND state IN ('queued','running','paused')"
            ) as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, json.JSONDecodeError):
                    # Un ancien travail corrompu ne doit pas empecher la
                    # suppression d'une tache sans rapport. Il est inexecutable
                    # de toute facon : on le met en quarantaine atomiquement.
                    quarantined.append(row["id"])
                    continue
                if not isinstance(payload, dict):
                    quarantined.append(row["id"])
                    continue
                nested = payload.get("payload") if isinstance(payload, dict) else None
                if payload.get("automation_id") and not isinstance(nested, dict):
                    quarantined.append(row["id"])
                    continue
                if (
                    payload.get("automation_id")
                    and isinstance(nested, dict)
                    and isinstance(nested.get("task_id"), int)
                    and not isinstance(nested.get("task_id"), bool)
                    and nested.get("task_id") == task_id
                ):
                    matched.append(row["id"])
            for job_id in (*matched, *quarantined):
                await db.execute(
                    """
                    UPDATE durable_jobs
                    SET state='cancelled', lease_owner=NULL, lease_token=NULL,
                        lease_until=NULL, updated_at=?, finished_at=?
                    WHERE id=? AND state IN ('queued','running','paused')
                    """,
                    (current_text, current_text, job_id),
                )
            await db.commit()
        return len(matched)

    async def cancel_notifications_for_task(
        self, task_id: int, *, now: datetime | None = None
    ) -> int:
        """Annule les notifications non terminales liees a une tache."""
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
            raise ValidationError("task_id doit etre un entier positif.")
        current_text = datetime_to_text(normalize_utc(now or utc_now(), "now"))
        matched: list[str] = []
        quarantined: list[str] = []
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT id,payload_json FROM durable_jobs "
                "WHERE kind='notify' AND state IN ('queued','running','paused')"
            ) as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, json.JSONDecodeError):
                    quarantined.append(row["id"])
                    continue
                if not isinstance(payload, dict):
                    quarantined.append(row["id"])
                    continue
                payload_task_id = payload.get("task_id")
                if (
                    isinstance(payload_task_id, int)
                    and not isinstance(payload_task_id, bool)
                    and payload_task_id == task_id
                ):
                    matched.append(row["id"])
            for job_id in (*matched, *quarantined):
                await db.execute(
                    """
                    UPDATE durable_jobs
                    SET state='cancelled', lease_owner=NULL, lease_token=NULL,
                        lease_until=NULL, updated_at=?, finished_at=?
                    WHERE id=? AND state IN ('queued','running','paused')
                    """,
                    (current_text, current_text, job_id),
                )
            await db.commit()
        return len(matched)

    async def _recover_expired_in_transaction(
        self, db: aiosqlite.Connection, current: datetime
    ) -> int:
        current_text = datetime_to_text(current)
        async with db.execute(
            "SELECT id, attempts, max_attempts, base_backoff_seconds "
            "FROM durable_jobs WHERE state='running' AND lease_until<=?",
            (current_text,),
        ) as cursor:
            expired = await cursor.fetchall()
        for row in expired:
            exhausted = row["attempts"] >= row["max_attempts"]
            if exhausted:
                state = JobState.FAILED.value
                available_at = current_text
                finished_at = current_text
            else:
                state = JobState.QUEUED.value
                delay = row["base_backoff_seconds"] * (2 ** max(0, row["attempts"] - 1))
                available_at = datetime_to_text(current + timedelta(seconds=delay))
                finished_at = None
            await db.execute(
                """
                UPDATE durable_jobs
                SET state=?, available_at=?, lease_owner=NULL, lease_token=NULL,
                    lease_until=NULL, last_error_code='lease_expired',
                    updated_at=?, finished_at=?
                WHERE id=? AND state='running'
                """,
                (state, available_at, current_text, finished_at, row["id"]),
            )
        return len(expired)

    async def recover_expired_leases(self, *, now: datetime | None = None) -> int:
        current = normalize_utc(now or utc_now(), "now")
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            count = await self._recover_expired_in_transaction(db, current)
            await db.commit()
        return count

    async def claim(
        self,
        *,
        worker_id: str,
        lease_seconds: float = 60.0,
        now: datetime | None = None,
    ) -> JobLease | None:
        worker_id = _name(worker_id, "worker_id")
        if (
            not isinstance(lease_seconds, (int, float))
            or isinstance(lease_seconds, bool)
            or not math.isfinite(float(lease_seconds))
            or not 1 <= float(lease_seconds) <= 3600
        ):
            raise ValidationError("lease_seconds doit etre compris entre 1 et 3600.")
        current = normalize_utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        lease_until = current + timedelta(seconds=float(lease_seconds))
        lease_token = uuid.uuid4().hex

        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            await self._recover_expired_in_transaction(db, current)
            async with db.execute(
                """
                SELECT id FROM durable_jobs
                WHERE state='queued' AND available_at<=? AND attempts<max_attempts
                ORDER BY priority DESC, available_at ASC, created_at ASC
                LIMIT 1
                """,
                (current_text,),
            ) as cursor:
                candidate = await cursor.fetchone()
            if candidate is None:
                await db.commit()
                return None
            await db.execute(
                """
                UPDATE durable_jobs
                SET state='running', attempts=attempts+1, lease_owner=?,
                    lease_token=?, lease_until=?, updated_at=?,
                    started_at=COALESCE(started_at, ?), finished_at=NULL
                WHERE id=? AND state='queued'
                """,
                (
                    worker_id,
                    lease_token,
                    datetime_to_text(lease_until),
                    current_text,
                    current_text,
                    candidate["id"],
                ),
            )
            async with db.execute(
                "SELECT * FROM durable_jobs WHERE id=?", (candidate["id"],)
            ) as cursor:
                row = await cursor.fetchone()
            await db.commit()
        return JobLease(
            job=_row_to_job(row),
            token=lease_token,
            worker_id=worker_id,
            lease_until=lease_until,
        )

    async def heartbeat(
        self,
        lease: JobLease,
        *,
        lease_seconds: float = 60.0,
        now: datetime | None = None,
    ) -> JobLease:
        if not isinstance(lease, JobLease):
            raise ValidationError("lease invalide.")
        if (
            not isinstance(lease_seconds, (int, float))
            or isinstance(lease_seconds, bool)
            or not 1 <= float(lease_seconds) <= 3600
        ):
            raise ValidationError("lease_seconds doit etre compris entre 1 et 3600.")
        current = normalize_utc(now or utc_now(), "now")
        new_until = current + timedelta(seconds=float(lease_seconds))
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """
                UPDATE durable_jobs SET lease_until=?, updated_at=?
                WHERE id=? AND state='running' AND lease_token=?
                    AND lease_owner=? AND lease_until>?
                """,
                (
                    datetime_to_text(new_until),
                    datetime_to_text(current),
                    lease.job.id,
                    lease.token,
                    lease.worker_id,
                    datetime_to_text(current),
                ),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                raise LeaseLost("Le bail a expire ou a ete retire.")
            async with db.execute(
                "SELECT * FROM durable_jobs WHERE id=?", (lease.job.id,)
            ) as row_cursor:
                row = await row_cursor.fetchone()
            await db.commit()
        return JobLease(_row_to_job(row), lease.token, lease.worker_id, new_until)

    async def complete(
        self,
        lease: JobLease,
        *,
        result: Any = None,
        now: datetime | None = None,
    ) -> Job:
        return await self._finish_lease(
            lease, succeeded=True, value=result, now=now
        )

    async def fail(
        self,
        lease: JobLease,
        *,
        error_code: str = "worker_error",
        now: datetime | None = None,
    ) -> Job:
        return await self._finish_lease(
            lease, succeeded=False, value=_error_code(error_code), now=now
        )

    async def release(
        self, lease: JobLease, *, now: datetime | None = None
    ) -> Job:
        """Rend atomiquement un bail a la file sans compter une tentative.

        Utilise lors d'une fermeture propre du serveur. La comparaison porte
        sur le jeton et le proprietaire du bail : un ancien worker ne peut pas
        remettre en file un travail deja repris ailleurs.
        """
        if not isinstance(lease, JobLease):
            raise ValidationError("lease invalide.")
        current = normalize_utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """
                UPDATE durable_jobs
                SET state='queued', available_at=?,
                    lease_owner=NULL, lease_token=NULL, lease_until=NULL,
                    attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,
                    updated_at=?, finished_at=NULL
                WHERE id=? AND state='running' AND lease_token=?
                    AND lease_owner=? AND lease_until>?
                """,
                (
                    current_text,
                    current_text,
                    lease.job.id,
                    lease.token,
                    lease.worker_id,
                    current_text,
                ),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                raise LeaseLost("Le bail a expire ou a deja ete retire.")
            async with db.execute(
                "SELECT * FROM durable_jobs WHERE id=?", (lease.job.id,)
            ) as row_cursor:
                row = await row_cursor.fetchone()
            await db.commit()
        return _row_to_job(row)

    async def _finish_lease(
        self,
        lease: JobLease,
        *,
        succeeded: bool,
        value: Any,
        now: datetime | None,
    ) -> Job:
        if not isinstance(lease, JobLease):
            raise ValidationError("lease invalide.")
        current = normalize_utc(now or utc_now(), "now")
        current_text = datetime_to_text(current)
        result_json = (
            _json_text(value, limit=MAX_RESULT_BYTES, label="result")
            if succeeded
            else None
        )
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                """
                SELECT * FROM durable_jobs
                WHERE id=? AND state='running' AND lease_token=? AND lease_owner=?
                    AND lease_until>?
                """,
                (
                    lease.job.id,
                    lease.token,
                    lease.worker_id,
                    current_text,
                ),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                await db.rollback()
                raise LeaseLost("Le bail a expire ou a ete retire.")

            if succeeded:
                state = JobState.SUCCEEDED.value
                available_at = row["available_at"]
                error_code = None
                finished_at = current_text
            elif row["attempts"] >= row["max_attempts"]:
                state = JobState.FAILED.value
                available_at = row["available_at"]
                error_code = value
                finished_at = current_text
            else:
                state = JobState.QUEUED.value
                delay = row["base_backoff_seconds"] * (2 ** max(0, row["attempts"] - 1))
                available_at = datetime_to_text(current + timedelta(seconds=delay))
                error_code = value
                finished_at = None
            await db.execute(
                """
                UPDATE durable_jobs
                SET state=?, available_at=?, lease_owner=NULL, lease_token=NULL,
                    lease_until=NULL, last_error_code=?, result_json=?,
                    updated_at=?, finished_at=?
                WHERE id=? AND state='running' AND lease_token=?
                """,
                (
                    state,
                    available_at,
                    error_code,
                    result_json,
                    current_text,
                    finished_at,
                    lease.job.id,
                    lease.token,
                ),
            )
            async with db.execute(
                "SELECT * FROM durable_jobs WHERE id=?", (lease.job.id,)
            ) as cursor:
                updated = await cursor.fetchone()
            await db.commit()
        return _row_to_job(updated)

    async def pause(self, job_id: str, *, now: datetime | None = None) -> Job | None:
        return await self._control(job_id, JobState.PAUSED, now=now)

    async def cancel(self, job_id: str, *, now: datetime | None = None) -> Job | None:
        return await self._control(job_id, JobState.CANCELLED, now=now)

    async def _control(
        self, job_id: str, target: JobState, *, now: datetime | None
    ) -> Job | None:
        _name(job_id, "job_id")
        current_text = datetime_to_text(normalize_utc(now or utc_now(), "now"))
        terminal = (JobState.SUCCEEDED.value, JobState.FAILED.value, JobState.CANCELLED.value)
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                """
                UPDATE durable_jobs
                SET state=?, lease_owner=NULL, lease_token=NULL, lease_until=NULL,
                    attempts=CASE
                        WHEN ?='paused' AND state='running' AND attempts>0
                        THEN attempts-1 ELSE attempts END,
                    updated_at=?, finished_at=CASE WHEN ?='cancelled' THEN ? ELSE NULL END
                WHERE id=? AND state NOT IN (?, ?, ?)
                """,
                (
                    target.value, target.value, current_text, target.value,
                    current_text, job_id, *terminal,
                ),
            )
            async with db.execute("SELECT * FROM durable_jobs WHERE id=?", (job_id,)) as cursor:
                row = await cursor.fetchone()
            await db.commit()
        return _row_to_job(row) if row else None

    async def resume(self, job_id: str, *, now: datetime | None = None) -> Job | None:
        _name(job_id, "job_id")
        current_text = datetime_to_text(normalize_utc(now or utc_now(), "now"))
        async with self._db() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                """
                UPDATE durable_jobs
                SET state='queued', available_at=?, updated_at=?, finished_at=NULL
                WHERE id=? AND state='paused'
                """,
                (current_text, current_text, job_id),
            )
            async with db.execute("SELECT * FROM durable_jobs WHERE id=?", (job_id,)) as cursor:
                row = await cursor.fetchone()
            await db.commit()
        return _row_to_job(row) if row else None

    async def lease_is_active(self, lease: JobLease, *, now: datetime | None = None) -> bool:
        if not isinstance(lease, JobLease):
            return False
        current_text = datetime_to_text(normalize_utc(now or utc_now(), "now"))
        async with self._db() as db:
            async with db.execute(
                """
                SELECT 1 FROM durable_jobs
                WHERE id=? AND state='running' AND lease_token=?
                    AND lease_owner=? AND lease_until>?
                """,
                (lease.job.id, lease.token, lease.worker_id, current_text),
            ) as cursor:
                return await cursor.fetchone() is not None


__all__ = [
    "DurableQueue",
    "IdempotencyConflict",
    "Job",
    "JobLease",
    "JobState",
    "LeaseLost",
    "MAX_PAYLOAD_BYTES",
    "QueueError",
    "TaskReservationConflict",
    "ValidationError",
    "datetime_from_text",
    "datetime_to_text",
    "normalize_utc",
    "utc_now",
]
