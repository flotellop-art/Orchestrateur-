"""Compétences documentaires proposées par les agents et validées par un humain.

Le module stocke uniquement du texte Markdown. Il ne l'interprète pas, ne
l'importe pas et ne lance jamais de commande. Une proposition reste ``pending``
et n'est jamais renvoyée par :func:`search_skills` avant un appel explicite à
:func:`approve_skill` avec ``human_confirmed=True``.

L'API est asynchrone afin de s'intégrer au reste de l'orchestrateur, qui utilise
déjà ``aiosqlite``. Toutes les transitions d'état importantes utilisent une
transaction ``BEGIN IMMEDIATE`` : deux validations concurrentes ne peuvent donc
pas activer deux versions portant le même nom.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import unicodedata
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

try:
    # Utilise le même emplacement persistant que les tâches dans l'application
    # empaquetée. Le fallback garde le module autonome dans les anciens dépôts.
    from app_paths import DB_PATH as _APPLICATION_DB_PATH
except ImportError:  # pragma: no cover - compatibilité avec l'ancienne arborescence
    _APPLICATION_DB_PATH = Path(__file__).parent / "apps.db"

DB_PATH = _APPLICATION_DB_PATH

MAX_NAME_CHARS = 64
MAX_ACTOR_CHARS = 80
MAX_SUMMARY_CHARS = 500
MAX_SUMMARY_BYTES = 2_000
MAX_INSTRUCTIONS_CHARS = 32_768
MAX_INSTRUCTIONS_BYTES = 65_536
MAX_TAGS = 12
MAX_TAG_CHARS = 32
MAX_REASON_CHARS = 500
MAX_QUERY_CHARS = 512
MAX_QUERY_TOKENS = 32
MAX_LIST_LIMIT = 200
MAX_SEARCH_LIMIT = 50


class SkillError(Exception):
    """Erreur de base du registre de compétences."""


class SkillValidationError(SkillError, ValueError):
    """Une entrée ne respecte pas le contrat public."""


class SkillNotFoundError(SkillError, LookupError):
    """La compétence demandée n'existe pas."""


class SkillStateError(SkillError):
    """La transition demandée n'est pas permise."""


class SkillConflictError(SkillStateError):
    """Une autre compétence active utilise déjà le même nom."""


class HumanApprovalRequired(SkillStateError):
    """Une action réservée à l'interface humaine a été appelée sans preuve."""


class SkillStorageError(SkillError):
    """Le stockage n'est pas initialisé ou contient une donnée incohérente."""


class SkillStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    REJECTED = "rejected"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class AgentSkill:
    id: int
    task_id: int
    proposed_by: str
    name: str
    slug: str
    summary: str
    instructions: str
    tags: tuple[str, ...]
    content_hash: str
    status: SkillStatus
    proposed_at: str
    decided_at: str | None = None
    decided_by: str | None = None
    decision_reason: str | None = None

    def to_payload(self, *, include_instructions: bool = True) -> dict:
        """Renvoie une représentation JSON sans objet interne SQLite."""
        payload = {
            "id": self.id,
            "task_id": self.task_id,
            "proposed_by": self.proposed_by,
            "name": self.name,
            "slug": self.slug,
            "summary": self.summary,
            "tags": list(self.tags),
            "content_hash": self.content_hash,
            "status": self.status.value,
            "proposed_at": self.proposed_at,
            "decided_at": self.decided_at,
            "decided_by": self.decided_by,
            "decision_reason": self.decision_reason,
        }
        if include_instructions:
            payload["instructions"] = self.instructions
        return payload


@dataclass(frozen=True, slots=True)
class SkillSearchResult:
    skill: AgentSkill
    score: int
    matched_terms: tuple[str, ...]

    def to_payload(self, *, include_instructions: bool = True) -> dict:
        payload = self.skill.to_payload(include_instructions=include_instructions)
        payload["score"] = self.score
        payload["matched_terms"] = list(self.matched_terms)
        return payload


_SAFE_NAME_RE = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9 ._-]{0,62}[A-Za-z0-9])?\Z", re.ASCII
)
_SAFE_ACTOR_RE = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9 _.@:+-]{0,78}[A-Za-z0-9])?\Z", re.ASCII
)
_SAFE_TAG_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?\Z", re.ASCII
)
_RAW_HTML_RE = re.compile(r"<\s*/?\s*[A-Za-z][^>\r\n]*>", re.IGNORECASE)
_UNSAFE_URI_RE = re.compile(
    r"(?:\]\s*\(|<)\s*(?:javascript|vbscript|data|file)\s*:", re.IGNORECASE
)
_WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)*", re.UNICODE)
_WINDOWS_RESERVED = {
    "con", "prn", "aux", "nul", "clock$",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
}


def _db_path(value: str | os.PathLike[str] | None) -> Path:
    if value is None:
        return DB_PATH
    try:
        raw = os.fspath(value)
    except TypeError as exc:
        raise SkillValidationError("db_path est invalide.") from exc
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise SkillValidationError("db_path est invalide.")
    return Path(raw)


@asynccontextmanager
async def _connect(db_path: str | os.PathLike[str] | None):
    import aiosqlite

    db = await aiosqlite.connect(str(_db_path(db_path)), timeout=30.0)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("PRAGMA busy_timeout=30000")
        yield db
    finally:
        await db.close()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SkillValidationError(f"{label} doit être un entier strictement positif.")
    return value


def _bounded_int(value: object, label: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SkillValidationError(f"{label} doit être un entier.")
    if not minimum <= value <= maximum:
        raise SkillValidationError(
            f"{label} doit être compris entre {minimum} et {maximum}."
        )
    return value


def _validate_actor(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise SkillValidationError(f"{label} doit être une chaîne.")
    value = " ".join(value.strip().split())
    if (
        not value
        or len(value) > MAX_ACTOR_CHARS
        or not value.isascii()
        or not _SAFE_ACTOR_RE.fullmatch(value)
    ):
        raise SkillValidationError(
            f"{label} doit être un identifiant ASCII simple de "
            f"{MAX_ACTOR_CHARS} caractères maximum."
        )
    return value


def _validate_name(value: object) -> tuple[str, str]:
    if not isinstance(value, str):
        raise SkillValidationError("name doit être une chaîne.")
    name = " ".join(value.strip().split())
    if (
        not name
        or len(name) > MAX_NAME_CHARS
        or not name.isascii()
        or not _SAFE_NAME_RE.fullmatch(name)
        or ".." in name
    ):
        raise SkillValidationError(
            "name doit être un nom ASCII simple, sans chemin ni option, "
            f"de {MAX_NAME_CHARS} caractères maximum."
        )
    slug = re.sub(r"[ ._]+", "-", name.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    if not slug or slug in _WINDOWS_RESERVED:
        raise SkillValidationError("name produit un nom de dossier réservé ou vide.")
    return name, slug


def _has_forbidden_control(value: str) -> bool:
    bidi_controls = {
        "\u202a", "\u202b", "\u202c", "\u202d", "\u202e",
        "\u2066", "\u2067", "\u2068", "\u2069",
    }
    for char in value:
        if char in bidi_controls:
            return True
        category = unicodedata.category(char)
        if category in {"Cf", "Cs", "Co"}:
            return True
        if category == "Cc" and char not in {"\n", "\t"}:
            return True
    return False


def _validate_document(
    value: object,
    label: str,
    *,
    max_chars: int,
    max_bytes: int,
    single_line: bool = False,
    minimum: int = 1,
) -> str:
    if not isinstance(value, str):
        raise SkillValidationError(f"{label} doit être une chaîne.")
    value = unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n")).strip()
    if len(value) < minimum:
        raise SkillValidationError(f"{label} est trop court.")
    if len(value) > max_chars or len(value.encode("utf-8")) > max_bytes:
        raise SkillValidationError(f"{label} est trop long.")
    if single_line and ("\n" in value or "\t" in value):
        raise SkillValidationError(f"{label} doit tenir sur une seule ligne.")
    if _has_forbidden_control(value):
        raise SkillValidationError(f"{label} contient des caractères de contrôle interdits.")
    # Le stockage ne rend pas le Markdown. Refuser le HTML brut et les schémas
    # actifs empêche aussi une future interface de les rendre par erreur.
    if _RAW_HTML_RE.search(value) or _UNSAFE_URI_RE.search(value):
        raise SkillValidationError(
            f"{label} doit rester documentaire : HTML brut et URI actives sont interdits."
        )
    return value


def _validate_tags(tags: object) -> tuple[str, ...]:
    if tags is None:
        return ()
    if isinstance(tags, (str, bytes)) or not isinstance(tags, Iterable):
        raise SkillValidationError("tags doit être une liste de libellés.")
    normalized: list[str] = []
    for raw in tags:
        if not isinstance(raw, str):
            raise SkillValidationError("Chaque tag doit être une chaîne.")
        tag = raw.strip().lower()
        if (
            not tag
            or len(tag) > MAX_TAG_CHARS
            or not tag.isascii()
            or not _SAFE_TAG_RE.fullmatch(tag)
        ):
            raise SkillValidationError(
                "Un tag doit être un identifiant ASCII simple de "
                f"{MAX_TAG_CHARS} caractères maximum."
            )
        if tag not in normalized:
            normalized.append(tag)
    if len(normalized) > MAX_TAGS:
        raise SkillValidationError(f"Une compétence accepte au maximum {MAX_TAGS} tags.")
    return tuple(sorted(normalized))


def _status(value: SkillStatus | str | None) -> SkillStatus | None:
    if value is None:
        return None
    if isinstance(value, SkillStatus):
        return value
    if not isinstance(value, str):
        raise SkillValidationError("status est invalide.")
    try:
        return SkillStatus(value)
    except ValueError as exc:
        raise SkillValidationError(
            "status doit valoir pending, active ou rejected."
        ) from exc


def _row_to_skill(row) -> AgentSkill:
    try:
        raw_tags = json.loads(row["tags_json"])
        if not isinstance(raw_tags, list) or not all(isinstance(t, str) for t in raw_tags):
            raise ValueError("tags_json invalide")
        return AgentSkill(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            proposed_by=str(row["proposed_by"]),
            name=str(row["name"]),
            slug=str(row["slug"]),
            summary=str(row["summary"]),
            instructions=str(row["instructions"]),
            tags=tuple(raw_tags),
            content_hash=str(row["content_hash"]),
            status=SkillStatus(row["status"]),
            proposed_at=str(row["proposed_at"]),
            decided_at=row["decided_at"],
            decided_by=row["decided_by"],
            decision_reason=row["decision_reason"],
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SkillStorageError("Une compétence stockée est incohérente.") from exc


async def init_agent_skills_db(
    db_path: str | os.PathLike[str] | None = None,
) -> None:
    """Crée le schéma de façon idempotente.

    La table ``tasks`` appartient à ``team.py`` et doit être initialisée avant
    la première proposition. La clé étrangère empêche une proposition orpheline.
    """
    path = _db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    async with _connect(path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_skills (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id         INTEGER NOT NULL,
                proposed_by     TEXT NOT NULL,
                name            TEXT NOT NULL,
                slug            TEXT NOT NULL,
                summary         TEXT NOT NULL,
                instructions    TEXT NOT NULL,
                tags_json       TEXT NOT NULL DEFAULT '[]',
                content_hash    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','active','rejected')),
                proposed_at     TEXT NOT NULL,
                decided_at      TEXT,
                decided_by      TEXT,
                decision_reason TEXT,
                FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                UNIQUE(task_id, proposed_by, content_hash),
                CHECK (
                    (status = 'pending' AND decided_at IS NULL
                     AND decided_by IS NULL AND decision_reason IS NULL)
                    OR
                    (status = 'active' AND decided_at IS NOT NULL
                     AND length(decided_by) > 0 AND decision_reason IS NULL)
                    OR
                    (status = 'rejected' AND decided_at IS NOT NULL
                     AND length(decided_by) > 0 AND length(decision_reason) > 0)
                )
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_skills_task_status "
            "ON agent_skills(task_id, status, id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_skills_status_slug "
            "ON agent_skills(status, slug, id)"
        )
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_skills_active_slug "
            "ON agent_skills(slug) WHERE status='active'"
        )
        await db.commit()


async def _assert_task_exists(db, task_id: int) -> None:
    try:
        async with db.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)) as cursor:
            row = await cursor.fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise SkillStorageError(
                "La table tasks n'existe pas : appeler init_team_db avant de "
                "proposer une compétence."
            ) from exc
        raise
    if row is None:
        raise SkillValidationError(f"La tâche {task_id} n'existe pas.")


def _canonical_proposal(
    *,
    task_id: int,
    proposed_by: str,
    name: str,
    slug: str,
    summary: str,
    instructions: str,
    tags: Sequence[str],
) -> tuple[str, str]:
    canonical = json.dumps(
        {
            "schema": 1,
            "task_id": task_id,
            "proposed_by": proposed_by,
            "name": name,
            "slug": slug,
            "summary": summary,
            "instructions": instructions,
            "tags": list(tags),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return canonical, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def propose_skill(
    *,
    task_id: int,
    agent: str,
    name: str,
    summary: str,
    instructions: str,
    tags: Sequence[str] | None = None,
    db_path: str | os.PathLike[str] | None = None,
) -> AgentSkill:
    """Crée une proposition ``pending`` ou renvoie son doublon exact.

    ``agent`` doit provenir du contexte d'exécution fiable de la tâche, et non
    d'un champ libre fourni par le modèle. Le hash canonique et la contrainte
    SQLite rendent l'appel idempotent, y compris entre workers concurrents.
    """
    task_id = _positive_int(task_id, "task_id")
    proposed_by = _validate_actor(agent, "agent")
    clean_name, slug = _validate_name(name)
    clean_summary = _validate_document(
        summary,
        "summary",
        max_chars=MAX_SUMMARY_CHARS,
        max_bytes=MAX_SUMMARY_BYTES,
        single_line=True,
        minimum=8,
    )
    clean_instructions = _validate_document(
        instructions,
        "instructions",
        max_chars=MAX_INSTRUCTIONS_CHARS,
        max_bytes=MAX_INSTRUCTIONS_BYTES,
        minimum=20,
    )
    clean_tags = _validate_tags(tags)
    _, content_hash = _canonical_proposal(
        task_id=task_id,
        proposed_by=proposed_by,
        name=clean_name,
        slug=slug,
        summary=clean_summary,
        instructions=clean_instructions,
        tags=clean_tags,
    )
    tags_json = json.dumps(list(clean_tags), ensure_ascii=True, separators=(",", ":"))

    async with _connect(db_path) as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            await _assert_task_exists(db, task_id)
            await db.execute(
                """
                INSERT OR IGNORE INTO agent_skills
                    (task_id, proposed_by, name, slug, summary, instructions,
                     tags_json, content_hash, status, proposed_at)
                VALUES (?,?,?,?,?,?,?,?, 'pending', ?)
                """,
                (
                    task_id, proposed_by, clean_name, slug, clean_summary,
                    clean_instructions, tags_json, content_hash, _utc_now(),
                ),
            )
            async with db.execute(
                "SELECT * FROM agent_skills "
                "WHERE task_id=? AND proposed_by=? AND content_hash=?",
                (task_id, proposed_by, content_hash),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise SkillStorageError("La proposition n'a pas pu être relue.")
            record = _row_to_skill(row)
            # Un hash identique ne doit jamais masquer une ligne différente.
            if (
                record.name != clean_name
                or record.slug != slug
                or record.summary != clean_summary
                or record.instructions != clean_instructions
                or record.tags != clean_tags
            ):
                raise SkillConflictError("Collision de hash détectée.")
            await db.commit()
            return record
        except BaseException:
            await db.rollback()
            raise


async def get_skill(
    skill_id: int,
    *,
    db_path: str | os.PathLike[str] | None = None,
) -> AgentSkill:
    skill_id = _positive_int(skill_id, "skill_id")
    async with _connect(db_path) as db:
        async with db.execute("SELECT * FROM agent_skills WHERE id=?", (skill_id,)) as cursor:
            row = await cursor.fetchone()
    if row is None:
        raise SkillNotFoundError(f"Compétence {skill_id} introuvable.")
    return _row_to_skill(row)


def _require_human_confirmation(human_confirmed: object) -> None:
    if human_confirmed is not True:
        raise HumanApprovalRequired(
            "Cette transition doit provenir d'une action humaine confirmée."
        )


async def approve_skill(
    skill_id: int,
    *,
    approved_by: str,
    human_confirmed: bool = False,
    replace_existing: bool = False,
    db_path: str | os.PathLike[str] | None = None,
) -> AgentSkill:
    """Active une proposition après confirmation humaine explicite.

    Si une autre version du même slug est active, l'appel échoue par défaut.
    ``replace_existing=True`` permet à la même action humaine de rejeter
    l'ancienne version puis d'activer la nouvelle, dans une seule transaction.
    """
    skill_id = _positive_int(skill_id, "skill_id")
    _require_human_confirmation(human_confirmed)
    reviewer = _validate_actor(approved_by, "approved_by")
    if not isinstance(replace_existing, bool):
        raise SkillValidationError("replace_existing doit être un booléen.")

    async with _connect(db_path) as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT * FROM agent_skills WHERE id=?", (skill_id,)
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise SkillNotFoundError(f"Compétence {skill_id} introuvable.")
            current = _row_to_skill(row)
            if current.status is SkillStatus.ACTIVE:
                await db.commit()
                return current
            if current.status is SkillStatus.REJECTED:
                raise SkillStateError("Une proposition rejetée ne peut plus être activée.")

            async with db.execute(
                "SELECT * FROM agent_skills "
                "WHERE slug=? AND status='active' AND id<>?",
                (current.slug, skill_id),
            ) as cursor:
                existing_row = await cursor.fetchone()
            now = _utc_now()
            if existing_row is not None:
                existing = _row_to_skill(existing_row)
                if not replace_existing:
                    raise SkillConflictError(
                        f"La compétence active {existing.id} utilise déjà le nom "
                        f"{current.slug!r}."
                    )
                await db.execute(
                    """
                    UPDATE agent_skills
                    SET status='rejected', decided_at=?, decided_by=?, decision_reason=?
                    WHERE id=? AND status='active'
                    """,
                    (
                        now,
                        reviewer,
                        f"Remplacée par la compétence #{skill_id} après validation humaine.",
                        existing.id,
                    ),
                )

            cursor = await db.execute(
                """
                UPDATE agent_skills
                SET status='active', decided_at=?, decided_by=?, decision_reason=NULL
                WHERE id=? AND status='pending'
                """,
                (now, reviewer, skill_id),
            )
            if cursor.rowcount != 1:
                raise SkillStateError("La proposition a changé d'état pendant la validation.")
            async with db.execute(
                "SELECT * FROM agent_skills WHERE id=?", (skill_id,)
            ) as cursor:
                updated = await cursor.fetchone()
            await db.commit()
            if updated is None:
                raise SkillStorageError("La compétence activée n'a pas pu être relue.")
            return _row_to_skill(updated)
        except BaseException:
            await db.rollback()
            raise


async def reject_skill(
    skill_id: int,
    *,
    rejected_by: str,
    reason: str,
    human_confirmed: bool = False,
    db_path: str | os.PathLike[str] | None = None,
) -> AgentSkill:
    """Rejette définitivement une proposition encore ``pending``."""
    skill_id = _positive_int(skill_id, "skill_id")
    _require_human_confirmation(human_confirmed)
    reviewer = _validate_actor(rejected_by, "rejected_by")
    clean_reason = _validate_document(
        reason,
        "reason",
        max_chars=MAX_REASON_CHARS,
        max_bytes=2_000,
        single_line=True,
        minimum=3,
    )
    async with _connect(db_path) as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT * FROM agent_skills WHERE id=?", (skill_id,)
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise SkillNotFoundError(f"Compétence {skill_id} introuvable.")
            current = _row_to_skill(row)
            if current.status is SkillStatus.REJECTED:
                await db.commit()
                return current
            if current.status is SkillStatus.ACTIVE:
                raise SkillStateError(
                    "Une compétence active ne peut pas être rejetée comme proposition."
                )
            cursor = await db.execute(
                """
                UPDATE agent_skills
                SET status='rejected', decided_at=?, decided_by=?, decision_reason=?
                WHERE id=? AND status='pending'
                """,
                (_utc_now(), reviewer, clean_reason, skill_id),
            )
            if cursor.rowcount != 1:
                raise SkillStateError("La proposition a changé d'état pendant la validation.")
            async with db.execute(
                "SELECT * FROM agent_skills WHERE id=?", (skill_id,)
            ) as cursor:
                updated = await cursor.fetchone()
            await db.commit()
            if updated is None:
                raise SkillStorageError("La proposition rejetée n'a pas pu être relue.")
            return _row_to_skill(updated)
        except BaseException:
            await db.rollback()
            raise


async def list_skills(
    *,
    status: SkillStatus | str | None = None,
    task_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
    db_path: str | os.PathLike[str] | None = None,
) -> list[AgentSkill]:
    """Liste stable des compétences, filtrable par statut et tâche."""
    clean_status = _status(status)
    if task_id is not None:
        task_id = _positive_int(task_id, "task_id")
    limit = _bounded_int(limit, "limit", minimum=1, maximum=MAX_LIST_LIMIT)
    offset = _bounded_int(offset, "offset", minimum=0, maximum=1_000_000)
    where: list[str] = []
    params: list[object] = []
    if clean_status is not None:
        where.append("status=?")
        params.append(clean_status.value)
    if task_id is not None:
        where.append("task_id=?")
        params.append(task_id)
    sql = "SELECT * FROM agent_skills"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY proposed_at DESC, id DESC LIMIT ? OFFSET ?"
    params.extend((limit, offset))
    async with _connect(db_path) as db:
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
    return [_row_to_skill(row) for row in rows]


def _search_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _tokens(value: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(match.group(0) for match in _WORD_RE.finditer(_search_text(value))))


def _score(
    skill: AgentSkill,
    query_text: str,
    query_tokens: Sequence[str],
) -> SkillSearchResult | None:
    name_text = _search_text(skill.name)
    slug_text = _search_text(skill.slug)
    summary_text = _search_text(skill.summary)
    instructions_text = _search_text(skill.instructions)
    name_tokens = set(_tokens(skill.name)) | set(_tokens(skill.slug))
    summary_tokens = set(_tokens(skill.summary))
    instruction_tokens = set(_tokens(skill.instructions))
    tag_tokens = set(skill.tags)

    score = 0
    if query_text == name_text or query_text == slug_text:
        score += 500
    elif query_text in name_text or query_text in slug_text:
        score += 180
    if query_text in summary_text:
        score += 60
    if query_text in instructions_text:
        score += 15

    matched: list[str] = []
    for token in query_tokens:
        token_score = 0
        if token in name_tokens:
            token_score += 90
        if token in tag_tokens:
            token_score += 70
        if token in summary_tokens:
            token_score += 30
        if token in instruction_tokens:
            token_score += 8
        if token_score:
            matched.append(token)
            score += token_score
    if score <= 0:
        return None
    return SkillSearchResult(skill=skill, score=score, matched_terms=tuple(sorted(matched)))


async def search_skills(
    query: str,
    *,
    limit: int = 8,
    db_path: str | os.PathLike[str] | None = None,
) -> list[SkillSearchResult]:
    """Recherche locale déterministe parmi les seules compétences actives.

    Aucun modèle, embedding ou service réseau n'est appelé. Le score est un
    simple recouvrement pondé : nom, tags, résumé puis corps documentaire. Les
    égalités sont toujours départagées par slug puis identifiant.
    """
    if not isinstance(query, str):
        raise SkillValidationError("query doit être une chaîne.")
    query_text = _search_text(query)
    if not query_text:
        return []
    if len(query_text) > MAX_QUERY_CHARS or len(query_text.encode("utf-8")) > 2_048:
        raise SkillValidationError("query est trop longue.")
    if _has_forbidden_control(query_text):
        raise SkillValidationError("query contient des caractères interdits.")
    query_tokens = _tokens(query_text)
    if not query_tokens:
        return []
    if len(query_tokens) > MAX_QUERY_TOKENS:
        raise SkillValidationError("query contient trop de termes.")
    limit = _bounded_int(limit, "limit", minimum=1, maximum=MAX_SEARCH_LIMIT)

    async with _connect(db_path) as db:
        async with db.execute(
            "SELECT * FROM agent_skills WHERE status='active' ORDER BY slug, id"
        ) as cursor:
            rows = await cursor.fetchall()
    results = []
    for row in rows:
        result = _score(_row_to_skill(row), query_text, query_tokens)
        if result is not None:
            results.append(result)
    results.sort(key=lambda item: (-item.score, item.skill.slug, item.skill.id))
    return results[:limit]


def render_skill_md(skill: AgentSkill) -> str:
    """Produit un document SKILL.md inerte pour une compétence active."""
    if not isinstance(skill, AgentSkill):
        raise SkillValidationError("skill doit être un AgentSkill.")
    if skill.status is not SkillStatus.ACTIVE:
        raise SkillStateError("Seule une compétence active peut être exportée.")
    description = json.dumps(skill.summary, ensure_ascii=False)
    tags = json.dumps(list(skill.tags), ensure_ascii=False)
    return (
        "---\n"
        f"name: {skill.slug}\n"
        f"description: {description}\n"
        f"tags: {tags}\n"
        "---\n\n"
        f"# {skill.name}\n\n"
        f"{skill.instructions.rstrip()}\n"
    )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


async def export_skill_md(
    skill_id: int,
    export_root: str | os.PathLike[str],
    *,
    overwrite: bool = False,
    db_path: str | os.PathLike[str] | None = None,
) -> Path:
    """Exporte atomiquement ``<racine>/<slug>/SKILL.md``.

    Le nom du fichier n'est jamais fourni par l'agent. Les liens symboliques qui
    sortiraient de la racine sont refusés et une exportation identique est
    idempotente. ``overwrite`` doit être une décision de l'interface humaine.
    """
    if not isinstance(overwrite, bool):
        raise SkillValidationError("overwrite doit être un booléen.")
    skill = await get_skill(skill_id, db_path=db_path)
    content = render_skill_md(skill)
    try:
        raw_root = os.fspath(export_root)
    except TypeError as exc:
        raise SkillValidationError("export_root est invalide.") from exc
    if not isinstance(raw_root, str) or not raw_root or "\x00" in raw_root:
        raise SkillValidationError("export_root est invalide.")

    root = Path(raw_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    skill_dir = root / skill.slug
    resolved_dir = skill_dir.resolve(strict=False)
    if not _is_relative_to(resolved_dir, root):
        raise SkillValidationError("Le dossier d'export sort de la racine autorisée.")
    if skill_dir.is_symlink():
        raise SkillValidationError("Un dossier de compétence symbolique est interdit.")
    skill_dir.mkdir(mode=0o700, exist_ok=True)
    resolved_dir = skill_dir.resolve()
    if not _is_relative_to(resolved_dir, root):
        raise SkillValidationError("Le dossier d'export sort de la racine autorisée.")

    destination = skill_dir / "SKILL.md"
    if destination.is_symlink():
        raise SkillValidationError("Un fichier SKILL.md symbolique est interdit.")
    if destination.exists():
        previous = destination.read_text(encoding="utf-8")
        if previous == content:
            return destination
        if not overwrite:
            raise SkillConflictError(
                "SKILL.md existe déjà avec un contenu différent."
            )

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=".SKILL.md.",
            suffix=".tmp",
            dir=skill_dir,
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            temporary_path.chmod(0o600)
        except OSError:
            pass
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
    return destination


__all__ = [
    "AgentSkill",
    "DB_PATH",
    "HumanApprovalRequired",
    "SkillConflictError",
    "SkillError",
    "SkillNotFoundError",
    "SkillSearchResult",
    "SkillStateError",
    "SkillStatus",
    "SkillStorageError",
    "SkillValidationError",
    "approve_skill",
    "export_skill_md",
    "get_skill",
    "init_agent_skills_db",
    "list_skills",
    "propose_skill",
    "reject_skill",
    "render_skill_md",
    "search_skills",
]
