"""Interface HTTP humaine du registre de compétences réutilisables.

Les agents n'ont aucun endpoint de décision dans leur contrat d'outils. Cette
API exige en plus une session de revue courte, liée au navigateur et consommée
à la première décision. Le corps HTTP ne peut fournir ni ``human_confirmed`` ni
l'identité du relecteur : ces deux valeurs sont injectées exclusivement ici.

Cette garde bloque les appels accidentels, les rejeux et les requêtes intersites.
La frontière forte reste le bac à sable des agents : un programme hostile ayant
les mêmes droits système que le serveur ne peut pas être distingué d'un humain
par un simple protocole HTTP local.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import threading
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from agent_skills import (
    HumanApprovalRequired,
    SkillConflictError,
    SkillError,
    SkillNotFoundError,
    SkillStateError,
    SkillStatus,
    SkillStorageError,
    SkillValidationError,
    approve_skill,
    export_skill_md,
    get_skill,
    init_agent_skills_db,
    list_skills,
    reject_skill,
)
from app_paths import DATA_ROOT, DB_PATH, STATIC_ROOT
from auth_middleware import MIN_API_SECRET_LENGTH, get_api_secret


log = logging.getLogger(__name__)

SKILLS_EXPORT_ROOT = DATA_ROOT / "skills"
HUMAN_REVIEWER = "human-ui"
REVIEW_SESSION_TTL_SECONDS = 300
REVIEW_SESSION_LIMIT = 256
REVIEW_COOKIE = "orchestrator_skills_review"
REVIEW_TOKEN_HEADER = "X-Orchestrator-Review-Token"
HUMAN_UI_HEADER = "X-Orchestrator-Human-UI"


@asynccontextmanager
async def _skills_router_lifespan(_app):
    await init_skills_api()
    yield


# Le cycle de vie du routeur initialise le schéma lorsqu'il est inclus dans
# FastAPI. L'intégration ne demande donc qu'un import et include_router().
router = APIRouter(lifespan=_skills_router_lifespan)

class SkillDecisionBody(BaseModel):
    """Le client choisit une action, jamais l'identité ou la preuve humaine."""

    model_config = ConfigDict(extra="forbid")

    decision: Literal["approve", "reject"]
    reason: str | None = Field(default=None, max_length=500)
    replace_existing: bool = False


@dataclass(frozen=True, slots=True)
class _ReviewSession:
    token_digest: str
    fingerprint: str
    expires_at: float


_review_sessions: OrderedDict[str, _ReviewSession] = OrderedDict()
_review_sessions_lock = threading.Lock()


def _fingerprint(request: Request) -> str:
    peer = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "")[:512]
    material = (peer + "\n" + user_agent).encode("utf-8", errors="replace")
    return hashlib.sha256(material).hexdigest()


def _purge_review_sessions(now: float) -> None:
    expired = [
        session_id
        for session_id, record in _review_sessions.items()
        if record.expires_at <= now
    ]
    for session_id in expired:
        _review_sessions.pop(session_id, None)
    while len(_review_sessions) >= REVIEW_SESSION_LIMIT:
        _review_sessions.popitem(last=False)


def _origin_is_same_host(request: Request) -> bool:
    """Valide les en-têtes que le navigateur ajoute à un fetch même origine."""
    if request.headers.get("sec-fetch-site", "").lower() != "same-origin":
        return False
    if request.headers.get(HUMAN_UI_HEADER, "") != "skills-page":
        return False
    origin = request.headers.get("origin", "")
    host = request.headers.get("host", "")
    if not origin or not host:
        return False
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    return hmac.compare_digest(parsed.netloc.casefold(), host.casefold())


def _require_review_authorization(request: Request) -> None:
    """Exige la clé serveur même si ce routeur est monté seul.

    Les métadonnées d'origine et la session à usage unique limitent les erreurs
    d'interface, mais un programme local peut les forger. La clé configurée est
    donc obligatoire pour les sessions et décisions, y compris sur loopback.
    """
    secret = get_api_secret()
    if len(secret) < MIN_API_SECRET_LENGTH:
        raise HTTPException(
            503,
            "La revue humaine exige une API_SECRET_KEY forte configurée sur le serveur.",
            headers={"Cache-Control": "no-store"},
        )
    token = request.headers.get("X-API-Key", "").strip()
    if not token:
        authorization = request.headers.get("Authorization", "").strip()
        if authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()
    if not token:
        raise HTTPException(
            401,
            "Clé API requise pour la revue humaine.",
            headers={"Cache-Control": "no-store"},
        )
    if not hmac.compare_digest(token, secret):
        raise HTTPException(
            403,
            "Clé API invalide.",
            headers={"Cache-Control": "no-store"},
        )


def _issue_review_session(request: Request) -> tuple[str, str]:
    if not _origin_is_same_host(request):
        raise HTTPException(403, "Cette action doit provenir de la page de revue.")
    session_id = secrets.token_urlsafe(32)
    review_token = secrets.token_urlsafe(32)
    now = time.monotonic()
    record = _ReviewSession(
        token_digest=hashlib.sha256(review_token.encode("ascii")).hexdigest(),
        fingerprint=_fingerprint(request),
        expires_at=now + REVIEW_SESSION_TTL_SECONDS,
    )
    with _review_sessions_lock:
        _purge_review_sessions(now)
        _review_sessions[session_id] = record
    return session_id, review_token


def _consume_human_review(request: Request) -> None:
    """Consomme une preuve de revue à usage unique ou refuse la décision."""
    if not _origin_is_same_host(request):
        raise HTTPException(403, "Cette décision doit provenir de la page de revue.")
    session_id = request.cookies.get(REVIEW_COOKIE, "")
    review_token = request.headers.get(REVIEW_TOKEN_HEADER, "")
    if not session_id or not review_token:
        raise HTTPException(403, "Session de revue humaine absente.")
    try:
        token_digest = hashlib.sha256(review_token.encode("ascii")).hexdigest()
    except UnicodeEncodeError as exc:
        raise HTTPException(403, "Session de revue humaine invalide.") from exc

    now = time.monotonic()
    with _review_sessions_lock:
        _purge_review_sessions(now)
        # Le retrait avant comparaison rend le jeton inutilisable après toute
        # tentative, y compris une tentative contenant une mauvaise valeur.
        record = _review_sessions.pop(session_id, None)
    if record is None or record.expires_at <= now:
        raise HTTPException(403, "Session de revue humaine expirée.")
    if not hmac.compare_digest(record.fingerprint, _fingerprint(request)):
        raise HTTPException(403, "Session de revue humaine invalide.")
    if not hmac.compare_digest(record.token_digest, token_digest):
        raise HTTPException(403, "Session de revue humaine invalide.")


def _skill_http_error(exc: SkillError) -> HTTPException:
    if isinstance(exc, SkillNotFoundError):
        return HTTPException(404, str(exc))
    if isinstance(exc, HumanApprovalRequired):
        return HTTPException(403, "Validation humaine obligatoire.")
    if isinstance(exc, SkillConflictError):
        return HTTPException(409, str(exc))
    if isinstance(exc, SkillStateError):
        return HTTPException(409, str(exc))
    if isinstance(exc, SkillValidationError):
        return HTTPException(400, str(exc))
    if isinstance(exc, SkillStorageError):
        return HTTPException(503, "Le registre de compétences est indisponible.")
    return HTTPException(500, "Erreur du registre de compétences.")


async def init_skills_api() -> None:
    """Initialise le registre après la création de la table ``tasks``."""
    await init_agent_skills_db(DB_PATH)
    SKILLS_EXPORT_ROOT.mkdir(parents=True, exist_ok=True)


@router.get("/skills", include_in_schema=False)
async def skills_page():
    return FileResponse(
        STATIC_ROOT / "skills.html",
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": (
                "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
                "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
                "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
                "form-action 'self'"
            ),
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/api/skills")
async def http_list_skills(
    status: SkillStatus | None = None,
    task_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=1_000_000),
):
    try:
        records = await list_skills(
            status=status,
            task_id=task_id,
            limit=limit,
            offset=offset,
            db_path=DB_PATH,
        )
    except SkillError as exc:
        raise _skill_http_error(exc) from exc
    return {
        "skills": [
            record.to_payload(include_instructions=False) for record in records
        ],
        "count": len(records),
    }


@router.get("/api/skills/{skill_id}")
async def http_get_skill(skill_id: int):
    if skill_id <= 0:
        raise HTTPException(400, "Identifiant de compétence invalide.")
    try:
        record = await get_skill(skill_id, db_path=DB_PATH)
    except SkillError as exc:
        raise _skill_http_error(exc) from exc
    return {"skill": record.to_payload(include_instructions=True)}


@router.post("/api/skills/review-session")
async def http_create_review_session(request: Request, response: Response):
    _require_review_authorization(request)
    session_id, review_token = _issue_review_session(request)
    response.set_cookie(
        REVIEW_COOKIE,
        session_id,
        max_age=REVIEW_SESSION_TTL_SECONDS,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/api/skills",
    )
    response.headers["Cache-Control"] = "no-store"
    return {
        "review_token": review_token,
        "expires_in": REVIEW_SESSION_TTL_SECONDS,
    }


@router.post("/api/skills/{skill_id}/decision")
async def http_decide_skill(
    skill_id: int,
    body: SkillDecisionBody,
    request: Request,
):
    _require_review_authorization(request)
    if skill_id <= 0:
        raise HTTPException(400, "Identifiant de compétence invalide.")
    # Cette preuve est consommée avant toute transition. La couche HTTP est la
    # seule à injecter human_confirmed=True et l'identité du relecteur.
    _consume_human_review(request)

    try:
        if body.decision == "reject":
            if body.replace_existing:
                raise HTTPException(
                    400, "replace_existing n'est disponible que pour une approbation."
                )
            reason = (body.reason or "").strip()
            if len(reason) < 3:
                raise HTTPException(400, "Une raison de rejet est obligatoire.")
            record = await reject_skill(
                skill_id,
                rejected_by=HUMAN_REVIEWER,
                reason=reason,
                human_confirmed=True,
                db_path=DB_PATH,
            )
            return {
                "skill": record.to_payload(include_instructions=True),
                "exported": False,
                "export_name": None,
            }

        if body.reason and body.reason.strip():
            raise HTTPException(
                400, "Une raison ne doit être fournie que pour un rejet."
            )
        record = await approve_skill(
            skill_id,
            approved_by=HUMAN_REVIEWER,
            human_confirmed=True,
            replace_existing=body.replace_existing,
            db_path=DB_PATH,
        )
    except SkillError as exc:
        raise _skill_http_error(exc) from exc

    # L'export est un artefact dérivé de la version approuvée. La racine et le
    # nom sont imposés par le serveur ; aucun chemin ne provient de la requête.
    export_name: str | None = None
    export_error: str | None = None
    try:
        exported_path = await export_skill_md(
            record.id,
            SKILLS_EXPORT_ROOT,
            overwrite=True,
            db_path=DB_PATH,
        )
        export_name = exported_path.relative_to(
            SKILLS_EXPORT_ROOT.resolve()
        ).as_posix()
    except (OSError, SkillError) as exc:
        log.error(
            "[skills] activation #%s réussie mais export impossible: %s",
            record.id,
            exc,
        )
        export_error = "La compétence est active, mais son export a échoué."

    return {
        "skill": record.to_payload(include_instructions=True),
        "exported": export_name is not None,
        "export_name": export_name,
        "export_error": export_error,
    }


__all__ = [
    "HUMAN_REVIEWER",
    "SKILLS_EXPORT_ROOT",
    "SkillDecisionBody",
    "init_skills_api",
    "router",
]
