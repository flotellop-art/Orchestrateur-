"""input_validation.py — Modeles Pydantic de validation et sanitization des entrees.

Fonctionnalites :
- Validation des inputs (longueur max, types, formats)
- Sanitization anti-XSS (echappement HTML, suppression balises)
- Validation des chemins de fichiers (anti-path traversal)
- Modeles couvrant tous les endpoints de orchestrator.py et team.py

Integration : voir SECURITY_INSTALL.md
"""
from __future__ import annotations

import html
import logging
import os
import re
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes de validation
# ---------------------------------------------------------------------------

MAX_DESCRIPTION_LEN  = 4_000   # description d'une app / objectif d'une tache
MAX_MESSAGE_LEN      = 8_000   # message de chat
MAX_FILENAME_LEN     = 255     # nom de fichier
MAX_FILE_CONTENT_LEN = 500_000 # contenu de fichier (~500 Ko)
MAX_URL_LEN          = 2_048
MAX_GITHUB_URL_LEN   = 512
MAX_PROVIDER_LEN     = 32
MAX_MODEL_LEN        = 64
MAX_TASK_ID_VAL      = 2**31 - 1  # SQLite INTEGER max

# Regex : identifiant alphanum + tirets/underscores
_RE_SAFE_IDENTIFIER  = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')
# Regex : nom de fichier safe (pas de chemin, pas de caracteres dangereux)
_RE_SAFE_FILENAME    = re.compile(r'^[a-zA-Z0-9_\-\.\s]{1,255}$')
# Caracteres dangereux dans les noms de fichiers
_DANGEROUS_PATH_CHARS = re.compile(r'[\.]{2,}|[/\\<>:"\|\?\*\x00-\x1f]')
# Sequences XSS communes
_RE_SCRIPT_TAG       = re.compile(r'<[^>]*script[^>]*>', re.IGNORECASE)
_RE_EVENT_HANDLER    = re.compile(r'on\w+\s*=', re.IGNORECASE)
_RE_JAVASCRIPT_URI   = re.compile(r'javascript\s*:', re.IGNORECASE)
_RE_DATA_URI         = re.compile(r'data\s*:[^,]*base64', re.IGNORECASE)
# Providers autorises
_ALLOWED_PROVIDERS   = {"claude", "gemini", "openai", "anthropic"}


# ---------------------------------------------------------------------------
# Fonctions de sanitization
# ---------------------------------------------------------------------------

def sanitize_text(value: str) -> str:
    """Sanitize une chaine de texte libre :
    - Echappe les caracteres HTML (<, >, &, ", ')
    - Supprime les balises <script> et handlers d'evenements
    - Refuse les URI javascript: et data:base64
    """
    if not isinstance(value, str):
        return str(value)

    # Detecter et rejeter les payloads XSS manifestes avant echappement
    if _RE_SCRIPT_TAG.search(value):
        log.warning("[VALIDATION] Balise script detectee dans l'input, sanitisation forcee.")
        value = _RE_SCRIPT_TAG.sub('', value)

    if _RE_JAVASCRIPT_URI.search(value):
        log.warning("[VALIDATION] URI javascript: detectee, suppression.")
        value = _RE_JAVASCRIPT_URI.sub('', value)

    if _RE_DATA_URI.search(value):
        log.warning("[VALIDATION] URI data:base64 detectee, suppression.")
        value = _RE_DATA_URI.sub('', value)

    if _RE_EVENT_HANDLER.search(value):
        log.warning("[VALIDATION] Handler d'evenement HTML detecte, suppression.")
        value = _RE_EVENT_HANDLER.sub('', value)

    # Echappement HTML
    return html.escape(value, quote=True)


def validate_file_path(path_str: str, base_dir: Optional[Path] = None) -> str:
    """Valide un chemin de fichier contre le path traversal.

    - Refuse les sequences .. dans le chemin
    - Refuse les chemins absolus (sauf si base_dir fourni et chemin dedans)
    - Refuse les caracteres dangereux
    - Si base_dir fourni : verifie que le chemin resolu reste dans base_dir

    Retourne le chemin normalise (string) ou leve ValueError.
    """
    if not path_str or not isinstance(path_str, str):
        raise ValueError("Chemin de fichier invalide ou vide.")

    if len(path_str) > MAX_FILENAME_LEN * 4:
        raise ValueError(f"Chemin trop long ({len(path_str)} > {MAX_FILENAME_LEN * 4}).")

    # Normaliser les separateurs
    normalized = path_str.replace('\\', '/')

    # Refuser les sequences de points (..)
    if '..' in normalized.split('/'):
        raise ValueError(f"Path traversal detecte dans le chemin : {path_str!r}")

    # Refuser les NULL bytes
    if '\x00' in normalized:
        raise ValueError("Caractere NULL detecte dans le chemin.")

    # Si base_dir fourni, verifier que le chemin resolu reste a l'interieur
    if base_dir is not None:
        try:
            base_resolved = Path(base_dir).resolve()
            target_resolved = (base_resolved / normalized).resolve()
            # S'assurer que target est bien sous base
            target_resolved.relative_to(base_resolved)
        except (ValueError, OSError) as exc:
            raise ValueError(
                f"Chemin hors du repertoire autorise : {path_str!r}"
            ) from exc

    return normalized


# ---------------------------------------------------------------------------
# Modeles Pydantic — orchestrator.py
# ---------------------------------------------------------------------------

class CreateAppRequest(BaseModel):
    """POST /api/create — Creation d'une application."""

    description: Annotated[str, Field(
        min_length=10,
        max_length=MAX_DESCRIPTION_LEN,
        description="Description de l'application a creer",
    )]

    @field_validator('description')
    @classmethod
    def sanitize_description(cls, v: str) -> str:
        return sanitize_text(v)


class AppActionRequest(BaseModel):
    """POST /api/apps/{id}/start — Demarrage d'une app."""
    # Pas de body requis, mais le modele evite d'injecter des champs inconnus
    model_config = {"extra": "forbid"}


# ---------------------------------------------------------------------------
# Modeles Pydantic — team.py
# ---------------------------------------------------------------------------

class CreateTaskRequest(BaseModel):
    """POST /api/tasks — Creation d'une tache multi-agents."""

    objective: Annotated[str, Field(
        min_length=10,
        max_length=MAX_DESCRIPTION_LEN,
        description="Objectif de la tache",
    )]
    max_iterations: Annotated[int, Field(
        default=15,
        ge=1,
        le=300,
        description="Nombre maximum d'iterations",
    )] = 15
    max_agents: Annotated[int, Field(
        default=4,
        ge=1,
        le=24,
        description="Nombre maximum d'agents",
    )] = 4
    provider: Annotated[str, Field(
        default="claude",
        max_length=MAX_PROVIDER_LEN,
        description="Fournisseur LLM",
    )] = "claude"
    model: Annotated[Optional[str], Field(
        default=None,
        max_length=MAX_MODEL_LEN,
        description="Modele LLM",
    )] = None
    github_url: Annotated[Optional[str], Field(
        default=None,
        max_length=MAX_GITHUB_URL_LEN,
        description="URL GitHub optionnelle",
    )] = None
    mode: Annotated[Optional[str], Field(
        default=None,
        max_length=32,
        description="Mode d'execution",
    )] = None

    model_config = {"extra": "forbid"}

    @field_validator('objective')
    @classmethod
    def sanitize_objective(cls, v: str) -> str:
        return sanitize_text(v)

    @field_validator('provider')
    @classmethod
    def validate_provider(cls, v: str) -> str:
        v_lower = v.lower().strip()
        if v_lower not in _ALLOWED_PROVIDERS:
            raise ValueError(
                f"Provider invalide : {v!r}. Valeurs autorisees : {sorted(_ALLOWED_PROVIDERS)}"
            )
        return v_lower

    @field_validator('github_url')
    @classmethod
    def validate_github_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v.startswith(("https://github.com/", "https://gitlab.com/",
                              "https://bitbucket.org/")):
            raise ValueError(
                f"URL de depot invalide : seuls github.com, gitlab.com et bitbucket.org sont autorises."
            )
        # Refuser les caracteres dangereux dans l'URL
        if re.search(r'[;&|`$()\'"]', v):
            raise ValueError("URL contient des caracteres non autorises.")
        return v

    @field_validator('mode')
    @classmethod
    def validate_mode(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        allowed_modes = {"company", "standard", "audit", None}
        if v.lower() not in {m for m in allowed_modes if m}:
            raise ValueError(f"Mode invalide : {v!r}")
        return v.lower()


class FileWriteRequest(BaseModel):
    """POST /api/tasks/{id}/file — Ecriture dans un fichier de tache."""

    path: Annotated[str, Field(
        max_length=MAX_FILENAME_LEN,
        description="Chemin relatif du fichier dans le dossier de tache",
    )]
    content: Annotated[str, Field(
        max_length=MAX_FILE_CONTENT_LEN,
        description="Contenu du fichier",
    )]

    model_config = {"extra": "forbid"}

    @field_validator('path')
    @classmethod
    def validate_path(cls, v: str) -> str:
        # Valider contre le path traversal (sans base_dir ici, verifie au niveau route)
        return validate_file_path(v)


class FileReadRequest(BaseModel):
    """GET /api/tasks/{id}/file?path=... — Lecture d'un fichier."""

    path: Annotated[str, Field(
        max_length=MAX_FILENAME_LEN,
        description="Chemin relatif du fichier dans le dossier de tache",
    )]

    @field_validator('path')
    @classmethod
    def validate_path(cls, v: str) -> str:
        return validate_file_path(v)


# ---------------------------------------------------------------------------
# Modeles Pydantic — chat_agent.py
# ---------------------------------------------------------------------------

class ChatMessageRequest(BaseModel):
    """POST /api/chat/message — Envoi d'un message au chat agent."""

    message: Annotated[str, Field(
        min_length=1,
        max_length=MAX_MESSAGE_LEN,
        description="Message de l'utilisateur",
    )]
    conversation_id: Annotated[Optional[str], Field(
        default=None,
        max_length=64,
        description="Identifiant de conversation UUID",
    )] = None

    model_config = {"extra": "forbid"}

    @field_validator('message')
    @classmethod
    def sanitize_message(cls, v: str) -> str:
        return sanitize_text(v)

    @field_validator('conversation_id')
    @classmethod
    def validate_conversation_id(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # UUID format: 8-4-4-4-12
        if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                        v, re.IGNORECASE):
            raise ValueError(f"conversation_id invalide : doit etre un UUID. Recu : {v!r}")
        return v


# ---------------------------------------------------------------------------
# Utilitaire : validation de task_id (entier positif)
# ---------------------------------------------------------------------------

def validate_task_id(task_id: Any) -> int:
    """Valide un task_id recu depuis un path parameter.

    Leve ValueError si invalide.
    """
    try:
        tid = int(task_id)
    except (TypeError, ValueError):
        raise ValueError(f"task_id invalide : {task_id!r} n'est pas un entier.")
    if tid <= 0 or tid > MAX_TASK_ID_VAL:
        raise ValueError(f"task_id hors plage : {tid}")
    return tid
