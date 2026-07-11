"""Security policy helpers shared by the HTTP and execution layers.

This module intentionally depends only on the Python standard library so the
most important boundaries can be tested without starting the application.
"""
from __future__ import annotations

import ipaddress
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit


ALLOWED_GENERATED_FILES = frozenset({"app.py", "requirements.txt"})
MAX_GENERATED_FILE_BYTES = 750_000
MAX_GENERATED_TOTAL_BYTES = 1_000_000

DEFAULT_ALLOWED_HOSTS = ("localhost", "127.0.0.1", "::1")
PROXY_MARKER_HEADERS = (
    "forwarded",
    "x-forwarded-for",
    "cf-connecting-ip",
    "true-client-ip",
)

_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_GITHUB_PATH = re.compile(
    r"^/(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git)?/?$"
)
_SENSITIVE_ENV_EXACT = frozenset(
    {
        "API_SECRET_KEY",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_SECRET",
        "DATABASE_URL",
        "AUTHORIZATION",
        "HTTP_AUTHORIZATION",
    }
)
_SENSITIVE_ENV_SUFFIXES = (
    "_API_KEY",
    "_TOKEN",
    "_SECRET",
    "_PASSWORD",
    "_PRIVATE_KEY",
    "_CREDENTIAL",
    "_CREDENTIALS",
)


def validate_generated_app(payload: object) -> dict:
    """Validate and normalize the small Flask bundle returned by the model.

    Only the two files described by the generator contract are accepted.  The
    strict allowlist also rules out absolute paths, UNC paths and traversal.
    """
    if not isinstance(payload, dict):
        raise ValueError("La reponse generee doit etre un objet JSON.")

    raw_files = payload.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise ValueError("La reponse generee ne contient aucun fichier.")
    if len(raw_files) > len(ALLOWED_GENERATED_FILES):
        raise ValueError("La reponse generee contient trop de fichiers.")

    normalized_files: list[dict[str, str]] = []
    seen: set[str] = set()
    total_bytes = 0

    for item in raw_files:
        if not isinstance(item, dict):
            raise ValueError("Chaque fichier genere doit etre un objet JSON.")
        filename = item.get("filename")
        content = item.get("content")
        if not isinstance(filename, str) or not isinstance(content, str):
            raise ValueError("Nom ou contenu de fichier genere invalide.")
        if "\x00" in filename or "\x00" in content:
            raise ValueError("Caractere nul interdit dans un fichier genere.")
        if (
            filename not in ALLOWED_GENERATED_FILES
            or "/" in filename
            or "\\" in filename
            or filename.startswith(("/", "\\"))
            or _WINDOWS_DRIVE.match(filename)
        ):
            raise ValueError("Fichier genere non autorise : " + filename[:120])
        if filename in seen:
            raise ValueError("Fichier genere en double : " + filename)

        size = len(content.encode("utf-8"))
        if size > MAX_GENERATED_FILE_BYTES:
            raise ValueError("Fichier genere trop volumineux : " + filename)
        total_bytes += size
        if total_bytes > MAX_GENERATED_TOTAL_BYTES:
            raise ValueError("Ensemble de fichiers generes trop volumineux.")

        if filename == "requirements.txt":
            requirements = [
                line.strip().lower()
                for line in content.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
            if requirements != ["flask"]:
                raise ValueError("Seule la dependance Flask est autorisee.")
            content = "flask\n"
        elif not content.strip():
            raise ValueError("app.py est vide.")

        normalized_files.append({"filename": filename, "content": content})
        seen.add(filename)

    missing = ALLOWED_GENERATED_FILES - seen
    if missing:
        raise ValueError("Fichier genere obligatoire absent : " + ", ".join(sorted(missing)))

    raw_name = payload.get("name", "app")
    if not isinstance(raw_name, str):
        raw_name = "app"
    name = re.sub(r"[^A-Za-z0-9_. -]", "", raw_name).strip()[:80] or "app"
    return {"name": name, "files": normalized_files}


def build_child_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return an environment suitable for untrusted generated processes.

    This is defense in depth, not an operating-system sandbox.  It prevents the
    most common accidental leak: API keys inherited directly by child code.
    """
    current = os.environ if source is None else source
    clean: dict[str, str] = {}
    for key, value in current.items():
        upper = key.upper()
        if upper in _SENSITIVE_ENV_EXACT:
            continue
        if any(upper.endswith(suffix) for suffix in _SENSITIVE_ENV_SUFFIXES):
            continue
        clean[str(key)] = str(value)
    clean["PYTHONDONTWRITEBYTECODE"] = "1"
    clean["ORCHESTRATOR_CHILD_PROCESS"] = "1"
    return clean


def is_loopback_address(host: str | None) -> bool:
    """Return True only for localhost or an IP loopback address."""
    if not host:
        return False
    value = host.strip().lower()
    if value == "localhost":
        return True
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]
    if "%" in value:
        value = value.split("%", 1)[0]
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def has_proxy_marker(headers: Mapping[str, str]) -> bool:
    """Detect a request that arrived through a forwarding proxy or tunnel."""
    lowered = {str(key).lower(): value for key, value in headers.items()}
    return any(str(lowered.get(name, "")).strip() for name in PROXY_MARKER_HEADERS)


def is_unproxied_local_request(peer_host: str | None, headers: Mapping[str, str]) -> bool:
    """Local mode is allowed only for a real loopback peer, without a tunnel."""
    return is_loopback_address(peer_host) and not has_proxy_marker(headers)


def parse_allowed_hosts(value: str | None) -> tuple[str, ...]:
    """Parse the explicit Host allowlist used to block DNS rebinding."""
    if value is None or not value.strip():
        return DEFAULT_ALLOWED_HOSTS
    hosts = tuple(part.strip().lower().rstrip(".") for part in value.split(",") if part.strip())
    if not hosts or "*" in hosts:
        raise ValueError("ORCHESTRATOR_ALLOWED_HOSTS doit contenir des hotes explicites.")
    return hosts


def _host_without_port(host_header: str | None) -> str:
    if not host_header:
        return ""
    value = host_header.strip().lower().rstrip(".")
    if value.startswith("["):
        end = value.find("]")
        return value[1:end] if end > 0 else ""
    if value.count(":") == 1:
        value = value.rsplit(":", 1)[0]
    return value


def is_allowed_host(host_header: str | None, allowed_hosts: Sequence[str]) -> bool:
    host = _host_without_port(host_header)
    if not host:
        return False
    for allowed in allowed_hosts:
        item = allowed.strip().lower().rstrip(".")
        if item.startswith("*."):
            suffix = item[1:]
            if host.endswith(suffix) and host != suffix[1:]:
                return True
        elif host == item.strip("[]"):
            return True
    return False


def configured_target_roots(value: str | None = None) -> tuple[Path, ...]:
    """Return the configured roots from ORCHESTRATOR_TARGET_ROOTS."""
    raw = os.getenv("ORCHESTRATOR_TARGET_ROOTS", "") if value is None else value
    roots: list[Path] = []
    for item in raw.split(os.pathsep):
        if not item.strip():
            continue
        root = Path(item.strip()).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError("Racine de depot invalide : " + str(root))
        roots.append(root)
    return tuple(roots)


def resolve_allowed_target_path(
    candidate: str,
    allowed_roots: Sequence[Path] | None = None,
) -> Path:
    """Resolve a local repository only when it is inside an approved root."""
    if not candidate or not isinstance(candidate, str):
        raise ValueError("Chemin de depot local manquant.")
    raw_candidate = candidate.strip()
    if raw_candidate.startswith(("\\\\", "//")):
        raise ValueError("Les chemins reseau UNC sont interdits.")

    roots = tuple(allowed_roots) if allowed_roots is not None else configured_target_roots()
    if not roots:
        raise ValueError(
            "Les depots locaux sont desactives. Configurez ORCHESTRATOR_TARGET_ROOTS."
        )
    trusted_roots = tuple(Path(root).expanduser().resolve(strict=True) for root in roots)

    requested = Path(raw_candidate).expanduser()
    if not requested.is_absolute():
        raise ValueError("Le chemin du depot local doit etre absolu.")

    # Comparer d'abord le chemin lexical. Sous Windows, TemporaryDirectory et
    # certains selecteurs natifs peuvent renvoyer un alias 8.3 (RUNNER~1)
    # alors que ``resolve`` developpe le nom long. Dans ce seul cas, on accepte
    # de poursuivre sur le meme volume local ; l'UNC a deja ete refuse et le
    # controle canonique ci-dessous reste obligatoire.
    lexical = Path(os.path.abspath(os.path.normpath(str(requested))))
    lexically_allowed = any(
        lexical == root or root in lexical.parents for root in trusted_roots
    )
    if not lexically_allowed:
        same_windows_volume = os.name == "nt" and any(
            lexical.drive.casefold() == root.drive.casefold()
            for root in trusted_roots
        )
        if not same_windows_volume:
            raise ValueError("Le depot local est hors des racines autorisees.")

    # Resolve once the lexical boundary passed, then check again to catch a
    # symlink or junction that escapes an allowed root.
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("Le chemin cible n'est pas un dossier.")
    if not any(resolved == root or root in resolved.parents for root in trusted_roots):
        raise ValueError(
            "Le depot local est hors des racines autorisees apres resolution."
        )
    if not (resolved / ".git").exists():
        raise ValueError("Le dossier cible n'est pas un depot Git.")
    return resolved


def validate_github_repo_url(url: str) -> str:
    """Accept a canonical github.com repository URL and nothing else."""
    value = (url or "").strip()
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "github.com"
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.port not in (None, 443)
    ):
        raise ValueError("Seules les URL de depot https://github.com sont autorisees.")
    match = _GITHUB_PATH.fullmatch(parsed.path)
    if not match or match.group("repo") in {".", ".."}:
        raise ValueError("URL GitHub de depot invalide.")
    return "https://github.com/{}/{}.git".format(match.group("owner"), match.group("repo"))
