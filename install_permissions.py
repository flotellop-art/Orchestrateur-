"""Plans d'installation structures et politique de consentement.

Ce module ne lance jamais de processus. Il transforme un petit payload strict
en plan immuable, decide si ce plan doit etre refuse, confirme ou execute
automatiquement, puis construit un ``argv`` utilisable avec ``subprocess`` sans
shell. Les appels reseau et l'execution restent a la charge de l'orchestrateur.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class InstallValidationError(ValueError):
    """Le plan d'installation ne respecte pas le contrat de securite."""


class _TextEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class InstallManager(_TextEnum):
    PYTHON = "python"
    NPM = "npm"
    WINGET = "winget"


class AccessLevel(_TextEnum):
    PROJECT = "project"
    USER = "user"
    ADMIN = "admin"


class InstallPolicy(_TextEnum):
    BLOCKED = "blocked"
    ASK = "ask"
    PROJECT = "project"
    USER = "user"
    ADMIN = "admin"


class DecisionOutcome(_TextEnum):
    DENIED = "denied"
    PROMPT = "prompt"
    AUTOMATIC = "automatic"


_REQUEST_FIELDS = frozenset(
    {"manager", "package", "version", "scope", "allow_scripts", "source"}
)
_PACKAGE_RE = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?\Z",
    re.ASCII,
)
_NPM_SCOPED_PACKAGE_RE = re.compile(
    r"@[A-Za-z0-9](?:[A-Za-z0-9._-]{0,61}[A-Za-z0-9])?/"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?\Z",
    re.ASCII,
)
_VERSION_RE = re.compile(
    r"(?=[A-Za-z0-9._+-]*[0-9])"
    r"[A-Za-z0-9](?:[A-Za-z0-9._+-]{0,126}[A-Za-z0-9])?\Z",
    re.ASCII,
)
_ACCESS_RANK = {
    AccessLevel.PROJECT: 0,
    AccessLevel.USER: 1,
    AccessLevel.ADMIN: 2,
}
_POLICY_RANK = {
    InstallPolicy.ASK: 0,
    InstallPolicy.PROJECT: 0,
    InstallPolicy.USER: 1,
    InstallPolicy.ADMIN: 2,
}


def _enum_value(enum_type: type[_TextEnum], value: object, label: str) -> _TextEnum:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise InstallValidationError(f"{label} doit etre une chaine.")
    try:
        return enum_type(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise InstallValidationError(
            f"{label} invalide : {value!r}. Valeurs permises : {allowed}."
        ) from exc


def _validate_package(value: object, manager: InstallManager) -> str:
    if not isinstance(value, str) or not value:
        raise InstallValidationError("package est obligatoire.")
    valid_name = bool(_PACKAGE_RE.fullmatch(value))
    if manager is InstallManager.NPM:
        valid_name = valid_name or bool(_NPM_SCOPED_PACKAGE_RE.fullmatch(value))
    if not value.isascii() or not valid_name or ".." in value:
        raise InstallValidationError(
            "package doit etre un identifiant ASCII simple, sans URL, chemin ni option."
        )
    return value


def _validate_version(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise InstallValidationError("version exacte est obligatoire.")
    if not value.isascii() or not _VERSION_RE.fullmatch(value) or ".." in value:
        raise InstallValidationError(
            "version doit etre une version exacte ASCII, sans URL, chemin ni option."
        )
    return value


def _validate_executable(value: str | os.PathLike[str] | None, label: str) -> str:
    if value is None:
        raise InstallValidationError(f"{label} est obligatoire.")
    try:
        executable = os.fspath(value)
    except TypeError as exc:
        raise InstallValidationError(f"{label} est invalide.") from exc
    if not isinstance(executable, str) or not executable or "\x00" in executable:
        raise InstallValidationError(f"{label} est invalide.")
    return executable


@dataclass(frozen=True, slots=True)
class InstallRequest:
    """Entree normalisee issue de l'appel d'outil structure."""

    manager: InstallManager
    package: str
    version: str
    scope: str | None = None
    allow_scripts: bool = False
    source: str | None = None

    def __post_init__(self) -> None:
        manager = _enum_value(InstallManager, self.manager, "manager")
        package = _validate_package(self.package, manager)
        version = _validate_version(self.version)
        if not isinstance(self.allow_scripts, bool):
            raise InstallValidationError("allow_scripts doit etre un booleen.")

        scope = self.scope
        source = self.source
        if manager in {InstallManager.PYTHON, InstallManager.NPM}:
            if scope is None:
                scope = AccessLevel.PROJECT.value
            if scope != AccessLevel.PROJECT.value:
                raise InstallValidationError(
                    f"{manager.value} ne peut installer que dans le projet."
                )
            if source is not None:
                raise InstallValidationError(
                    "source n'est permise que pour le gestionnaire winget."
                )
            if manager is InstallManager.PYTHON and self.allow_scripts:
                raise InstallValidationError(
                    "allow_scripts n'est disponible que pour npm."
                )
            if manager is InstallManager.NPM and self.allow_scripts:
                raise InstallValidationError(
                    "Les scripts npm sont refuses tant que les agents ne sont pas isoles."
                )
        else:
            if scope not in {AccessLevel.USER.value, "system"}:
                raise InstallValidationError(
                    "winget exige un scope explicite user ou system."
                )
            if source is None:
                source = InstallManager.WINGET.value
            if source != InstallManager.WINGET.value:
                raise InstallValidationError("Seule la source winget est permise.")
            if self.allow_scripts:
                raise InstallValidationError(
                    "allow_scripts n'est disponible que pour npm."
                )

        object.__setattr__(self, "manager", manager)
        object.__setattr__(self, "package", package)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "source", source)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "InstallRequest":
        """Refuse les champs libres afin qu'aucune option CLI ne soit injectable."""
        if not isinstance(payload, Mapping):
            raise InstallValidationError("La demande d'installation doit etre un objet.")
        unknown = set(payload) - _REQUEST_FIELDS
        if unknown:
            rendered = ", ".join(sorted(repr(item) for item in unknown))
            raise InstallValidationError(f"Champs d'installation inconnus : {rendered}.")
        missing = {"manager", "package", "version"} - set(payload)
        if missing:
            raise InstallValidationError(
                "Champs d'installation manquants : " + ", ".join(sorted(missing)) + "."
            )
        return cls(
            manager=payload["manager"],
            package=payload["package"],
            version=payload["version"],
            scope=payload.get("scope"),
            allow_scripts=payload.get("allow_scripts", False),
            source=payload.get("source"),
        )

    def to_plan(self, *, platform: str | None = None) -> "InstallPlan":
        current_platform = sys.platform if platform is None else platform
        if self.manager is InstallManager.WINGET and current_platform != "win32":
            raise InstallValidationError("winget est disponible uniquement sur Windows.")
        return InstallPlan(
            manager=self.manager,
            package=self.package,
            version=self.version,
            scope=self.scope,
            allow_scripts=self.allow_scripts,
            source=self.source,
        )


@dataclass(frozen=True, slots=True)
class InstallPlan:
    """Plan immuable ; les droits et l'empreinte ne sont jamais fournis par l'agent."""

    manager: InstallManager
    package: str
    version: str
    scope: str
    allow_scripts: bool = False
    source: str | None = None
    access: AccessLevel = field(init=False)
    reason: str = field(init=False)
    plan_hash: str = field(init=False)

    def __post_init__(self) -> None:
        request = InstallRequest(
            manager=self.manager,
            package=self.package,
            version=self.version,
            scope=self.scope,
            allow_scripts=self.allow_scripts,
            source=self.source,
        )
        manager = request.manager
        if manager is InstallManager.PYTHON:
            access = AccessLevel.PROJECT
            reason = "Installe des roues Python dans l'environnement virtuel du projet."
        elif manager is InstallManager.NPM:
            access = AccessLevel.PROJECT
            reason = "Ajoute une dependance npm au projet sans lancer ses scripts."
        elif request.scope == AccessLevel.USER.value:
            access = AccessLevel.USER
            reason = (
                "Installe une application Windows pour votre compte et accepte "
                "les accords de la source et du paquet winget."
            )
        else:
            access = AccessLevel.ADMIN
            reason = (
                "Installe une application Windows pour tous les comptes et accepte "
                "les accords de la source et du paquet winget."
            )

        canonical = {
            "access": access.value,
            "allow_scripts": request.allow_scripts,
            "manager": manager.value,
            "package": request.package,
            "schema": 1,
            "scope": request.scope,
            "source": request.source,
            "version": request.version,
        }
        encoded = json.dumps(
            canonical,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")

        object.__setattr__(self, "manager", manager)
        object.__setattr__(self, "package", request.package)
        object.__setattr__(self, "version", request.version)
        object.__setattr__(self, "scope", request.scope)
        object.__setattr__(self, "allow_scripts", request.allow_scripts)
        object.__setattr__(self, "source", request.source)
        object.__setattr__(self, "access", access)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "plan_hash", hashlib.sha256(encoded).hexdigest())

    @property
    def display(self) -> str:
        return f"{self.package} {self.version} via {self.manager.value}"

    @property
    def command_preview(self) -> tuple[str, ...]:
        if self.manager is InstallManager.PYTHON:
            executable: str | Path = "<venv-python>"
            return build_install_argv(self, venv_python=executable)
        if self.manager is InstallManager.NPM:
            return build_install_argv(
                self, npm_executable="npm", npm_prefix="<task-npm-env>"
            )
        # Le plan winget a deja ete refuse hors win32 par ``to_plan``. Le
        # preview est une representation pure qui ne sonde pas la machine.
        return _build_winget_argv(self, "winget")

    def to_payload(self) -> dict[str, object]:
        """Payload complet et directement affichable par l'interface."""
        return {
            "manager": self.manager.value,
            "package": self.package,
            "version": self.version,
            "scope": self.scope,
            "access": self.access.value,
            "reason": self.reason,
            "allow_scripts": self.allow_scripts,
            "source": self.source,
            "plan_hash": self.plan_hash,
            "display": self.display,
            "command_preview": list(self.command_preview),
        }


def make_install_plan(
    payload: Mapping[str, Any], *, platform: str | None = None
) -> InstallPlan:
    """Point d'entree court pour un appel d'outil JSON."""
    return InstallRequest.from_payload(payload).to_plan(platform=platform)


@dataclass(frozen=True, slots=True)
class InstallDecision:
    plan: InstallPlan
    outcome: DecisionOutcome
    reason: str

    @property
    def plan_hash(self) -> str:
        return self.plan.plan_hash

    @property
    def access(self) -> AccessLevel:
        return self.plan.access

    def to_payload(self) -> dict[str, object]:
        return {
            "outcome": self.outcome.value,
            "reason": self.reason,
            "plan_hash": self.plan_hash,
            "access": self.access.value,
        }


def evaluate_install(
    plan: InstallPlan,
    policy: InstallPolicy | str,
) -> InstallDecision:
    """Applique la politique de la tache.

    ``blocked`` reste prioritaire. La politique fixe le niveau maximal que la
    tache peut demander. Seules les dependances strictement locales au projet
    peuvent etre automatiques ; les niveaux utilisateur et administrateur
    demandent toujours un accord visible.
    """
    if not isinstance(plan, InstallPlan):
        raise InstallValidationError("plan d'installation invalide.")
    normalized_policy = _enum_value(InstallPolicy, policy, "policy")
    if normalized_policy is InstallPolicy.BLOCKED:
        return InstallDecision(
            plan,
            DecisionOutcome.DENIED,
            "Les installations sont desactivees par la politique active.",
        )

    policy_rank = _POLICY_RANK[normalized_policy]
    if _ACCESS_RANK[plan.access] > policy_rank:
        return InstallDecision(
            plan,
            DecisionOutcome.DENIED,
            "Ce niveau d'acces depasse la limite fixee pour cette tache.",
        )

    if plan.access is AccessLevel.PROJECT and normalized_policy is not InstallPolicy.ASK:
        return InstallDecision(
            plan,
            DecisionOutcome.AUTOMATIC,
            "Les dependances locales au projet sont autorisees automatiquement.",
        )

    return InstallDecision(
        plan,
        DecisionOutcome.PROMPT,
        "Une confirmation est requise pour ce niveau d'acces.",
    )


def _build_winget_argv(plan: InstallPlan, executable: str) -> tuple[str, ...]:
    command_scope = "user" if plan.scope == AccessLevel.USER.value else "machine"
    return (
        executable,
        "install",
        "--source",
        InstallManager.WINGET.value,
        "--id",
        plan.package,
        "--version",
        plan.version,
        "--exact",
        "--scope",
        command_scope,
        "--accept-package-agreements",
        "--accept-source-agreements",
        "--disable-interactivity",
    )


def build_install_argv(
    plan: InstallPlan,
    *,
    venv_python: str | os.PathLike[str] | None = None,
    npm_executable: str | os.PathLike[str] = "npm",
    npm_prefix: str | os.PathLike[str] | None = None,
    winget_executable: str | os.PathLike[str] = "winget",
    platform: str | None = None,
) -> tuple[str, ...]:
    """Construit une liste d'arguments ; aucune chaine de shell n'est produite."""
    if not isinstance(plan, InstallPlan):
        raise InstallValidationError("plan d'installation invalide.")

    if plan.manager is InstallManager.PYTHON:
        executable = _validate_executable(venv_python, "venv_python")
        return (
            executable,
            "-I",
            "-m",
            "pip",
            "install",
            "--isolated",
            "--require-virtualenv",
            "--only-binary=:all:",
            "--no-input",
            "--disable-pip-version-check",
            "--index-url",
            "https://pypi.org/simple",
            f"{plan.package}=={plan.version}",
        )

    if plan.manager is InstallManager.NPM:
        executable = _validate_executable(npm_executable, "npm_executable")
        argv = [
            executable,
            "install",
            "--save-exact",
            "--registry=https://registry.npmjs.org/",
            "--global=false",
            "--userconfig=" + os.devnull,
            "--package-lock=true",
        ]
        if npm_prefix is not None:
            argv.extend(("--prefix", _validate_executable(npm_prefix, "npm_prefix")))
        argv.append("--ignore-scripts")
        argv.extend(("--", f"{plan.package}@{plan.version}"))
        return tuple(argv)

    current_platform = sys.platform if platform is None else platform
    if current_platform != "win32":
        raise InstallValidationError("winget est disponible uniquement sur Windows.")
    executable = _validate_executable(winget_executable, "winget_executable")
    return _build_winget_argv(plan, executable)


__all__ = [
    "AccessLevel",
    "DecisionOutcome",
    "InstallDecision",
    "InstallManager",
    "InstallPlan",
    "InstallPolicy",
    "InstallRequest",
    "InstallValidationError",
    "build_install_argv",
    "evaluate_install",
    "make_install_plan",
]
