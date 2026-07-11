"""Exécution de commandes d'agents dans des conteneurs Docker verrouillés.

Ce module est volontairement indépendant de ``team.py``.  Il fournit une
frontière unique que l'orchestrateur pourra brancher sur ``run_command``,
``run_tests`` et le lancement d'applications.  Aucun repli vers une exécution
locale n'est effectué : si Docker ou l'image de confiance ne sont pas
disponibles, l'appel échoue explicitement.

Le backend utilise le client Docker en arguments structurés (jamais de shell),
un conteneur par exécution et des labels privés à cette installation.  L'image
doit être préparée par l'administrateur ; elle n'est jamais téléchargée à la
demande d'un agent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Protocol, Sequence


log = logging.getLogger(__name__)


_MANAGED_LABEL = "com.orchestrator.sandbox"
_NAMESPACE_LABEL = "com.orchestrator.namespace"
_TASK_LABEL = "com.orchestrator.task-id"
_WORKSPACE_LABEL = "com.orchestrator.workspace"
_KIND_LABEL = "com.orchestrator.kind"
_CONTAINER_ID_RE = re.compile(r"^[0-9a-f]{12,64}$")
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_IMAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}$")
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_MEMORY_RE = re.compile(r"^[1-9][0-9]*[kKmMgG]$")
_DOCKER_MINIMUM_VERSION = (23, 0)


class SandboxError(RuntimeError):
    """Erreur de base présentable à l'utilisateur."""


class SandboxUnavailableError(SandboxError):
    """Docker ou l'image approuvée ne sont pas disponibles."""


class SandboxValidationError(SandboxError, ValueError):
    """Une demande sortirait de la frontière autorisée."""


class SandboxExecutionError(SandboxError):
    """Docker n'a pas pu créer, démarrer ou inspecter le conteneur."""


class SandboxStoppedError(SandboxError):
    """La tâche a été arrêtée pendant la création de son conteneur."""


class SandboxStopError(SandboxExecutionError):
    """Au moins un conteneur n'a pas pu être arrêté."""

    def __init__(self, result: "SandboxStopResult") -> None:
        self.result = result
        details = "; ".join(f"{cid}: {reason}" for cid, reason in result.failed)
        super().__init__("Impossible d'arrêter tous les conteneurs de la tâche. " + details)


@dataclass(frozen=True)
class CliResult:
    """Résultat brut d'un appel au client Docker."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


class CliTimeoutError(TimeoutError):
    """Le client Docker local a dépassé son délai."""

    def __init__(self, stdout: str = "", stderr: str = "") -> None:
        self.stdout = stdout
        self.stderr = stderr
        super().__init__("Le client Docker n'a pas répondu dans le délai imparti.")


class CliRunner(Protocol):
    async def run(
        self, argv: Sequence[str], *, timeout: float | None = None
    ) -> CliResult:
        """Lance un processus sans shell et renvoie ses sorties."""


class AsyncioCliRunner:
    """Client de processus borné, avec arrêt de l'arbre en cas de délai."""

    def __init__(self, max_output_bytes: int = 128 * 1024) -> None:
        if max_output_bytes < 4096:
            raise ValueError("max_output_bytes doit être supérieur ou égal à 4096")
        self.max_output_bytes = max_output_bytes

    async def run(
        self, argv: Sequence[str], *, timeout: float | None = None
    ) -> CliResult:
        creation: dict[str, object] = {}
        if os.name == "nt":
            creation["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            creation["start_new_session"] = True

        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **creation,
            )
        except FileNotFoundError as exc:
            raise SandboxUnavailableError(
                "Docker est introuvable. Installez Docker Desktop puis redémarrez "
                "l'Orchestrateur."
            ) from exc
        except OSError as exc:
            raise SandboxUnavailableError(
                f"Docker ne peut pas être lancé : {exc}"
            ) from exc

        stdout_task = asyncio.create_task(self._read_limited(process.stdout))
        stderr_task = asyncio.create_task(self._read_limited(process.stderr))
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            await self._terminate_tree(process)
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
            raise CliTimeoutError(stdout, stderr) from exc
        except asyncio.CancelledError:
            await self._terminate_tree(process)
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise

        stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
        return CliResult(process.returncode or 0, stdout, stderr)

    async def _read_limited(
        self, stream: asyncio.StreamReader | None
    ) -> str:
        if stream is None:
            return ""
        kept = bytearray()
        omitted = 0
        while True:
            chunk = await stream.read(8192)
            if not chunk:
                break
            room = max(0, self.max_output_bytes - len(kept))
            kept.extend(chunk[:room])
            omitted += max(0, len(chunk) - room)
        text = kept.decode("utf-8", errors="replace")
        if omitted:
            text += f"\n… {omitted} octets de sortie ont été ignorés."
        return text

    async def _terminate_tree(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        if os.name == "nt":
            try:
                killer = await asyncio.create_subprocess_exec(
                    "taskkill",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
                await asyncio.wait_for(killer.wait(), timeout=5)
            except (FileNotFoundError, OSError, asyncio.TimeoutError):
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass


def _default_user() -> str:
    if hasattr(os, "getuid") and hasattr(os, "getgid"):
        uid = int(os.getuid())
        gid = int(os.getgid())
        if uid > 0:
            return f"{uid}:{max(gid, 1)}"
    # Docker Desktop autorise ce compte numérique à écrire dans un bind mount
    # Windows. Sur Linux lancé en root, l'intégrateur peut fournir l'UID hôte.
    return "65532:65532"


def _default_namespace() -> str:
    configured = os.getenv("ORCHESTRATOR_SANDBOX_NAMESPACE", "").strip().lower()
    if configured:
        return configured
    location = str(Path(__file__).resolve().parent).casefold().encode("utf-8")
    return "orch-" + hashlib.sha256(location).hexdigest()[:12]


def _default_roots() -> tuple[Path, ...]:
    return (Path(__file__).resolve().parent / "projects",)


def _memory_bytes(value: str) -> int:
    match = _MEMORY_RE.fullmatch(value)
    if not match:
        raise SandboxValidationError(
            "La mémoire doit être exprimée comme 512m ou 1g."
        )
    units = {"k": 1024, "m": 1024**2, "g": 1024**3}
    return int(value[:-1]) * units[value[-1].lower()]


@dataclass(frozen=True)
class SandboxConfig:
    """Configuration administrateur de la frontière Docker."""

    docker_bin: str = field(
        default_factory=lambda: os.getenv("ORCHESTRATOR_DOCKER_BIN", "docker")
    )
    image: str = field(
        default_factory=lambda: os.getenv(
            "ORCHESTRATOR_SANDBOX_IMAGE", "orchestrator-sandbox:0.2.0"
        )
    )
    allowed_workspace_roots: tuple[Path, ...] = field(default_factory=_default_roots)
    namespace: str = field(default_factory=_default_namespace)
    user: str = field(default_factory=_default_user)
    cpus: float = 1.0
    memory: str = "512m"
    pids_limit: int = 128
    default_timeout: float = 120.0
    lifecycle_timeout: float = 15.0
    stop_timeout: int = 5
    max_launch_seconds: float = 3600.0
    max_output_chars: int = 20_000
    allow_network: bool = False
    allowed_env_keys: tuple[str, ...] = (
        "CI",
        "NODE_ENV",
        "NO_COLOR",
        "PORT",
        "PYTHONPATH",
        "PYTHONUNBUFFERED",
    )

    def __post_init__(self) -> None:
        roots = tuple(Path(root).expanduser().resolve(strict=False) for root in self.allowed_workspace_roots)
        object.__setattr__(self, "allowed_workspace_roots", roots)
        object.__setattr__(self, "allowed_env_keys", tuple(self.allowed_env_keys))

        if not self.docker_bin or "\x00" in self.docker_bin:
            raise SandboxValidationError("Le chemin du client Docker est invalide.")
        if not _IMAGE_RE.fullmatch(self.image):
            raise SandboxValidationError("Le nom de l'image Docker est invalide.")
        user_match = re.fullmatch(r"([0-9]+):([0-9]+)", self.user)
        if not user_match or int(user_match.group(1)) == 0:
            raise SandboxValidationError(
                "Le conteneur doit utiliser un UID numérique différent de root."
            )
        if not self.allowed_workspace_roots:
            raise SandboxValidationError(
                "Au moins une racine de projets autorisée est obligatoire."
            )
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,31}", self.namespace):
            raise SandboxValidationError("L'identifiant de l'installation est invalide.")
        if not (0.1 <= self.cpus <= 64):
            raise SandboxValidationError("La limite CPU doit être comprise entre 0,1 et 64.")
        memory_bytes = _memory_bytes(self.memory)
        if memory_bytes < 6 * 1024**2:
            raise SandboxValidationError(
                "Docker exige une limite mémoire d'au moins 6 Mo."
            )
        if not (16 <= self.pids_limit <= 4096):
            raise SandboxValidationError(
                "La limite de processus doit être comprise entre 16 et 4096."
            )
        if self.default_timeout <= 0 or self.lifecycle_timeout <= 0:
            raise SandboxValidationError("Les délais doivent être strictement positifs.")
        if not (1 <= self.stop_timeout <= 60):
            raise SandboxValidationError("Le délai d'arrêt doit être compris entre 1 et 60 s.")
        if self.max_launch_seconds <= 0:
            raise SandboxValidationError(
                "La durée maximale d'une application doit être positive."
            )
        if self.max_output_chars < 1000:
            raise SandboxValidationError("La borne de sortie est trop petite.")
        for key in self.allowed_env_keys:
            if not _ENV_KEY_RE.fullmatch(key):
                raise SandboxValidationError(
                    f"La variable d'environnement autorisée {key!r} est invalide."
                )


@dataclass(frozen=True)
class SandboxStatus:
    available: bool
    message: str
    docker_version: str | None = None
    image_ready: bool = False


@dataclass(frozen=True)
class SandboxResult:
    task_id: str
    exit_code: int
    output: str
    timed_out: bool
    duration_seconds: float

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


@dataclass(frozen=True)
class SandboxHandle:
    task_id: str
    container_id: str
    container_name: str
    published_ports: tuple[tuple[int, int], ...]
    lifetime_seconds: float


@dataclass(frozen=True)
class SandboxStopResult:
    task_id: str
    stopped: tuple[str, ...]
    failed: tuple[tuple[str, str], ...]

    @property
    def ok(self) -> bool:
        return not self.failed


class DockerSandboxRuntime:
    """Exécute et suit les conteneurs associés aux tâches de l'Orchestrateur."""

    def __init__(
        self,
        config: SandboxConfig | None = None,
        *,
        runner: CliRunner | None = None,
    ) -> None:
        self.config = config or SandboxConfig()
        self._runner = runner or AsyncioCliRunner()
        self._uses_default_runner = runner is None
        self._containers: dict[str, set[str]] = {}
        self._container_names: dict[str, str] = {}
        self._expiry_tasks: dict[str, asyncio.Task[None]] = {}
        self._stopping_tasks: set[str] = set()
        self._stop_operations: dict[
            str, asyncio.Task[SandboxStopResult]
        ] = {}
        self._stop_all_operation: asyncio.Task[SandboxStopResult] | None = None
        self._stopping_all = False
        self._container_stop_operations: dict[
            str, asyncio.Task[str | None]
        ] = {}
        self._creating: dict[str, set[asyncio.Future[None]]] = {}
        self._state_lock = asyncio.Lock()
        self._blocked_reason: str | None = None

    @property
    def blocked_reason(self) -> str | None:
        return self._blocked_reason

    def block_execution(self, reason: str) -> None:
        self._blocked_reason = (reason or "Nettoyage Docker incomplet.")[:1000]

    def unblock_execution(self) -> None:
        self._blocked_reason = None

    def _require_execution_allowed(self) -> None:
        if self._blocked_reason:
            raise SandboxUnavailableError(self._blocked_reason)

    async def probe(self) -> SandboxStatus:
        """Vérifie Docker, le moteur Linux et l'image sans modifier le système."""

        if self._blocked_reason:
            return SandboxStatus(False, self._blocked_reason, None, False)

        engine = await self._probe_engine()
        if not engine.available:
            return engine
        try:
            image = await self._docker(
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                self.config.image,
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError) as exc:
            return SandboxStatus(
                False,
                f"L'image Docker approuvée ne peut pas être vérifiée : {exc}",
                engine.docker_version,
                False,
            )
        if image.returncode != 0 or not image.stdout.strip():
            return SandboxStatus(
                False,
                "L'image Docker approuvée n'est pas installée. "
                f"Un administrateur doit préparer {self.config.image!r} ; "
                "l'agent n'est pas autorisé à la télécharger.",
                engine.docker_version,
                False,
            )
        return SandboxStatus(
            True,
            "L'exécution isolée Docker est prête.",
            engine.docker_version,
            True,
        )

    async def ensure_available(self) -> SandboxStatus:
        status = await self.probe()
        if not status.available:
            raise SandboxUnavailableError(status.message)
        return status

    async def run_command(
        self,
        task_id: int | str,
        workspace: str | os.PathLike[str],
        command: Sequence[str],
        *,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
        network_enabled: bool = False,
    ) -> SandboxResult:
        """Exécute une commande isolée puis détruit son conteneur.

        Un dépassement de délai est un résultat explicite (code 124). Les
        erreurs de frontière ou de moteur lèvent une ``SandboxError``.
        """

        self._require_execution_allowed()
        task = self._validate_task_id(task_id)
        folder = self._validate_workspace(workspace)
        argv = self._validate_command(command)
        clean_env = self._validate_env(env)
        limit = self._validate_timeout(timeout)
        started = time.monotonic()
        container_id = await self._create_container(
            task,
            folder,
            argv,
            clean_env,
            kind="command",
            network_enabled=network_enabled,
            published_ports=(),
        )

        try:
            execution = await self._docker(
                "start", "--attach", container_id, timeout=limit
            )
        except (CliTimeoutError, asyncio.TimeoutError) as exc:
            output = self._timeout_output(exc, limit)
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=True
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "La commande a dépassé son délai et son conteneur n'a pas pu "
                    f"être supprimé : {cleanup_error}"
                ) from exc
            return SandboxResult(
                task,
                124,
                output,
                True,
                round(time.monotonic() - started, 3),
            )
        except BaseException as exc:
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=True
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Le démarrage a échoué et le conteneur n'a pas pu être "
                    f"supprimé : {cleanup_error}"
                ) from exc
            raise

        try:
            inspected = await self._docker(
                "inspect",
                "--format",
                "{{json .State}}",
                container_id,
                timeout=self.config.lifecycle_timeout,
            )
            if inspected.returncode != 0:
                details = self._combined_output(inspected) or self._combined_output(
                    execution
                )
                raise SandboxExecutionError(
                    "Le résultat du conteneur ne peut pas être vérifié. " + details
                )
            try:
                state = json.loads(inspected.stdout)
                status = str(state["Status"]).strip().lower()
                started_at = str(state["StartedAt"]).strip()
                state_error = str(state.get("Error") or "").strip()
                exit_code = int(state["ExitCode"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise SandboxExecutionError(
                    "Docker a renvoyé un état d'exécution incompréhensible."
                ) from exc

            never_started = (
                not started_at
                or started_at.startswith("0001-01-01T00:00:00")
                or status != "exited"
            )
            if never_started or state_error:
                details = state_error or self._combined_output(execution)
                if not details:
                    details = f"état Docker {status or 'inconnu'}"
                raise SandboxExecutionError(
                    "Docker n'a pas démarré correctement le conteneur : " + details
                )
            output = self._bounded_output(self._combined_output(execution))
            result = SandboxResult(
                task,
                exit_code,
                output,
                False,
                round(time.monotonic() - started, 3),
            )
        except BaseException as exc:
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=True
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Le résultat n'a pas pu être vérifié et le conteneur n'a pas "
                    f"pu être supprimé : {cleanup_error}"
                ) from exc
            raise

        cleanup_error = await self._cleanup_container(
            container_id, stop_first=False
        )
        if cleanup_error:
            raise SandboxExecutionError(
                "La commande est terminée mais son conteneur n'a pas pu être "
                f"supprimé : {cleanup_error}"
            )
        return result

    async def run_tests(
        self,
        task_id: int | str,
        workspace: str | os.PathLike[str],
        command: Sequence[str] = ("python", "-m", "pytest", "-q"),
        *,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
        network_enabled: bool = False,
    ) -> SandboxResult:
        """Raccourci de test ; le réseau reste coupé sauf décision explicite."""

        return await self.run_command(
            task_id,
            workspace,
            command,
            timeout=timeout,
            env=env,
            network_enabled=network_enabled,
        )

    async def launch(
        self,
        task_id: int | str,
        workspace: str | os.PathLike[str],
        command: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        network_enabled: bool = False,
        published_ports: Mapping[int, int] | None = None,
        lifetime_seconds: float | None = None,
    ) -> SandboxHandle:
        """Démarre une application et renvoie un handle à arrêter.

        Les ports sont toujours liés à ``127.0.0.1``. Les publier active le
        réseau ``bridge`` et exige donc les deux accords : configuration
        administrateur ``allow_network`` et argument ``network_enabled``.
        """

        self._require_execution_allowed()
        task = self._validate_task_id(task_id)
        folder = self._validate_workspace(workspace)
        argv = self._validate_command(command)
        clean_env = self._validate_env(env)
        ports = self._validate_ports(published_ports, network_enabled)
        lifetime = self._validate_lifetime(lifetime_seconds)
        container_id = await self._create_container(
            task,
            folder,
            argv,
            clean_env,
            kind="launch",
            network_enabled=network_enabled,
            published_ports=ports,
        )
        try:
            started = await self._docker(
                "start", container_id, timeout=self.config.lifecycle_timeout
            )
            if started.returncode != 0:
                raise SandboxExecutionError(
                    "L'application isolée n'a pas pu démarrer : "
                    + self._combined_output(started)
                )
            state = await self._docker(
                "inspect",
                "--format",
                "{{.State.Running}}|{{.State.ExitCode}}|{{.State.Error}}",
                container_id,
                timeout=self.config.lifecycle_timeout,
            )
            if state.returncode != 0 or not state.stdout.strip().lower().startswith("true|"):
                logs = await self._docker(
                    "logs", "--tail", "100", container_id,
                    timeout=self.config.lifecycle_timeout,
                )
                details = self._combined_output(logs) or self._combined_output(state)
                raise SandboxExecutionError(
                    "L'application isolée s'est arrêtée immédiatement. " + details
                )

            async with self._state_lock:
                name = self._container_names.get(container_id, container_id[:12])
                expiry = asyncio.create_task(
                    self._expire_container(task, container_id, lifetime),
                    name=f"sandbox-expiry-{container_id[:12]}",
                )
                self._expiry_tasks[container_id] = expiry
            return SandboxHandle(task, container_id, name, ports, lifetime)
        except BaseException as exc:
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=True
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Le lancement a échoué et le conteneur n'a pas pu être "
                    f"supprimé : {cleanup_error}"
                ) from exc
            raise

    async def is_handle_running(self, handle: SandboxHandle) -> bool:
        """Confirme qu'un handle suivi designe encore un conteneur actif."""

        if not isinstance(handle, SandboxHandle):
            raise SandboxValidationError("Le handle de bac a sable est invalide.")
        task = self._validate_task_id(handle.task_id)
        container_id = str(handle.container_id).strip().lower()
        if not _CONTAINER_ID_RE.fullmatch(container_id):
            raise SandboxValidationError("L'identifiant du conteneur est invalide.")

        async with self._state_lock:
            owners = tuple(
                owner
                for owner, container_ids in self._containers.items()
                if container_id in container_ids
            )
            known_name = self._container_names.get(container_id)
        if not owners:
            return False
        if owners != (task,) or known_name not in (None, handle.container_name):
            raise SandboxValidationError(
                "Ce handle ne correspond pas au conteneur suivi pour cette tache."
            )

        state = await self._docker(
            "inspect",
            "--format",
            "{{.State.Running}}|{{.State.ExitCode}}|{{.State.Error}}",
            container_id,
            timeout=self.config.lifecycle_timeout,
        )
        return state.returncode == 0 and state.stdout.strip().lower().startswith(
            "true|"
        )

    async def stop(
        self, task_id: int | str, *, strict: bool = True
    ) -> SandboxStopResult:
        """Arrête tous les conteneurs connus, y compris après un redémarrage.

        Le balayage n'utilise que les labels de cette installation et de cette
        tâche. ``strict=True`` lève une erreur si un conteneur résiste.
        """

        task = self._validate_task_id(task_id)
        async with self._state_lock:
            operation = self._stop_operations.get(task)
            if operation is None:
                # La marque est posée avant de planifier l'opération afin qu'une
                # création concurrente ne puisse pas se glisser entre les deux.
                self._stopping_tasks.add(task)
                operation = asyncio.create_task(
                    self._perform_stop(task),
                    name=f"sandbox-stop-{task}",
                )
                self._stop_operations[task] = operation

        # L'annulation d'un appelant ne doit pas annuler l'arrêt partagé attendu
        # par les autres appelants, ni laisser le conteneur sans surveillance.
        result = await asyncio.shield(operation)
        if strict and result.failed:
            raise SandboxStopError(result)
        return result

    async def stop_handle(
        self, handle: SandboxHandle, *, strict: bool = True
    ) -> SandboxStopResult:
        """Arrête exactement le conteneur d'un handle encore suivi.

        Un handle déjà nettoyé produit un succès vide, ce qui rend l'appel
        idempotent. Un identifiant suivi par une autre tâche est refusé.
        """

        if not isinstance(handle, SandboxHandle):
            raise SandboxValidationError("Le handle de bac à sable est invalide.")
        task = self._validate_task_id(handle.task_id)
        container_id = str(handle.container_id).strip().lower()
        if not _CONTAINER_ID_RE.fullmatch(container_id):
            raise SandboxValidationError("L'identifiant du conteneur est invalide.")

        async with self._state_lock:
            owners = tuple(
                owner
                for owner, container_ids in self._containers.items()
                if container_id in container_ids
            )
            known_name = self._container_names.get(container_id)
        if not owners:
            return SandboxStopResult(task, (), ())
        if owners != (task,) or known_name not in (None, handle.container_name):
            raise SandboxValidationError(
                "Ce handle ne correspond pas au conteneur suivi pour cette tâche."
            )

        error = await self._stop_and_remove(container_id)
        result = SandboxStopResult(
            task,
            () if error else (container_id,),
            ((container_id, error),) if error else (),
        )
        if strict and error:
            raise SandboxStopError(result)
        return result

    async def _perform_stop(self, task: str) -> SandboxStopResult:
        """Réalise l'unique arrêt partagé d'une tâche."""

        stopped: list[str] = []
        failed: list[tuple[str, str]] = []
        try:
            async with self._state_lock:
                pending_creations = tuple(self._creating.get(task, set()))
            if pending_creations:
                await asyncio.gather(
                    *(asyncio.shield(marker) for marker in pending_creations)
                )
            engine = await self._probe_engine()
            if not engine.available:
                failed.append(("moteur Docker", engine.message))
            else:
                async with self._state_lock:
                    tracked = set(self._containers.get(task, set()))

                discovered, discovery_error = await self._discover_task_containers(task)
                tracked.update(discovered)
                if discovered:
                    async with self._state_lock:
                        self._containers.setdefault(task, set()).update(discovered)
                if discovery_error:
                    failed.append(("recherche", discovery_error))

                attempted: set[str] = set()
                for container_id in sorted(tracked):
                    attempted.add(container_id)
                    error = await self._stop_and_remove(container_id)
                    if error:
                        failed.append((container_id, error))
                    else:
                        stopped.append(container_id)

                # Une création déjà envoyée à Docker au moment de stop() peut
                # apparaître après le premier balayage. La marque _stopping_tasks
                # l'empêche de démarrer et ce second passage la récupère.
                late, late_error = await self._discover_task_containers(task)
                if late_error:
                    failed.append(("seconde recherche", late_error))
                if late:
                    async with self._state_lock:
                        self._containers.setdefault(task, set()).update(late)
                for container_id in sorted(set(late) - attempted):
                    error = await self._stop_and_remove(container_id)
                    if error:
                        failed.append((container_id, error))
                    else:
                        stopped.append(container_id)
        finally:
            async with self._state_lock:
                self._stopping_tasks.discard(task)
                current = self._stop_operations.get(task)
                if current is asyncio.current_task():
                    self._stop_operations.pop(task, None)

        result = SandboxStopResult(
            task, tuple(dict.fromkeys(stopped)), tuple(failed)
        )
        if failed:
            self.block_execution(
                "Le nettoyage Docker de la tâche "
                f"{task} est incomplet : "
                + "; ".join(f"{item}: {reason}" for item, reason in failed)
            )
        return result

    async def stop_all_managed(self, *, strict: bool = True) -> SandboxStopResult:
        """Nettoie tous les conteneurs de l'installation en une opération partagée."""
        async with self._state_lock:
            operation = self._stop_all_operation
            if operation is None:
                # La barrière précède tous les snapshots. Une création déjà
                # enregistrée sera attendue ; une nouvelle création sera
                # refusée jusqu'à la fin du second balayage Docker.
                self._stopping_all = True
                operation = asyncio.create_task(
                    self._perform_stop_all_managed(),
                    name="sandbox-stop-all",
                )
                self._stop_all_operation = operation

        result = await asyncio.shield(operation)
        if strict and result.failed:
            raise SandboxStopError(result)
        return result

    async def _perform_stop_all_managed(self) -> SandboxStopResult:
        try:
            return await self._stop_all_managed_inner()
        finally:
            async with self._state_lock:
                if self._stop_all_operation is asyncio.current_task():
                    self._stop_all_operation = None
                    self._stopping_all = False

    async def _stop_all_managed_inner(self) -> SandboxStopResult:
        async with self._state_lock:
            known_tasks = tuple(sorted(set(self._containers) | set(self._creating)))
        stopped: list[str] = []
        failed: list[tuple[str, str]] = []
        for task in known_tasks:
            try:
                partial = await self.stop(task, strict=False)
                stopped.extend(partial.stopped)
                failed.extend(partial.failed)
            except SandboxError as exc:
                failed.append((task, str(exc)))

        try:
            discovered = await self._docker(
                "ps", "--all", "--no-trunc", "--quiet",
                "--filter", f"label={_MANAGED_LABEL}=true",
                "--filter", f"label={_NAMESPACE_LABEL}={self.config.namespace}",
                timeout=self.config.lifecycle_timeout,
            )
            if discovered.returncode != 0:
                failed.append(("recherche", self._combined_output(discovered)))
                ids: tuple[str, ...] = ()
            else:
                valid_ids: list[str] = []
                invalid_ids: list[str] = []
                for line in discovered.stdout.splitlines():
                    candidate = line.strip()
                    if not candidate:
                        continue
                    if not _CONTAINER_ID_RE.fullmatch(candidate):
                        invalid_ids.append(candidate[:80])
                    else:
                        valid_ids.append(candidate)
                ids = tuple(valid_ids)
                if invalid_ids:
                    failed.append((
                        "recherche",
                        "Docker a renvoyé un identifiant de conteneur invalide.",
                    ))
        except (SandboxError, CliTimeoutError) as exc:
            failed.append(("recherche", str(exc)))
            ids = ()

        for container_id in ids:
            if container_id in stopped:
                continue
            error = await self._stop_and_remove(container_id)
            if error:
                failed.append((container_id, error))
            else:
                stopped.append(container_id)
        result = SandboxStopResult(
            "*", tuple(dict.fromkeys(stopped)), tuple(failed)
        )
        if failed:
            self.block_execution(
                "Le nettoyage global des conteneurs Docker est incomplet : "
                + "; ".join(f"{item}: {reason}" for item, reason in failed)
            )
        return result

    async def _probe_engine(self) -> SandboxStatus:
        if self._uses_default_runner and shutil.which(self.config.docker_bin) is None:
            return SandboxStatus(
                False,
                "Docker est introuvable. Installez Docker Desktop puis redémarrez "
                "l'Orchestrateur.",
            )
        try:
            version = await self._docker(
                "version",
                "--format",
                "{{.Server.Version}}",
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError) as exc:
            return SandboxStatus(False, f"Le moteur Docker ne répond pas : {exc}")
        version_text = version.stdout.strip()
        if version.returncode != 0 or not version_text:
            return SandboxStatus(
                False,
                "Le moteur Docker n'est pas démarré ou n'est pas accessible : "
                + self._combined_output(version),
            )
        parsed = self._parse_version(version_text)
        if parsed is None or parsed < _DOCKER_MINIMUM_VERSION:
            return SandboxStatus(
                False,
                "Docker 23.0 ou plus récent est requis pour appliquer toutes les "
                "protections du bac à sable.",
                version_text,
            )
        try:
            os_type = await self._docker(
                "info",
                "--format",
                "{{.OSType}}",
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError) as exc:
            return SandboxStatus(
                False,
                f"Le type de conteneur Docker ne peut pas être vérifié : {exc}",
                version_text,
            )
        if os_type.returncode != 0 or os_type.stdout.strip().lower() != "linux":
            return SandboxStatus(
                False,
                "Docker doit utiliser les conteneurs Linux pour garantir l'utilisateur "
                "non-root et la suppression des capacités.",
                version_text,
            )
        try:
            warnings_result = await self._docker(
                "info",
                "--format",
                "{{json .Warnings}}",
                timeout=self.config.lifecycle_timeout,
            )
            warnings = json.loads(warnings_result.stdout or "null")
        except (SandboxError, CliTimeoutError, json.JSONDecodeError) as exc:
            return SandboxStatus(
                False,
                f"Les limites de ressources Docker ne peuvent pas être vérifiées : {exc}",
                version_text,
            )
        if warnings_result.returncode != 0 or warnings is not None and not isinstance(warnings, list):
            return SandboxStatus(
                False,
                "Les limites de ressources Docker ne peuvent pas être vérifiées.",
                version_text,
            )
        unsafe_warnings = (
            "no memory limit support",
            "no swap limit support",
            "no cpu cfs quota support",
            "no cpu cfs period support",
            "no pids limit support",
        )
        reported = "\n".join(str(item).lower() for item in (warnings or ()))
        if any(warning in reported for warning in unsafe_warnings):
            return SandboxStatus(
                False,
                "Le moteur Docker annonce qu'une limite CPU, mémoire, swap ou "
                "processus n'est pas disponible.",
                version_text,
            )
        return SandboxStatus(True, "Le moteur Docker est prêt.", version_text)

    async def _docker(self, *args: str, timeout: float) -> CliResult:
        return await self._runner.run(
            (self.config.docker_bin, *args), timeout=timeout
        )

    async def _create_container(
        self,
        task: str,
        workspace: Path,
        command: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        *,
        kind: str,
        network_enabled: bool,
        published_ports: tuple[tuple[int, int], ...],
    ) -> str:
        marker = await self._begin_creation(task)
        try:
            return await self._create_container_inner(
                task,
                workspace,
                command,
                env,
                kind=kind,
                network_enabled=network_enabled,
                published_ports=published_ports,
            )
        finally:
            await self._end_creation(task, marker)

    async def _protected_best_effort_remove(
        self, identifier: str, *, task: str
    ) -> str | None:
        """Attend le nettoyage incertain avant de liberer la barriere de creation."""

        cleanup = asyncio.create_task(
            self._best_effort_remove(identifier),
            name=f"sandbox-uncertain-create-{task}",
        )
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                error = await asyncio.shield(cleanup)
                break
            except asyncio.CancelledError as exc:
                # Le conteneur peut deja exister sans identifiant connu. Ne pas
                # rendre la main a stop_all tant que son absence n'est pas
                # confirmee, meme si le client annule une seconde fois.
                if cleanup.done():
                    raise
                cancellation = cancellation or exc
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
        if cancellation is not None:
            raise cancellation
        return error

    async def _create_container_inner(
        self,
        task: str,
        workspace: Path,
        command: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        *,
        kind: str,
        network_enabled: bool,
        published_ports: tuple[tuple[int, int], ...],
    ) -> str:
        await self.ensure_available()
        if network_enabled and not self.config.allow_network:
            raise SandboxValidationError(
                "Le réseau du bac à sable n'a pas été autorisé par l'administrateur."
            )
        name = self._container_name(task, kind)
        workspace_hash = hashlib.sha256(
            os.path.normcase(str(workspace)).encode("utf-8")
        ).hexdigest()[:16]
        mount = f"type=bind,source={workspace},target=/workspace"
        create = [
            "create",
            "--pull=never",
            "--name",
            name,
            "--label",
            f"{_MANAGED_LABEL}=true",
            "--label",
            f"{_NAMESPACE_LABEL}={self.config.namespace}",
            "--label",
            f"{_TASK_LABEL}={task}",
            "--label",
            f"{_WORKSPACE_LABEL}={workspace_hash}",
            "--label",
            f"{_KIND_LABEL}={kind}",
            "--network",
            "bridge" if network_enabled else "none",
            "--user",
            self.config.user,
            "--workdir",
            "/workspace",
            "--read-only",
            "--mount",
            mount,
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,nodev,size=64m",
            "--tmpfs",
            "/home/sandbox:rw,noexec,nosuid,nodev,size=32m",
            "--env",
            "HOME=/home/sandbox",
            "--env",
            "TMPDIR=/tmp",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--security-opt",
            "seccomp=builtin",
            "--pids-limit",
            str(self.config.pids_limit),
            "--cpus",
            str(self.config.cpus),
            "--memory",
            self.config.memory,
            "--memory-swap",
            self.config.memory,
            "--memory-swappiness",
            "0",
            "--ulimit",
            "nofile=1024:1024",
            "--init",
            "--ipc",
            "none",
            "--cgroupns",
            "private",
            "--no-healthcheck",
            "--log-driver",
            "local",
            "--log-opt",
            "max-size=1m",
            "--log-opt",
            "max-file=1",
            "--log-opt",
            "compress=false",
            "--restart",
            "no",
            "--stop-timeout",
            str(self.config.stop_timeout),
        ]
        for key, value in env:
            create.extend(("--env", f"{key}={value}"))
        for host_port, container_port in published_ports:
            create.extend(
                ("--publish", f"127.0.0.1:{host_port}:{container_port}/tcp")
            )
        # Neutralise l'ENTRYPOINT de l'image : le premier argument approuvé est
        # toujours l'exécutable réellement lancé.
        create.extend(("--entrypoint", command[0], self.config.image))
        create.extend(command[1:])

        try:
            result = await self._docker(
                *create, timeout=self.config.lifecycle_timeout
            )
        except BaseException as exc:
            # Le daemon peut avoir créé le conteneur alors que le client Docker
            # est annulé avant de rendre son identifiant. Le nom, généré avant
            # l'appel, reste alors notre capacité de récupération. Le nettoyage
            # est protégé de l'annulation et confirme l'absence avant d'accepter
            # comme idempotent un `rm` qui a échoué.
            cleanup_error = await self._protected_best_effort_remove(
                name, task=task
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "La création Docker a été interrompue et l'absence de son "
                    f"conteneur n'a pas pu être confirmée : {cleanup_error}"
                ) from exc
            if isinstance(exc, (CliTimeoutError, asyncio.TimeoutError)):
                raise SandboxExecutionError(
                    "Docker a dépassé le délai pendant la création du bac à sable."
                ) from exc
            raise
        if result.returncode != 0:
            cleanup_error = await self._protected_best_effort_remove(
                name, task=task
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Docker a refuse la creation et l'absence du conteneur "
                    f"n'a pas pu etre confirmee : {cleanup_error}"
                )
            raise SandboxExecutionError(
                "Docker a refusé la création du bac à sable : "
                + self._combined_output(result)
            )
        container_id = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
        if not _CONTAINER_ID_RE.fullmatch(container_id):
            cleanup_error = await self._protected_best_effort_remove(
                name, task=task
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Docker a renvoye un identifiant invalide et l'absence du "
                    f"conteneur n'a pas pu etre confirmee : {cleanup_error}"
                )
            raise SandboxExecutionError(
                "Docker n'a pas renvoyé un identifiant de conteneur vérifiable."
            )

        # Dès que Docker fournit un identifiant fiable, le conteneur reste suivi
        # jusqu'à ce qu'un ``rm`` réussi confirme sa disparition. Cela couvre
        # aussi un refus de politique ou un arrêt demandé pendant l'inspection.
        async with self._state_lock:
            self._containers.setdefault(task, set()).add(container_id)
            self._container_names[container_id] = name
        try:
            await self._verify_container_policy(
                container_id,
                task,
                workspace,
                command,
                kind=kind,
                network_enabled=network_enabled,
                published_ports=published_ports,
                workspace_hash=workspace_hash,
            )
        except BaseException as exc:
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=False
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "Les protections Docker ne peuvent pas être vérifiées et le "
                    f"conteneur n'a pas pu être supprimé : {cleanup_error}"
                ) from exc
            raise

        async with self._state_lock:
            stopped_during_creation = task in self._stopping_tasks
        if stopped_during_creation:
            cleanup_error = await self._cleanup_container(
                container_id, stop_first=False
            )
            if cleanup_error:
                raise SandboxExecutionError(
                    "La tâche a été arrêtée pendant la création, mais son "
                    f"conteneur n'a pas pu être supprimé : {cleanup_error}"
                )
            raise SandboxStoppedError(
                "La tâche a été arrêtée pendant la création de son bac à sable."
            )
        return container_id

    async def _verify_container_policy(
        self,
        container_id: str,
        task: str,
        workspace: Path,
        command: tuple[str, ...],
        *,
        kind: str,
        network_enabled: bool,
        published_ports: tuple[tuple[int, int], ...],
        workspace_hash: str,
    ) -> None:
        """Relit la configuration réellement acceptée par le démon.

        Cette vérification évite qu'un moteur ancien ou configuré de manière
        inhabituelle ignore silencieusement une limite importante.
        """

        try:
            inspected = await self._docker(
                "inspect",
                "--format",
                "{{json .}}",
                container_id,
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError) as exc:
            raise SandboxExecutionError(
                f"Les protections du conteneur ne peuvent pas être vérifiées : {exc}"
            ) from exc
        if inspected.returncode != 0:
            raise SandboxExecutionError(
                "Les protections du conteneur ne peuvent pas être vérifiées : "
                + self._combined_output(inspected)
            )
        try:
            data = json.loads(inspected.stdout)
            host = data["HostConfig"]
            container = data["Config"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise SandboxExecutionError(
                "Docker a renvoyé une configuration de conteneur incompréhensible."
            ) from exc

        errors: list[str] = []
        expected_network = "bridge" if network_enabled else "none"
        if container.get("User") != self.config.user:
            errors.append("utilisateur non-root non confirmé")
        if host.get("NetworkMode") != expected_network:
            errors.append("mode réseau inattendu")
        if host.get("ReadonlyRootfs") is not True:
            errors.append("racine en lecture seule absente")
        if host.get("Privileged") is not False:
            errors.append("mode privilégié détecté")
        if host.get("Init") is not True:
            errors.append("processus init absent")
        if str(host.get("IpcMode", "")).lower() != "none":
            errors.append("espace IPC non isolé")
        if str(host.get("CgroupnsMode", "")).lower() != "private":
            errors.append("espace cgroup non privé")
        if str(host.get("PidMode", "")).lower() not in ("", "private"):
            errors.append("espace de processus partagé")
        if str(host.get("UTSMode", "")).lower() == "host":
            errors.append("espace UTS de l'hôte partagé")

        cap_drop = {str(value).upper() for value in (host.get("CapDrop") or ())}
        if "ALL" not in cap_drop or host.get("CapAdd"):
            errors.append("capacités Linux non supprimées")
        security = {
            str(value).lower() for value in (host.get("SecurityOpt") or ())
        }
        if not any(value.startswith("no-new-privileges") for value in security):
            errors.append("no-new-privileges absent")
        if "seccomp=builtin" not in security:
            errors.append("profil seccomp intégré absent")

        expected_memory = _memory_bytes(self.config.memory)
        if host.get("Memory") != expected_memory:
            errors.append("limite mémoire non confirmée")
        if host.get("MemorySwap") != expected_memory:
            errors.append("limite mémoire et swap non confirmée")
        # Sous cgroup v2 (Docker Desktop/WSL2), Docker confirme
        # ``--memory-swap == --memory`` mais sérialise swappiness à null. Cette
        # combinaison interdit tout swap supplémentaire et équivaut donc au 0
        # explicite rapporté par les moteurs cgroup v1.
        if host.get("MemorySwappiness") not in (None, 0):
            errors.append("swap anonyme non désactivé")
        if host.get("NanoCpus") != int(self.config.cpus * 1_000_000_000):
            errors.append("limite CPU non confirmée")
        if host.get("PidsLimit") != self.config.pids_limit:
            errors.append("limite de processus non confirmée")

        if (host.get("RestartPolicy") or {}).get("Name") not in ("", "no"):
            errors.append("redémarrage automatique actif")
        if host.get("AutoRemove") is not False:
            errors.append("suppression automatique Docker inattendue")
        if host.get("Devices") or host.get("DeviceRequests") or host.get("DeviceCgroupRules"):
            errors.append("périphérique hôte exposé")
        if host.get("Binds"):
            errors.append("montage Docker supplémentaire détecté")

        mounts = host.get("Mounts") or ()
        if len(mounts) != 1:
            errors.append("nombre de dossiers montés inattendu")
        else:
            mount = mounts[0]
            target = mount.get("Target", mount.get("Destination"))
            if mount.get("Type") != "bind" or target != "/workspace":
                errors.append("montage du projet invalide")
            if not self._mount_source_matches(mount.get("Source"), workspace):
                errors.append("source du projet monté inattendue")
            actual_mounts = data.get("Mounts")
            if actual_mounts:
                matching = [
                    item
                    for item in actual_mounts
                    if isinstance(item, dict)
                    and item.get("Type") == "bind"
                    and item.get("Destination", item.get("Target")) == "/workspace"
                    and self._mount_source_matches(item.get("Source"), workspace)
                ]
                if (
                    len(actual_mounts) != 1
                    or len(matching) != 1
                    or matching[0].get("RW") is not True
                ):
                    errors.append("projet non monté en lecture/écriture")
            elif mount.get("ReadOnly") is not False:
                errors.append("projet non monté en lecture/écriture")

        tmpfs = host.get("Tmpfs") or {}
        if set(tmpfs) != {"/tmp", "/home/sandbox"}:
            errors.append("espaces temporaires inattendus")
        else:
            for target, options in tmpfs.items():
                option_set = {part.lower() for part in str(options).split(",")}
                if not {"rw", "noexec", "nosuid", "nodev"}.issubset(option_set):
                    errors.append(f"protections temporaires absentes pour {target}")

        ulimits = {
            item.get("Name"): (item.get("Soft"), item.get("Hard"))
            for item in (host.get("Ulimits") or ())
            if isinstance(item, dict)
        }
        if ulimits.get("nofile") != (1024, 1024):
            errors.append("limite de fichiers ouverts absente")

        log_config = host.get("LogConfig") or {}
        log_options = log_config.get("Config") or {}
        if log_config.get("Type") != "local":
            errors.append("journal borné non activé")
        if (
            log_options.get("max-size") != "1m"
            or log_options.get("max-file") != "1"
            or log_options.get("compress") != "false"
        ):
            errors.append("taille du journal non bornée")

        actual_bindings = host.get("PortBindings") or {}
        expected_bindings = {
            f"{container_port}/tcp": [
                {"HostIp": "127.0.0.1", "HostPort": str(host_port)}
            ]
            for host_port, container_port in published_ports
        }
        if actual_bindings != expected_bindings:
            errors.append("publication de ports inattendue")

        labels = container.get("Labels") or {}
        expected_labels = {
            _MANAGED_LABEL: "true",
            _NAMESPACE_LABEL: self.config.namespace,
            _TASK_LABEL: task,
            _WORKSPACE_LABEL: workspace_hash,
            _KIND_LABEL: kind,
        }
        if any(labels.get(key) != value for key, value in expected_labels.items()):
            errors.append("labels de récupération incomplets")
        if tuple(container.get("Entrypoint") or ()) != (command[0],):
            errors.append("exécutable réel inattendu")
        if tuple(container.get("Cmd") or ()) != command[1:]:
            errors.append("arguments réels inattendus")
        health_test = (container.get("Healthcheck") or {}).get("Test")
        if health_test not in (["NONE"], ("NONE",)):
            errors.append("healthcheck de l'image encore actif")

        if errors:
            raise SandboxExecutionError(
                "Docker n'a pas confirmé toutes les protections demandées : "
                + "; ".join(errors)
            )

    @staticmethod
    def _mount_source_matches(source: object, workspace: Path) -> bool:
        if not isinstance(source, str) or not source:
            return False
        expected = os.path.normcase(os.path.normpath(str(workspace)))
        candidate = source
        if os.name == "nt":
            normalized = source.replace("\\", "/")
            for prefix in ("/host_mnt/", "/run/desktop/mnt/host/"):
                if normalized.lower().startswith(prefix) and len(normalized) > len(prefix) + 1:
                    remainder = normalized[len(prefix) :]
                    if remainder[1:2] == "/":
                        candidate = remainder[0] + ":/" + remainder[2:]
                        break
        actual = os.path.normcase(os.path.normpath(candidate))
        return actual == expected

    async def _begin_creation(self, task: str) -> asyncio.Future[None]:
        marker: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        async with self._state_lock:
            if self._stopping_all:
                raise SandboxStoppedError(
                    "Le nettoyage global des conteneurs est en cours ; aucun "
                    "nouveau conteneur ne peut être créé."
                )
            if task in self._stopping_tasks:
                raise SandboxStoppedError(
                    "La tâche est en cours d'arrêt ; aucun nouveau conteneur ne "
                    "peut être créé."
                )
            self._creating.setdefault(task, set()).add(marker)
        return marker

    async def _end_creation(
        self, task: str, marker: asyncio.Future[None]
    ) -> None:
        async with self._state_lock:
            markers = self._creating.get(task)
            if markers is not None:
                markers.discard(marker)
                if not markers:
                    self._creating.pop(task, None)
            if not marker.done():
                marker.set_result(None)

    async def _discover_task_containers(
        self, task: str
    ) -> tuple[tuple[str, ...], str | None]:
        try:
            result = await self._docker(
                "ps",
                "--all",
                "--no-trunc",
                "--quiet",
                "--filter",
                f"label={_MANAGED_LABEL}=true",
                "--filter",
                f"label={_NAMESPACE_LABEL}={self.config.namespace}",
                "--filter",
                f"label={_TASK_LABEL}={task}",
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError) as exc:
            return (), str(exc)
        if result.returncode != 0:
            return (), self._combined_output(result)
        ids: list[str] = []
        for line in result.stdout.splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            if not _CONTAINER_ID_RE.fullmatch(candidate):
                return (), "Docker a renvoyé un identifiant de conteneur invalide."
            ids.append(candidate)
        return tuple(ids), None

    async def _confirm_container_absent(
        self, identifier: str
    ) -> tuple[bool, str | None]:
        """Confirme via une nouvelle lecture Docker qu'un conteneur a disparu.

        Un ``docker rm`` peut perdre sa réponse après que le daemon a bien
        supprimé le conteneur. L'erreur du client n'est donc ni un succès ni
        un échec suffisant : seule une liste relue avec succès permet de rendre
        le nettoyage idempotent sans ouvrir la frontière en cas de panne du
        daemon.
        """

        if _CONTAINER_ID_RE.fullmatch(identifier):
            filter_value = f"id={identifier}"
        else:
            # Le filtre Docker par nom accepte une sous-chaîne. Le suffixe
            # aléatoire de nos noms rend une collision irréaliste et un faux
            # positif resterait sûr (nettoyage refusé), tandis que des ancres
            # échappées différemment selon les versions du moteur pourraient
            # produire un faux vide dangereux.
            filter_value = f"name={identifier}"
        try:
            result = await self._docker(
                "ps",
                "--all",
                "--no-trunc",
                "--quiet",
                "--filter",
                filter_value,
                timeout=self.config.lifecycle_timeout,
            )
        except (SandboxError, CliTimeoutError, asyncio.TimeoutError) as exc:
            return False, f"absence non vérifiable : {exc}"
        if result.returncode != 0:
            details = self._combined_output(result) or "liste Docker refusée"
            return False, f"absence non vérifiable : {details}"

        present: list[str] = []
        for line in result.stdout.splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            if not _CONTAINER_ID_RE.fullmatch(candidate):
                return False, "Docker a renvoyé un identifiant de conteneur invalide."
            present.append(candidate)
        if present:
            return False, "Docker confirme que le conteneur existe encore."
        return True, None

    async def _stop_and_remove(self, container_id: str) -> str | None:
        async with self._state_lock:
            operation = self._container_stop_operations.get(container_id)
            if operation is None:
                operation = asyncio.create_task(
                    self._perform_stop_and_remove(container_id),
                    name=f"sandbox-container-stop-{container_id[:12]}",
                )
                self._container_stop_operations[container_id] = operation
        return await asyncio.shield(operation)

    async def _perform_stop_and_remove(self, container_id: str) -> str | None:
        messages: list[str] = []
        needs_kill = False
        try:
            try:
                stopped = await self._docker(
                    "stop",
                    "--time",
                    str(self.config.stop_timeout),
                    container_id,
                    timeout=self.config.stop_timeout
                    + self.config.lifecycle_timeout,
                )
                if stopped.returncode != 0:
                    needs_kill = True
                    messages.append(
                        self._combined_output(stopped) or "arrêt gracieux refusé"
                    )
            except (SandboxError, CliTimeoutError, asyncio.TimeoutError) as exc:
                needs_kill = True
                messages.append(str(exc))

            if needs_kill:
                try:
                    killed = await self._docker(
                        "kill", container_id, timeout=self.config.lifecycle_timeout
                    )
                    if killed.returncode != 0:
                        messages.append(
                            self._combined_output(killed) or "arrêt forcé refusé"
                        )
                except (SandboxError, CliTimeoutError, asyncio.TimeoutError) as exc:
                    messages.append(str(exc))

            removal_error: str | None = None
            try:
                removed = await self._docker(
                    "rm",
                    "--force",
                    container_id,
                    timeout=self.config.lifecycle_timeout,
                )
            except (SandboxError, CliTimeoutError, asyncio.TimeoutError) as exc:
                removal_error = str(exc)
            else:
                if removed.returncode != 0:
                    removal_error = (
                        self._combined_output(removed)
                        or "suppression Docker refusée"
                    )
            if removal_error:
                absent, confirmation_error = await self._confirm_container_absent(
                    container_id
                )
                if absent:
                    await self._unregister(container_id)
                    return None
                messages.append(removal_error)
                if confirmation_error:
                    messages.append(confirmation_error)
                error = "; ".join(message for message in messages if message)
                self.block_execution(
                    f"Le conteneur Docker {container_id} n'a pas pu être supprimé : {error}"
                )
                return error
            await self._unregister(container_id)
            return None
        finally:
            async with self._state_lock:
                current = self._container_stop_operations.get(container_id)
                if current is asyncio.current_task():
                    self._container_stop_operations.pop(container_id, None)

    async def _cleanup_container(
        self, container_id: str, *, stop_first: bool
    ) -> str | None:
        # Même après une fin normale, le nettoyage peut courir en parallèle
        # avec stop(task) ou stop_handle(). Tous les chemins doivent partager
        # l'opération par conteneur, sinon le second `docker rm` voit un faux
        # échec « No such container » et bloque globalement le runtime.
        return await self._stop_and_remove(container_id)

    async def _best_effort_remove(self, identifier: str) -> str | None:
        removal_error: str | None = None
        try:
            removed = await self._docker(
                "rm", "--force", identifier, timeout=self.config.lifecycle_timeout
            )
            if removed.returncode != 0:
                removal_error = (
                    self._combined_output(removed) or "suppression Docker refusée"
                )
        except (SandboxError, CliTimeoutError, asyncio.TimeoutError) as exc:
            removal_error = str(exc)
        if removal_error:
            absent, confirmation_error = await self._confirm_container_absent(
                identifier
            )
            if absent:
                await self._unregister(identifier)
                return None
            error = removal_error
            if confirmation_error:
                error += "; " + confirmation_error
            self.block_execution(
                f"Le conteneur Docker {identifier} n'a pas pu être supprimé : {error}"
            )
            return error
        return None

    async def _unregister(self, container_id: str) -> None:
        async with self._state_lock:
            for task, container_ids in list(self._containers.items()):
                container_ids.discard(container_id)
                if not container_ids:
                    self._containers.pop(task, None)
            self._container_names.pop(container_id, None)
            expiry = self._expiry_tasks.pop(container_id, None)
            if expiry is not None and expiry is not asyncio.current_task():
                expiry.cancel()

    async def _expire_container(
        self, task: str, container_id: str, lifetime: float
    ) -> None:
        try:
            await asyncio.sleep(lifetime)
            error = await self._stop_and_remove(container_id)
            if error:
                self.block_execution(
                    "L'expiration du conteneur Docker "
                    f"{container_id} a échoué : {error}"
                )
                log.error(
                    "Le conteneur %s de la tâche %s a résisté à son expiration : %s",
                    container_id,
                    task,
                    error,
                )
        except asyncio.CancelledError:
            return

    def _validate_workspace(self, workspace: str | os.PathLike[str]) -> Path:
        try:
            folder = Path(workspace).expanduser().resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise SandboxValidationError(
                "Le dossier de travail n'existe pas ou ne peut pas être résolu."
            ) from exc
        if not folder.is_dir():
            raise SandboxValidationError("Le dossier de travail n'est pas un dossier.")
        text = str(folder)
        if any(character in text for character in (",", "\n", "\r", "\x00")):
            raise SandboxValidationError(
                "Le chemin du projet contient un caractère incompatible avec un montage sûr."
            )
        for root in self.config.allowed_workspace_roots:
            try:
                relative = folder.relative_to(root)
            except ValueError:
                continue
            if relative != Path("."):
                return folder
        raise SandboxValidationError(
            "Le dossier demandé n'appartient à aucune racine de projets autorisée."
        )

    @staticmethod
    def _validate_task_id(task_id: int | str) -> str:
        if isinstance(task_id, bool):
            raise SandboxValidationError("L'identifiant de tâche est invalide.")
        task = str(task_id)
        if not _TASK_ID_RE.fullmatch(task):
            raise SandboxValidationError("L'identifiant de tâche est invalide.")
        return task

    @staticmethod
    def _validate_command(command: Sequence[str]) -> tuple[str, ...]:
        if isinstance(command, (str, bytes)):
            raise SandboxValidationError(
                "La commande doit être une liste d'arguments, pas une ligne de shell."
            )
        argv = tuple(command)
        if not argv or len(argv) > 128:
            raise SandboxValidationError("La commande doit contenir entre 1 et 128 arguments.")
        total = 0
        for argument in argv:
            if not isinstance(argument, str) or not argument:
                raise SandboxValidationError(
                    "Chaque argument de commande doit être un texte non vide."
                )
            if any(character in argument for character in ("\x00", "\n", "\r")):
                raise SandboxValidationError(
                    "Un argument de commande contient un caractère de contrôle interdit."
                )
            if len(argument) > 4096:
                raise SandboxValidationError("Un argument de commande est trop long.")
            total += len(argument)
        if total > 32_768:
            raise SandboxValidationError("La commande est trop longue.")
        if argv[0].startswith("-"):
            raise SandboxValidationError("Le premier argument doit être un exécutable.")
        return argv

    def _validate_env(
        self, env: Mapping[str, str] | None
    ) -> tuple[tuple[str, str], ...]:
        if env is None:
            return ()
        if not isinstance(env, Mapping) or len(env) > 64:
            raise SandboxValidationError("L'environnement demandé est invalide.")
        allowed = set(self.config.allowed_env_keys)
        clean: list[tuple[str, str]] = []
        for key, value in env.items():
            if not isinstance(key, str) or not _ENV_KEY_RE.fullmatch(key):
                raise SandboxValidationError(
                    "Un nom de variable d'environnement est invalide."
                )
            if key not in allowed or key in {"HOME", "TMPDIR"}:
                raise SandboxValidationError(
                    f"La variable {key!r} n'est pas autorisée dans le bac à sable."
                )
            if not isinstance(value, str) or len(value) > 4096:
                raise SandboxValidationError(
                    f"La valeur de {key!r} est invalide ou trop longue."
                )
            if any(character in value for character in ("\x00", "\n", "\r")):
                raise SandboxValidationError(
                    f"La valeur de {key!r} contient un caractère interdit."
                )
            clean.append((key, value))
        return tuple(sorted(clean))

    def _validate_timeout(self, timeout: float | None) -> float:
        value = self.config.default_timeout if timeout is None else float(timeout)
        if value <= 0 or value > self.config.default_timeout:
            raise SandboxValidationError(
                "Le délai demandé doit être positif et ne peut pas dépasser la limite administrateur."
            )
        return value

    def _validate_lifetime(self, lifetime: float | None) -> float:
        value = self.config.max_launch_seconds if lifetime is None else float(lifetime)
        if value <= 0 or value > self.config.max_launch_seconds:
            raise SandboxValidationError(
                "La durée demandée dépasse la limite autorisée pour une application."
            )
        return value

    def _validate_ports(
        self,
        ports: Mapping[int, int] | None,
        network_enabled: bool,
    ) -> tuple[tuple[int, int], ...]:
        if ports is None:
            return ()
        if not isinstance(ports, Mapping) or len(ports) > 16:
            raise SandboxValidationError("La liste des ports est invalide.")
        clean: list[tuple[int, int]] = []
        for host_port, container_port in ports.items():
            if isinstance(host_port, bool) or isinstance(container_port, bool):
                raise SandboxValidationError("Un port est invalide.")
            try:
                host = int(host_port)
                container = int(container_port)
            except (TypeError, ValueError) as exc:
                raise SandboxValidationError("Un port est invalide.") from exc
            if not (1 <= host <= 65535 and 1 <= container <= 65535):
                raise SandboxValidationError("Les ports doivent être compris entre 1 et 65535.")
            clean.append((host, container))
        if clean and not network_enabled:
            raise SandboxValidationError(
                "Publier un port exige une autorisation réseau explicite."
            )
        return tuple(sorted(clean))

    def _container_name(self, task: str, kind: str) -> str:
        safe_task = re.sub(r"[^a-z0-9_.-]", "-", task.lower())[:20]
        return (
            f"orchestrator-{self.config.namespace}-{safe_task}-{kind}-"
            f"{uuid.uuid4().hex[:10]}"
        )[:120]

    def _combined_output(self, result: CliResult) -> str:
        pieces = [part.strip() for part in (result.stdout, result.stderr) if part.strip()]
        return self._bounded_output("\n".join(pieces))

    def _bounded_output(self, output: str) -> str:
        if len(output) <= self.config.max_output_chars:
            return output
        omitted = len(output) - self.config.max_output_chars
        return output[: self.config.max_output_chars] + f"\n… {omitted} caractères ignorés."

    def _timeout_output(self, exc: BaseException, timeout: float) -> str:
        stdout = getattr(exc, "stdout", "") or ""
        stderr = getattr(exc, "stderr", "") or ""
        details = "\n".join(part.strip() for part in (stdout, stderr) if part.strip())
        message = f"Commande interrompue après {timeout:g} secondes."
        return self._bounded_output(message + (("\n" + details) if details else ""))

    @staticmethod
    def _parse_version(value: str) -> tuple[int, int] | None:
        match = re.match(r"^([0-9]+)\.([0-9]+)", value)
        if not match:
            return None
        return int(match.group(1)), int(match.group(2))


__all__ = [
    "AsyncioCliRunner",
    "CliResult",
    "CliRunner",
    "CliTimeoutError",
    "DockerSandboxRuntime",
    "SandboxConfig",
    "SandboxError",
    "SandboxExecutionError",
    "SandboxHandle",
    "SandboxResult",
    "SandboxStatus",
    "SandboxStopError",
    "SandboxStopResult",
    "SandboxStoppedError",
    "SandboxUnavailableError",
    "SandboxValidationError",
]
