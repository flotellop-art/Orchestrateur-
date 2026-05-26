"""sandbox_commands.py — Sandbox et whitelist stricte pour l'execution de commandes.

Fonctionnalites :
- Whitelist non contournable des executables autorises
- Blocage de pip install, python -c, et combinaisons dangereuses
- Validation des arguments (pas de ; | & ` $() dans les args)
- Logging de toutes les commandes executees
- Remplacement drop-in pour les fonctions run_command de team.py et chat_agent.py

Integration : voir SECURITY_INSTALL.md
"""
from __future__ import annotations

import asyncio
import logging
import re
import shlex
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Whitelist des executables autorises
# ---------------------------------------------------------------------------
# IMPORTANT : cette whitelist est STRICTE. Toute commande dont l'executable
# n'est pas dans cette liste est rejetee, quelle que soit la maniere dont
# elle est formulee.

COMMAND_WHITELIST: frozenset[str] = frozenset({
    # Python
    "python",
    "python3",
    # Tests
    "pytest",
    # Utilitaires systeme (lecture seule)
    "ls",
    "cat",
    "echo",
    "pwd",
    "head",
    "tail",
    "wc",
    # Node
    "node",
    "npm",
    # pip (lecture seule — pip install bloque separement)
    "pip",
    "pip3",
})

# ---------------------------------------------------------------------------
# Sous-commandes / arguments BLOQUES explicitement
# Ces patterns sont verifies sur toute la ligne de commande.
# ---------------------------------------------------------------------------

# Pattern pour pip install et ses variantes
_RE_PIP_INSTALL = re.compile(
    r'\bpip\b.*\binstall\b|\bpip3\b.*\binstall\b',
    re.IGNORECASE
)

# Pattern pour python -c (execution de code inline)
_RE_PYTHON_INLINE = re.compile(
    r'\bpython[23]?\b.*\s-[^-]*c\b',
    re.IGNORECASE
)

# Pattern pour python -m (execution de module arbitraire)
# Sauf modules explicitement autorises
_RE_PYTHON_MODULE = re.compile(
    r'\bpython[23]?\b.*\s-[^-]*m\s+(?!pytest\b|http\.server\b)',
    re.IGNORECASE
)

# Modules npm dangereux
_BLOCKED_NPM_SUBCOMMANDS: frozenset[str] = frozenset({
    "install", "i", "ci", "update", "upgrade",
    "run",    # npm run <script> peut executer n'importe quoi
    "exec",   # npm exec = npx
    "x",      # alias de npm exec
    "publish", "pack",
})

# ---------------------------------------------------------------------------
# Caracteres de meta-shell interdits dans les ARGUMENTS
# (pas dans le nom de l'executable, deja filtre par whitelist)
# ---------------------------------------------------------------------------

_SHELL_METACHAR_RE = re.compile(
    r'[;|&`$()<>!\\]|\$\(|`[^`]*`|>>|&&|\|\|'
)

# Sequences d'injection supplementaires dans les arguments
_INJECTION_PATTERNS = [
    re.compile(r'\beval\b', re.IGNORECASE),
    re.compile(r'\bexec\b', re.IGNORECASE),
    re.compile(r'__import__'),
    re.compile(r'\bos\.system\b'),
    re.compile(r'\bsubprocess\b'),
    re.compile(r'\bopen\s*\('),
    re.compile(r'/etc/passwd'),
    re.compile(r'/etc/shadow'),
    re.compile(r'\.\./'),  # path traversal dans les args
]


# ---------------------------------------------------------------------------
# Fonctions de validation
# ---------------------------------------------------------------------------

class CommandForbiddenError(ValueError):
    """Levee quand une commande est refusee par le sandbox."""


def _validate_command_string(cmd: str) -> list[str]:
    """Parse et valide une commande shell.

    Retourne la liste des tokens [executable, arg1, arg2, ...] si valide.
    Leve CommandForbiddenError si la commande est refusee.
    """
    if not cmd or not isinstance(cmd, str):
        raise CommandForbiddenError("Commande vide ou invalide.")

    cmd = cmd.strip()

    # 1. Verifications pre-parse sur la commande brute
    #    (avant shlex.split pour attraper les injections camouflees)
    if _RE_PIP_INSTALL.search(cmd):
        raise CommandForbiddenError(
            f"[SANDBOX] pip install est interdit pour eviter l'installation de paquets arbitraires. "
            f"Commande bloquee : {cmd!r}"
        )

    if _RE_PYTHON_INLINE.search(cmd):
        raise CommandForbiddenError(
            f"[SANDBOX] python -c (code inline) est interdit. "
            f"Commande bloquee : {cmd!r}"
        )

    if _RE_PYTHON_MODULE.search(cmd):
        raise CommandForbiddenError(
            f"[SANDBOX] python -m <module_arbitraire> est interdit. "
            f"Commande bloquee : {cmd!r}"
        )

    # 2. Refuser les meta-caracteres shell dans la commande entiere
    #    (evite les injections via ; | & etc.)
    if _SHELL_METACHAR_RE.search(cmd):
        raise CommandForbiddenError(
            f"[SANDBOX] Caracteres shell dangereux detectes (;|&`$...). "
            f"Commande bloquee : {cmd!r}"
        )

    # 3. Parser la commande de maniere sure
    try:
        tokens = shlex.split(cmd)
    except ValueError as exc:
        raise CommandForbiddenError(
            f"[SANDBOX] Erreur de parsing de la commande : {exc}. Commande : {cmd!r}"
        ) from exc

    if not tokens:
        raise CommandForbiddenError("Commande vide apres parsing.")

    executable = tokens[0].lower().strip()

    # 4. Verifier l'executable contre la whitelist
    #    Accepter aussi les chemins absolus vers des executables whitelistes
    #    (ex: /usr/bin/python3 -> "python3" whiteliste)
    exe_base = executable.split('/')[-1].split('\\')[-1]
    # Supprimer l'extension .exe (Windows)
    if exe_base.endswith('.exe'):
        exe_base = exe_base[:-4]

    if exe_base not in COMMAND_WHITELIST:
        raise CommandForbiddenError(
            f"[SANDBOX] Executable non autorise : {exe_base!r}. "
            f"Whitelist : {sorted(COMMAND_WHITELIST)}"
        )

    # 5. Verifications supplementaires selon l'executable
    args = tokens[1:]

    if exe_base in ('pip', 'pip3'):
        if args and args[0].lower() in ('install', 'download', 'wheel',
                                         'hash', '--editable', '-e'):
            raise CommandForbiddenError(
                f"[SANDBOX] pip {args[0]} est interdit. "
                f"Seules les commandes de lecture (pip list, pip show, pip freeze) sont autorisees."
            )

    if exe_base == 'npm':
        if args and args[0].lower() in _BLOCKED_NPM_SUBCOMMANDS:
            raise CommandForbiddenError(
                f"[SANDBOX] npm {args[0]} est interdit."
            )

    # 6. Valider les arguments individuellement
    for arg in args:
        for pattern in _INJECTION_PATTERNS:
            if pattern.search(arg):
                raise CommandForbiddenError(
                    f"[SANDBOX] Argument dangereux detecte : {arg!r} "
                    f"(pattern: {pattern.pattern}). Commande bloquee."
                )

    return tokens


def is_command_allowed(cmd: str) -> tuple[bool, str]:
    """Verifie si une commande est autorisee sans l'executer.

    Retourne (True, "") si autorisee, (False, raison) sinon.
    Utile pour les UI qui veulent afficher un message avant execution.
    """
    try:
        _validate_command_string(cmd)
        return True, ""
    except CommandForbiddenError as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# Executeur securise (remplace subprocess.run / asyncio.create_subprocess_shell)
# ---------------------------------------------------------------------------

async def safe_run_command(
    cmd: str,
    cwd: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[int, str, str]:
    """Execute une commande de maniere securisee apres validation.

    - Valide la commande contre la whitelist
    - Utilise create_subprocess_exec (pas de shell=True !)
    - Timeout configurable
    - Log toutes les executions

    Retourne (returncode, stdout, stderr).
    Leve CommandForbiddenError si la commande est bloquee.
    """
    tokens = _validate_command_string(cmd)  # leve CommandForbiddenError si invalide

    log.info("[SANDBOX] Execution autorisee : %s (cwd=%s)", tokens, cwd)

    try:
        proc = await asyncio.create_subprocess_exec(
            *tokens,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            # PAS de shell=True — evite l'interpretation des meta-caracteres
        )
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            log.warning("[SANDBOX] Timeout (%ss) depasse pour : %s", timeout, tokens)
            return -1, "", f"Timeout: la commande a depasse {timeout}s"

        stdout = stdout_bytes.decode('utf-8', errors='replace')
        stderr = stderr_bytes.decode('utf-8', errors='replace')
        rc = proc.returncode or 0

        log.info("[SANDBOX] Resultat : rc=%d, stdout=%d chars, stderr=%d chars",
                 rc, len(stdout), len(stderr))
        return rc, stdout, stderr

    except FileNotFoundError as exc:
        log.error("[SANDBOX] Executable introuvable : %s — %s", tokens[0], exc)
        return 1, "", f"Executable introuvable : {tokens[0]}"
    except PermissionError as exc:
        log.error("[SANDBOX] Permission refusee pour : %s — %s", tokens[0], exc)
        return 1, "", f"Permission refusee : {exc}"


def safe_run_command_sync(
    cmd: str,
    cwd: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[int, str, str]:
    """Version synchrone de safe_run_command.

    A utiliser dans les contextes non-async (ex: fonctions utilitaires).
    """
    import subprocess

    tokens = _validate_command_string(cmd)  # leve CommandForbiddenError si invalide

    log.info("[SANDBOX] Execution synchrone autorisee : %s (cwd=%s)", tokens, cwd)

    try:
        result = subprocess.run(
            tokens,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
            shell=False,  # JAMAIS shell=True
        )
        log.info("[SANDBOX] Resultat sync : rc=%d", result.returncode)
        return result.returncode, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        log.warning("[SANDBOX] Timeout sync (%ss) pour : %s", timeout, tokens)
        return -1, "", f"Timeout: la commande a depasse {timeout}s"
    except FileNotFoundError:
        return 1, "", f"Executable introuvable : {tokens[0]}"
    except PermissionError as exc:
        return 1, "", f"Permission refusee : {exc}"


# ---------------------------------------------------------------------------
# Patch de la whitelist team.py (mise a jour de COMMAND_WHITELIST)
# ---------------------------------------------------------------------------

def get_safe_command_whitelist() -> frozenset[str]:
    """Retourne la whitelist securisee a utiliser dans team.py.

    Usage dans team.py ::

        from patches.security.sandbox_commands import get_safe_command_whitelist
        COMMAND_WHITELIST = get_safe_command_whitelist()
    """
    return COMMAND_WHITELIST


# ---------------------------------------------------------------------------
# Utilitaire : audit de la whitelist chat_agent.py
# ---------------------------------------------------------------------------

# L'ancienne whitelist de chat_agent.py incluait des commandes sensibles
# comme 'python --version' et 'pip list' sous forme de chaines completes.
# La nouvelle approche valide l'executable + les arguments separement.

CHAT_AGENT_SAFE_COMMANDS: list[str] = [
    # Commandes autorisees pour le chat agent (lecture seule uniquement)
    "echo",
    "pwd",
    "ls",
    "python --version",
    "python -V",
    "pip list",
    "pip freeze",
    "pip show",
    "git status",
    "git log --oneline",
    "git branch",
    "node --version",
    "npm --version",
]
