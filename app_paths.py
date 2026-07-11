"""Chemins de l'application, separes entre ressources et donnees modifiables.

En developpement les deux racines pointent vers le depot. Dans l'application
desktop empaquetee, les ressources restent dans le bundle tandis qu'Electron
fournit ``ORCHESTRATOR_DATA_DIR`` pour placer la base et les projets dans le
dossier utilisateur, qui est inscriptible et persistant entre les mises a jour.
"""

from __future__ import annotations

import os
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = APP_ROOT / "static"


def _data_root() -> Path:
    configured = (os.getenv("ORCHESTRATOR_DATA_DIR") or "").strip()
    root = Path(configured).expanduser() if configured else APP_ROOT
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


DATA_ROOT = _data_root()
DB_PATH = DATA_ROOT / "apps.db"
PROJECTS_ROOT = DATA_ROOT / "projects"
CHAT_WORKSPACE_ROOT = DATA_ROOT / "chat_workspace"

PROJECTS_ROOT.mkdir(parents=True, exist_ok=True)
CHAT_WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)


def describe_paths() -> dict[str, str]:
    """Retourne des chemins affichables sans exposer de secret."""

    return {
        "app_root": str(APP_ROOT),
        "data_root": str(DATA_ROOT),
        "database": str(DB_PATH),
        "projects": str(PROJECTS_ROOT),
        "static": str(STATIC_ROOT),
    }
