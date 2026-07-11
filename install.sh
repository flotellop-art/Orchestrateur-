#!/usr/bin/env bash
# Prepare the Orchestrateur development environment from this repository.
set -euo pipefail

if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "Python 3.12 is required." >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "Node.js 24 and npm are required for the desktop application." >&2
  exit 1
fi

"$PY" -m venv .venv
if [ -x .venv/Scripts/python.exe ]; then
  VENV_PY=.venv/Scripts/python.exe
else
  VENV_PY=.venv/bin/python
fi

"$VENV_PY" -m pip install -r requirements-dev.txt
(cd electron-app && npm ci)

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example; add only the provider keys you use."
fi

echo "Development environment ready."
echo "Build the Docker sandbox with: ./scripts/build_sandbox.ps1 (PowerShell)"
echo "Start the server with: $VENV_PY orchestrator.py"
