#!/usr/bin/env bash
# Quick Start installer for the Claude Managed Agents tutorial.
# Tested for Git Bash on Windows + macOS/Linux bash.
set -euo pipefail

# Pick a Python launcher that exists on this machine.
# On Windows + Git Bash, "python" is normal; "python3" often does NOT exist.
# On macOS/Linux, "python3" is normal.
if command -v python >/dev/null 2>&1; then
  PY=python
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v py >/dev/null 2>&1; then
  PY="py -3"     # Windows py launcher fallback
else
  echo "ERROR: no Python interpreter found on PATH (tried python, python3, py)." >&2
  exit 1
fi

echo "Using Python: $($PY --version)"

# Always install via "python -m pip" — avoids the "pip not on PATH" problem
# that bites Windows users in Git Bash.
$PY -m pip install --upgrade pip
$PY -m pip install claude-agent-sdk python-dotenv

# claude-agent-sdk shells out to the Claude Code CLI (a Node binary).
# Warn the user if it isn't installed. Don't hard-fail — they may install later.
if ! command -v claude >/dev/null 2>&1; then
  cat >&2 <<'EOF'

WARNING: the "claude" CLI was not found on PATH.
claude-agent-sdk spawns it as a subprocess, so run.py will fail without it.
Install Node.js 18+ then:  npm install -g @anthropic-ai/claude-code
On Git Bash you may need to restart the shell so PATH picks up the npm global bin.

EOF
fi

# Create .env only if it doesn't exist, so we don't clobber a real key.
if [ ! -f .env ]; then
  printf 'ANTHROPIC_API_KEY=sk-ant-your-key\n' > .env
  echo "Created .env — replace sk-ant-your-key with your real key from"
  echo "  https://platform.claude.com/settings/keys"
else
  echo ".env already exists — leaving it alone."
fi

echo "Done. Next steps:"
echo "  1. Edit .env and paste your real ANTHROPIC_API_KEY"
echo "  2. Run:  $PY run.py"
