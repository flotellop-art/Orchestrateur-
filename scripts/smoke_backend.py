"""Demarre le backend empaquete, verifie /health puis l'arrete."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: smoke_backend.py <executable>", file=sys.stderr)
        return 2
    executable = Path(sys.argv[1]).resolve()
    if not executable.is_file():
        print(f"executable introuvable: {executable}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="orchestrator-smoke-") as data_dir:
        instance_token = secrets.token_urlsafe(32)
        env = os.environ.copy()
        env.update(
            {
                "ORCHESTRATOR_DATA_DIR": data_dir,
                "ORCHESTRATOR_HOST": "127.0.0.1",
                "PORT": "8765",
                "ORCHESTRATOR_INSTANCE_TOKEN": instance_token,
                "PYTHONUNBUFFERED": "1",
            }
        )
        proc = subprocess.Popen(
            [str(executable)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    output = proc.stdout.read() if proc.stdout else ""
                    print(output, file=sys.stderr)
                    return 1
                try:
                    request = urllib.request.Request(
                        "http://127.0.0.1:8765/health",
                        headers={"X-Orchestrator-Instance": instance_token},
                    )
                    with urllib.request.urlopen(request, timeout=1) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                    if (
                        response.status == 200
                        and payload.get("status") == "ok"
                        and payload.get("instance") is True
                        and instance_token not in json.dumps(payload)
                    ):
                        return 0
                except Exception:
                    time.sleep(0.5)
            print("le backend n'a pas repondu dans les delais", file=sys.stderr)
            return 1
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
