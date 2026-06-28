import sys
from types import SimpleNamespace

import pytest

import orchestrator


def test_default_host_is_localhost(monkeypatch):
    monkeypatch.delenv("HOST", raising=False)

    assert orchestrator.get_server_host() == "127.0.0.1"


def test_public_host_without_api_secret_is_refused(monkeypatch):
    monkeypatch.delenv("API_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Refus de demarrer.*API_SECRET_KEY"):
        orchestrator.ensure_public_host_requires_secret("0.0.0.0")

    with pytest.raises(RuntimeError, match="Refus de demarrer.*API_SECRET_KEY"):
        orchestrator.ensure_public_host_requires_secret("::")


def test_public_host_with_api_secret_is_allowed():
    orchestrator.ensure_public_host_requires_secret("0.0.0.0", api_secret="super-secret")
    orchestrator.ensure_public_host_requires_secret("::", api_secret="super-secret")


def test_run_server_uses_host_env_when_valid(monkeypatch):
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setenv("HOST", "127.0.0.1")
    monkeypatch.delenv("API_SECRET_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))

    orchestrator.run_server()

    assert calls == [(('orchestrator:app',), {'host': '127.0.0.1', 'port': 8000, 'reload': False})]
