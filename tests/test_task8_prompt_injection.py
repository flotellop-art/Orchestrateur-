"""Tache 8 — defense contre l'injection de prompt via depot cible.

Les tests supposent un agent DEJA COMPROMIS : la defense doit tenir au niveau
des outils, sans dependre de l'obeissance du modele. Aucun appel LLM ni reseau.
"""
import asyncio
import os

import pytest

import team
from patches.security import repo_guard

SECRET = "API_SECRET=sk-live-DEADBEEF-supersecret"


def _symlink(target, link) -> bool:
    try:
        os.symlink(target, link)
        return True
    except (OSError, NotImplementedError):
        return False


def _run(coro):
    return asyncio.run(coro)


# ── Brique 1 : fichiers sensibles ─────────────────────────────────────────────
@pytest.mark.parametrize("rel", [
    ".env", ".env.local", "config/.env.prod", ".git/config", "deploy/id_rsa",
    "certs/server.key", "tls/server.pem", "store.keystore", "creds.secret",
    "..\\.env", ".npmrc", ".netrc",
])
def test_fichiers_sensibles_detectes(rel):
    assert repo_guard.is_sensitive_target_file(rel) is True


@pytest.mark.parametrize("rel", [
    "app.py", "README.md", "src/main.js", "schema.sql",
    ".env.example", ".env.sample", "id_rsa.pub",  # gabarits / cle publique : permis
])
def test_fichiers_normaux_non_sensibles(rel):
    assert repo_guard.is_sensitive_target_file(rel) is False


# ── Chokepoint : _target_file protege TOUS les appelants (agent + HTTP) ───────
def test_target_file_refuse_secret_direct(tmp_path):
    (tmp_path / ".env").write_text(SECRET)
    # protege aussi l'endpoint /api/tasks/{id}/file qui passe par _target_file
    assert team._target_file(str(tmp_path), ".env") is None


def test_symlink_alias_vers_secret_bloque(tmp_path):
    (tmp_path / ".env").write_text(SECRET)
    if not _symlink(".env", str(tmp_path / "notes.txt")):
        pytest.skip("symlinks non supportes sur cette plateforme")
    # lecture de l'alias -> bloquee (cible reelle sensible)
    assert team._target_file(str(tmp_path), "notes.txt") is None
    # et l'alias n'est meme pas liste
    assert "notes.txt" not in team._list_target_files(str(tmp_path))


# ── Critere B : le listing du depot cible exclut les fichiers sensibles ───────
def test_listing_depot_exclut_secrets(tmp_path):
    (tmp_path / ".env").write_text(SECRET)
    (tmp_path / "README.md").write_text("# projet")
    (tmp_path / "app.py").write_text("print(1)")
    listed = team._list_target_files(str(tmp_path))
    assert ".env" not in listed
    assert "README.md" in listed and "app.py" in listed


# ── Critere A : read_file refuse les fichiers sensibles (sans rien divulguer) ─
@pytest.mark.parametrize("rel", [".env", ".git/config", "id_rsa"])
def test_read_file_refuse_fichier_sensible(tmp_path, rel):
    out = _run(team.execute_tool(0, str(tmp_path), False, {"tool": "read_file", "path": rel}))
    assert "refus" in out.lower()
    assert "DEADBEEF" not in out


# ── Critere C : un agent compromis ne peut pas exfiltrer un secret via write ──
def test_write_file_bloque_secret(tmp_path):
    out = _run(team.execute_tool(0, str(tmp_path), False, {
        "tool": "write_file", "path": "delivery/audit.md", "content": "Voici la cle: " + SECRET,
    }))
    assert "refus" in out.lower()
    assert not (tmp_path / "delivery" / "audit.md").exists()  # rien ecrit


# ── Critere D : ni via web_search ─────────────────────────────────────────────
def test_web_search_bloque_secret(tmp_path):
    out = _run(team.execute_tool(0, str(tmp_path), True, {
        "tool": "web_search", "query": "sk-live-DEADBEEF-supersecret",
    }))
    assert "refus" in out.lower()


def test_mcp_call_bloque_secret(tmp_path):
    out = _run(team.execute_tool(0, str(tmp_path), False, {
        "tool": "mcp_call", "server": "x", "name": "y", "arguments": {"q": SECRET},
    }))
    assert "refus" in out.lower()


# ── Critere E : contenu non fiable delimite, secret-scan sans faux positif ────
def test_wrap_untrusted_delimite():
    wrapped = repo_guard.wrap_untrusted("contenu du readme", source="depot cible")
    assert "NON FIABLES" in wrapped and "contenu du readme" in wrapped


def test_scan_secrets_detecte_les_formats_courants():
    for s in ["sk-live-DEADBEEF-supersecret", "AKIAIOSFODNN7EXAMPLE",
              "ghp_0123456789abcdefghijABCDEF", "-----BEGIN RSA PRIVATE KEY-----"]:
        assert repo_guard.contains_secret(s) is True


def test_scan_secrets_pas_de_faux_positif_sur_texte_normal():
    normal = "Le rapport d'audit decrit l'architecture et les risques. Aucun secret ici."
    assert repo_guard.find_secrets(normal) == []


# ── Critere F : l'usage legitime fonctionne encore ────────────────────────────
def test_write_file_legitime_fonctionne(tmp_path):
    out = _run(team.execute_tool(0, str(tmp_path), False, {
        "tool": "write_file", "path": "delivery/rapport.md", "content": "# Audit\nRAS.",
    }))
    assert "ecrit" in out.lower()
    assert (tmp_path / "delivery" / "rapport.md").read_text() == "# Audit\nRAS."
