"""Tests de securite : un test par correctif du durcissement.

Lancement : `pytest -q` depuis la racine du depot.
Aucun appel reseau ni LLM : on teste les fonctions pures et le cablage.
"""
import asyncio
import inspect
import json
from pathlib import Path

import pytest

import orchestrator
import team
from patches.security.sandbox_commands import is_command_allowed

REPO = Path(__file__).resolve().parent.parent


# ── Tache 1 : garde HOST / API_SECRET_KEY ─────────────────────────────────────
def test_host_localhost_ok_sans_cle():
    # 127.0.0.1 sans cle : autorise (aucune exception).
    orchestrator.check_host_security("127.0.0.1", "")


@pytest.mark.parametrize("host", ["0.0.0.0", "::"])
def test_host_public_sans_cle_refuse(host):
    with pytest.raises(RuntimeError):
        orchestrator.check_host_security(host, "   ")


def test_host_public_avec_cle_ok():
    orchestrator.check_host_security("0.0.0.0", "cle-secrete")


def test_main_utilise_localhost_par_defaut():
    # La valeur par defaut de HOST dans orchestrator.py doit etre 127.0.0.1.
    src = (REPO / "orchestrator.py").read_text(encoding="utf-8")
    assert 'os.getenv("HOST", "127.0.0.1")' in src
    assert 'host="0.0.0.0"' not in src


# ── Tache 2 : path traversal sur les fichiers generes ─────────────────────────
def test_chemin_genere_autorise_fichiers_connus(tmp_path):
    p = orchestrator.safe_generated_path(tmp_path, "app.py")
    assert p.parent == tmp_path.resolve()
    assert p.name == "app.py"


@pytest.mark.parametrize("bad", [
    "../evil.py", "/etc/passwd", "..\\evil.py", "sub/app.py",
    "app.py\x00", "config.py", "", "  ",
])
def test_chemin_genere_bloque_traversal_et_inconnus(tmp_path, bad):
    with pytest.raises(ValueError):
        orchestrator.safe_generated_path(tmp_path, bad)


def test_aucune_ecriture_hors_dossier(tmp_path):
    # On simule des `filename` malveillants : rien ne doit etre ecrit dehors.
    outside = tmp_path.parent / "evil.py"
    if outside.exists():
        outside.unlink()
    for bad in ["../evil.py", "/etc/passwd", "../../evil.py"]:
        try:
            orchestrator.safe_generated_path(tmp_path, bad).write_text("x")
        except ValueError:
            pass
    assert not outside.exists()


# ── Tache 3 : validation de requirements.txt ──────────────────────────────────
def test_requirements_autorise_flask():
    assert "flask" in orchestrator.validate_requirements_txt("flask\n").lower()


def test_requirements_autorise_flask_pinne_et_commentaires():
    out = orchestrator.validate_requirements_txt("# dep\nFlask==3.0.0\n")
    assert "flask" in out.lower()


@pytest.mark.parametrize("bad", [
    "requests",
    "flask\nrequests",
    "git+https://evil/repo.git",
    "https://evil/pkg.whl",
    "-e .",
    "--extra-index-url https://evil",
    "../local-pkg",
])
def test_requirements_refuse_tout_le_reste(bad):
    with pytest.raises(ValueError):
        orchestrator.validate_requirements_txt(bad)


# ── Tache 4 : pas de secret dans le packaging Electron ────────────────────────
def test_electron_ne_package_ni_env_ni_db():
    pkg = json.loads((REPO / "electron-app/package.json").read_text(encoding="utf-8"))
    flt = pkg["build"]["extraResources"][0]["filter"]
    assert ".env" not in flt
    assert "*.db" not in flt


# ── Tache 5 : sandbox de commandes branchee dans team.py ──────────────────────
def test_team_run_cmd_delegue_a_la_sandbox():
    assert "safe_run_command" in inspect.getsource(team._run_cmd)


@pytest.mark.parametrize("cmd", [
    "pip install requests", "pip3 install evil", "npm install x",
    "python -c 'import os'", "ls; rm -rf /", "cat /etc/passwd",
])
def test_sandbox_bloque_dangereux(cmd):
    ok, _ = is_command_allowed(cmd)
    assert ok is False


@pytest.mark.parametrize("cmd", ["pytest", "echo hello", "ls -la", "pip list"])
def test_sandbox_autorise_inoffensif(cmd):
    ok, _ = is_command_allowed(cmd)
    assert ok is True


def test_team_run_cmd_refuse_pip_install_sans_executer():
    out = asyncio.run(team._run_cmd(".", "pip install requests"))
    assert "refus" in out.lower()


# ── Tache 6 : chat casse retire des points d'entree UI ────────────────────────
def test_menu_electron_sans_lien_chat_casse():
    assert "/chat" not in (REPO / "electron-app/main.js").read_text(encoding="utf-8")


def test_control_html_sans_onglet_chat():
    assert 'href="#chat"' not in (REPO / "static/control.html").read_text(encoding="utf-8")


# ── Tache 7 : XSS echappe dans chat.html ──────────────────────────────────────
def test_chat_html_echappe_le_html():
    src = (REPO / "static/chat.html").read_text(encoding="utf-8")
    # une fonction d'echappement existe
    assert "&amp;" in src and "&lt;" in src
    # plus d'injection directe de contenu non echappe via innerHTML
    assert "innerHTML = content.replace" not in src
    assert "innerHTML = assistantText.replace" not in src
