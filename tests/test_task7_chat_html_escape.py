from pathlib import Path


def test_chat_never_injects_dynamic_content_with_inner_html():
    html = Path("static/chat.html").read_text(encoding="utf-8")

    assert ".innerHTML" not in html
    assert "document.createTextNode" in html
    assert "textContent" in html


def test_chat_renderer_escapes_html_payloads_by_construction():
    html = Path("static/chat.html").read_text(encoding="utf-8")

    assert "String(text ?? '').split" in html
    assert "container.appendChild(document.createTextNode(part))" in html
    assert "strong.textContent = part.slice(2, -2)" in html
    assert "cd.appendChild(document.createTextNode(description))" in html


import shutil
import subprocess

import pytest


def test_chat_xss_payload_renders_inert_when_executed():
    """Preuve comportementale : on execute le vrai code de rendu de chat.html
    dans Node contre une charge XSS et on verifie qu'aucun element actif
    (IMG/SCRIPT) n'est cree et que le texte reste litteral."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node introuvable : test comportemental XSS ignore")

    harness = Path(__file__).parent / "xss_render_harness.js"
    result = subprocess.run(
        [node, str(harness)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "FAIL" not in result.stdout, result.stdout
    assert "no IMG element created" in result.stdout
    assert "no SCRIPT element created" in result.stdout
