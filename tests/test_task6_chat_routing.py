from pathlib import Path

import orchestrator


def test_chat_router_is_included_in_orchestrator():
    paths = {getattr(route, "path", None) for route in orchestrator.app.routes}

    assert "/chat" in paths
    assert "/api/chat" in paths
    assert "/api/chat/history" in paths


def test_chat_frontend_handles_backend_event_names():
    html = Path("static/chat.html").read_text(encoding="utf-8")

    assert "evt.type === 'text_delta'" in html
    assert "evt.content || evt.text" in html
    assert "evt.type === 'tool_executing'" in html
    assert "evt.type === 'confirmation_needed'" in html
    assert "m.content || m.text" in html
