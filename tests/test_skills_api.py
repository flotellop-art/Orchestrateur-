import asyncio
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import skills_api
from agent_skills import (
    SkillStatus,
    get_skill,
    init_agent_skills_db,
    propose_skill,
)
from auth_middleware import PUBLIC_PATHS


SUMMARY = "Réaliser une revue fiable avant de livrer un changement important."
INSTRUCTIONS = """Lire tous les fichiers concernés avant de conclure.

1. Comparer le comportement obtenu au comportement attendu.
2. Chercher les risques de sécurité et les erreurs silencieuses.
3. Exécuter les tests pertinents et expliquer les résultats.
"""


TEST_API_SECRET = "test-review-secret-that-is-long-enough"


class SkillsAPITests(unittest.TestCase):
    def setUp(self):
        self.secret_patch = patch.dict(
            os.environ, {"API_SECRET_KEY": TEST_API_SECRET}, clear=False
        )
        self.secret_patch.start()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "skills-api.db"
        self.export_root = self.root / "data" / "skills"
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY, objective TEXT NOT NULL)"
            )
            db.executemany(
                "INSERT INTO tasks(id, objective) VALUES (?, ?)",
                ((1, "Première tâche"), (2, "Deuxième tâche")),
            )
            db.commit()
        asyncio.run(init_agent_skills_db(self.db_path))

        self.db_patch = patch.object(skills_api, "DB_PATH", self.db_path)
        self.export_patch = patch.object(
            skills_api, "SKILLS_EXPORT_ROOT", self.export_root
        )
        self.db_patch.start()
        self.export_patch.start()
        with skills_api._review_sessions_lock:
            skills_api._review_sessions.clear()

        app = FastAPI()
        app.include_router(skills_api.router)
        self.client = TestClient(app, base_url="http://testserver")
        self.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Site": "same-origin",
            "X-Orchestrator-Human-UI": "skills-page",
            "User-Agent": "orchestrator-human-browser",
            "X-API-Key": TEST_API_SECRET,
        }

    def tearDown(self):
        self.client.close()
        with skills_api._review_sessions_lock:
            skills_api._review_sessions.clear()
        self.export_patch.stop()
        self.db_patch.stop()
        self.secret_patch.stop()
        self.temp.cleanup()

    def propose(self, **changes):
        values = {
            "task_id": 1,
            "agent": "Review Agent",
            "name": "API Review",
            "summary": SUMMARY,
            "instructions": INSTRUCTIONS,
            "tags": ["api", "review"],
            "db_path": self.db_path,
        }
        values.update(changes)
        return asyncio.run(propose_skill(**values))

    def stored(self, skill_id):
        return asyncio.run(get_skill(skill_id, db_path=self.db_path))

    def review_token(self, headers=None):
        response = self.client.post(
            "/api/skills/review-session",
            headers=headers or self.browser_headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("HttpOnly", response.headers.get("set-cookie", ""))
        self.assertIn("SameSite=strict", response.headers.get("set-cookie", ""))
        return response.json()["review_token"]

    def decide(self, skill_id, body, *, token=None, headers=None):
        request_headers = dict(headers or self.browser_headers)
        if token is not None:
            request_headers["X-Orchestrator-Review-Token"] = token
        return self.client.post(
            f"/api/skills/{skill_id}/decision",
            headers=request_headers,
            json=body,
        )

    def test_page_is_a_public_data_free_shell_with_security_headers(self):
        self.assertIn("/skills", PUBLIC_PATHS)
        response = self.client.get("/skills")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertIn("object-src 'none'", response.headers["content-security-policy"])
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertIn("Méthodes proposées", response.text)

    def test_static_ui_never_renders_server_data_with_html_apis(self):
        html = (Path(__file__).parents[1] / "static" / "skills.html").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("innerHTML", html)
        self.assertNotIn("outerHTML", html)
        self.assertNotIn("insertAdjacentHTML", html)
        self.assertNotIn("document.write", html)
        self.assertNotIn("eval(", html)
        self.assertIn("textContent", html)
        self.assertIn("replaceChildren", html)
        self.assertIn("X-Orchestrator-Review-Token", html)
        self.assertNotIn("localStorage.setItem", html)

    def test_list_is_compact_and_detail_contains_the_document(self):
        pending = self.propose()
        second = self.propose(
            task_id=2,
            agent="Other Agent",
            name="Other Review",
        )

        response = self.client.get("/api/skills", params={"status": "pending"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 2)
        self.assertEqual(
            {item["id"] for item in payload["skills"]}, {pending.id, second.id}
        )
        self.assertTrue(
            all("instructions" not in item for item in payload["skills"])
        )

        detail = self.client.get(f"/api/skills/{pending.id}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["skill"]["instructions"], INSTRUCTIONS.strip())
        self.assertEqual(self.client.get("/api/skills/99999").status_code, 404)

    def test_agent_style_direct_calls_cannot_approve(self):
        skill = self.propose()
        no_session = self.decide(skill.id, {"decision": "approve"})
        self.assertEqual(no_session.status_code, 403)
        self.assertEqual(self.stored(skill.id).status, SkillStatus.PENDING)

        forbidden_fields = (
            {"decision": "approve", "human_confirmed": True},
            {"decision": "approve", "approved_by": "Agent"},
            {"decision": "approve", "export_root": "../outside"},
        )
        for body in forbidden_fields:
            with self.subTest(body=body):
                response = self.decide(skill.id, body)
                self.assertEqual(response.status_code, 422)
        self.assertEqual(self.stored(skill.id).status, SkillStatus.PENDING)

    def test_review_session_requires_same_origin_browser_metadata(self):
        bad_headers = (
            {},
            {"Origin": "http://testserver"},
            {
                "Origin": "https://evil.example",
                "Sec-Fetch-Site": "same-origin",
                "X-Orchestrator-Human-UI": "skills-page",
            },
            {
                "Origin": "http://testserver",
                "Sec-Fetch-Site": "cross-site",
                "X-Orchestrator-Human-UI": "skills-page",
            },
        )
        for headers in bad_headers:
            with self.subTest(headers=headers):
                headers = {**headers, "X-API-Key": TEST_API_SECRET}
                response = self.client.post(
                    "/api/skills/review-session", headers=headers
                )
                self.assertEqual(response.status_code, 403)

    def test_forged_loopback_review_requires_the_configured_secret(self):
        forged = {
            "Origin": "http://testserver",
            "Sec-Fetch-Site": "same-origin",
            "X-Orchestrator-Human-UI": "skills-page",
            "User-Agent": "forged-local-client",
        }
        missing = self.client.post("/api/skills/review-session", headers=forged)
        self.assertEqual(missing.status_code, 401)
        wrong = self.client.post(
            "/api/skills/review-session",
            headers={**forged, "X-API-Key": "wrong-secret"},
        )
        self.assertEqual(wrong.status_code, 403)
        with patch.dict(os.environ, {"API_SECRET_KEY": ""}, clear=False):
            unconfigured = self.client.post(
                "/api/skills/review-session",
                headers={**forged, "X-API-Key": TEST_API_SECRET},
            )
        self.assertEqual(unconfigured.status_code, 503)

    def test_decision_rechecks_the_server_secret_before_consuming_session(self):
        skill = self.propose()
        token = self.review_token()
        without_key = dict(self.browser_headers)
        without_key.pop("X-API-Key")
        refused = self.decide(
            skill.id,
            {"decision": "approve"},
            token=token,
            headers=without_key,
        )
        self.assertEqual(refused.status_code, 401)
        approved = self.decide(skill.id, {"decision": "approve"}, token=token)
        self.assertEqual(approved.status_code, 200, approved.text)

    def test_review_session_is_bound_to_browser_and_used_once(self):
        first = self.propose(name="First Review")
        second = self.propose(name="Second Review")
        token = self.review_token()
        changed_headers = dict(self.browser_headers)
        changed_headers["User-Agent"] = "agent-http-client"
        wrong_browser = self.decide(
            first.id,
            {"decision": "approve"},
            token=token,
            headers=changed_headers,
        )
        self.assertEqual(wrong_browser.status_code, 403)
        self.assertEqual(self.stored(first.id).status, SkillStatus.PENDING)

        # La tentative invalide a consommé le jeton.
        replay = self.decide(first.id, {"decision": "approve"}, token=token)
        self.assertEqual(replay.status_code, 403)

        valid_token = self.review_token()
        approved = self.decide(
            first.id, {"decision": "approve"}, token=valid_token
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        second_replay = self.decide(
            second.id, {"decision": "approve"}, token=valid_token
        )
        self.assertEqual(second_replay.status_code, 403)
        self.assertEqual(self.stored(second.id).status, SkillStatus.PENDING)

    def test_approval_injects_human_identity_and_exports_only_under_data_root(self):
        skill = self.propose()
        token = self.review_token()
        response = self.decide(
            skill.id,
            {"decision": "approve"},
            token=token,
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["skill"]["status"], "active")
        self.assertEqual(payload["skill"]["decided_by"], "human-ui")
        self.assertTrue(payload["exported"])
        self.assertEqual(payload["export_name"], "api-review/SKILL.md")
        self.assertNotIn(str(self.root), payload["export_name"])

        export = self.export_root / "api-review" / "SKILL.md"
        self.assertTrue(export.is_file())
        self.assertIn("name: api-review", export.read_text(encoding="utf-8"))
        self.assertEqual(self.stored(skill.id).status, SkillStatus.ACTIVE)

    def test_rejection_requires_reason_and_never_exports(self):
        skill = self.propose(name="Rejected Review")
        missing_reason = self.decide(
            skill.id,
            {"decision": "reject"},
            token=self.review_token(),
        )
        self.assertEqual(missing_reason.status_code, 400)
        self.assertEqual(self.stored(skill.id).status, SkillStatus.PENDING)

        response = self.decide(
            skill.id,
            {"decision": "reject", "reason": "Trop spécifique à cette tâche."},
            token=self.review_token(),
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["skill"]["status"], "rejected")
        self.assertEqual(payload["skill"]["decided_by"], "human-ui")
        self.assertFalse(payload["exported"])
        self.assertFalse(self.export_root.exists())

    def test_replacement_needs_an_explicit_second_human_decision(self):
        first = self.propose()
        first_response = self.decide(
            first.id,
            {"decision": "approve"},
            token=self.review_token(),
        )
        self.assertEqual(first_response.status_code, 200)

        second = self.propose(
            task_id=2,
            agent="Other Agent",
            instructions=INSTRUCTIONS + "\n4. Ajouter une preuve pour chaque conclusion.",
        )
        conflict = self.decide(
            second.id,
            {"decision": "approve"},
            token=self.review_token(),
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(self.stored(first.id).status, SkillStatus.ACTIVE)
        self.assertEqual(self.stored(second.id).status, SkillStatus.PENDING)

        replacement = self.decide(
            second.id,
            {"decision": "approve", "replace_existing": True},
            token=self.review_token(),
        )
        self.assertEqual(replacement.status_code, 200, replacement.text)
        self.assertEqual(self.stored(first.id).status, SkillStatus.REJECTED)
        self.assertEqual(self.stored(second.id).status, SkillStatus.ACTIVE)
        exported = (self.export_root / "api-review" / "SKILL.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("Ajouter une preuve", exported)

    def test_initialization_wrapper_uses_the_shared_database_and_export_root(self):
        self.export_root.mkdir(parents=True)
        asyncio.run(skills_api.init_skills_api())
        asyncio.run(skills_api.init_skills_api())
        self.assertTrue(self.export_root.is_dir())
        with closing(sqlite3.connect(self.db_path)) as db:
            table = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_skills'"
            ).fetchone()
        self.assertEqual(table, ("agent_skills",))

    def test_router_lifespan_initializes_the_component_automatically(self):
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute("DROP TABLE agent_skills")
            db.commit()
        app = FastAPI()
        app.include_router(skills_api.router)
        with TestClient(app, base_url="http://testserver") as client:
            response = client.get("/api/skills")
            self.assertEqual(response.status_code, 200, response.text)
        with closing(sqlite3.connect(self.db_path)) as db:
            table = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_skills'"
            ).fetchone()
        self.assertEqual(table, ("agent_skills",))


if __name__ == "__main__":
    unittest.main()
