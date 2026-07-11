import asyncio
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import automation_api
from automation_api import configure, router
from automations import AutomationStore
from durable_queue import DurableQueue
from messaging import MessagingGateway, ServerChannelRegistry


SLACK_WEBHOOK = "https://hooks.slack.com/services/T000/B000/server-secret"


class NoNetworkTransport:
    async def post_json(self, url, payload, *, timeout, max_response_bytes):
        raise AssertionError("Aucun appel reseau n'est attendu dans ces tests.")


def build_gateway():
    environment = {
        "ORCHESTRATOR_MESSAGING_CHANNELS": json.dumps(
            {
                "build_slack": {
                    "type": "slack",
                    "webhook_env": "BUILD_SLACK_WEBHOOK",
                }
            }
        ),
        "ORCHESTRATOR_MESSAGING_ALLOWED_CHANNELS": "build_slack",
        "BUILD_SLACK_WEBHOOK": SLACK_WEBHOOK,
    }
    registry = ServerChannelRegistry.from_environment(environment)
    return MessagingGateway(registry, transport=NoNetworkTransport())


class AutomationApiTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "orchestrator.db"
        self.queue = DurableQueue(self.db_path)
        self.store = AutomationStore(self.db_path)
        asyncio.run(self.queue.init())
        asyncio.run(self.store.init())
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY, objective TEXT NOT NULL, execution_mode TEXT NOT NULL)"
            )
            db.execute(
                "INSERT INTO tasks (id, objective, execution_mode) VALUES (?, ?, ?)",
                (7, "Verifier les sauvegardes", "docker"),
            )
            db.execute(
                "INSERT INTO tasks (id, objective, execution_mode) VALUES (?, ?, ?)",
                (8, "Tache locale", "local"),
            )
            db.commit()

        self.gateway = build_gateway()
        configure(self.store, self.queue, self.gateway)
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        self.tempdir.cleanup()

    def body(self, **changes):
        value = {
            "name": "Rapport quotidien",
            "job_kind": "run_task",
            "payload": {"task_id": 7, "notification_channel": "build_slack"},
            "schedule": {
                "type": "interval",
                "seconds": 3600,
                "anchor": "2026-07-11T08:00:00Z",
            },
            "idempotency_key": "ui:test-automation",
        }
        value.update(changes)
        return value

    def create(self, **changes):
        return self.client.post("/api/automations", json=self.body(**changes))

    def test_page_has_security_headers_and_uses_safe_dom_rendering(self):
        response = self.client.get("/automations")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertIn("frame-ancestors 'none'", response.headers["content-security-policy"])
        self.assertIn("textContent", response.text)
        self.assertIn("replaceChildren", response.text)
        self.assertNotIn("innerHTML", response.text)

    def test_channel_list_exposes_names_but_not_webhooks(self):
        response = self.client.get("/api/automations/channels")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"channels": ["build_slack"]})
        self.assertNotIn("server-secret", response.text)
        self.assertEqual(response.headers["cache-control"], "no-store")
        with self.assertRaises(TypeError):
            configure(
                self.store,
                self.queue,
                self.gateway,
                channel_names=["build_slack", "unconfigured_channel"],
            )

    def test_create_and_list_are_idempotent(self):
        first = self.create()
        self.assertEqual(first.status_code, 201, first.text)
        created = first.json()
        self.assertEqual(created["job_kind"], "run_task")
        self.assertEqual(
            created["payload"],
            {"task_id": 7, "notification_channel": "build_slack"},
        )
        self.assertEqual(created["state"], "enabled")
        self.assertTrue(created["next_run_at"].endswith("Z"))

        replay = self.create()
        self.assertEqual(replay.status_code, 201)
        self.assertEqual(replay.json()["id"], created["id"])

        listed = self.client.get("/api/automations")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(len(listed.json()["automations"]), 1)
        self.assertNotIn("idempotency_key", listed.text)

    def test_notification_is_optional(self):
        body_payload = {"task_id": 7}
        response = self.create(payload=body_payload)
        self.assertEqual(response.status_code, 201, response.text)
        self.assertEqual(response.json()["payload"], body_payload)

    def test_only_existing_task_and_fixed_job_kind_are_accepted(self):
        response = self.create(job_kind="shell_command")
        self.assertEqual(response.status_code, 400)

        response = self.create(payload={"task_id": 999})
        self.assertEqual(response.status_code, 404)

        response = self.create(payload={"task_id": True})
        self.assertEqual(response.status_code, 400)

        response = self.create(payload={"task_id": 8})
        self.assertEqual(response.status_code, 400)
        self.assertIn("Docker", response.text)

    def test_channels_and_extra_fields_fail_closed(self):
        response = self.create(
            payload={"task_id": 7, "notification_channel": "agent_webhook"}
        )
        self.assertEqual(response.status_code, 400)

        response = self.create(payload={"task_id": 7, "url": "https://evil.invalid"})
        self.assertEqual(response.status_code, 400)

        body = self.body()
        body["webhook"] = "https://evil.invalid"
        response = self.client.post("/api/automations", json=body)
        self.assertEqual(response.status_code, 400)

    def test_schedule_is_parsed_by_strict_utc_contract(self):
        daily = self.create(
            schedule={"type": "daily", "hour": 8, "minute": 30, "second": 0}
        )
        self.assertEqual(daily.status_code, 201, daily.text)
        self.assertEqual(daily.json()["schedule"]["type"], "daily")

        naive = self.body(
            schedule={
                "type": "interval",
                "seconds": 60,
                "anchor": "2026-07-11T08:00:00",
            },
            idempotency_key="ui:naive-date",
        )
        response = self.client.post("/api/automations", json=naive)
        self.assertEqual(response.status_code, 400)

        extra = self.body(
            schedule={"type": "daily", "hour": 8, "timezone": "Europe/Paris"},
            idempotency_key="ui:extra-schedule",
        )
        response = self.client.post("/api/automations", json=extra)
        self.assertEqual(response.status_code, 400)

    def test_pause_resume_and_cancel_routes(self):
        created = self.create().json()
        automation_id = created["id"]

        paused = self.client.post(f"/api/automations/{automation_id}/pause")
        self.assertEqual(paused.status_code, 200)
        self.assertEqual(paused.json()["state"], "paused")

        resumed = self.client.post(f"/api/automations/{automation_id}/resume")
        self.assertEqual(resumed.status_code, 200)
        self.assertEqual(resumed.json()["state"], "enabled")

        cancelled = self.client.post(f"/api/automations/{automation_id}/cancel")
        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(cancelled.json()["state"], "cancelled")

        cannot_resume = self.client.post(f"/api/automations/{automation_id}/resume")
        self.assertEqual(cannot_resume.status_code, 200)
        self.assertEqual(cannot_resume.json()["state"], "cancelled")

    def test_missing_or_malformed_automation_returns_not_found(self):
        malformed = self.client.post("/api/automations/not-an-id/pause")
        self.assertEqual(malformed.status_code, 404)
        missing = self.client.post("/api/automations/" + "a" * 32 + "/pause")
        self.assertEqual(missing.status_code, 404)

    def test_task_checker_errors_are_reduced_to_safe_service_error(self):
        async def unavailable(_task_id):
            raise RuntimeError("database password=server-secret")

        configure(self.store, self.queue, self.gateway, task_exists=unavailable)
        response = self.create()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("server-secret", response.text)

    def test_runtime_compatibility_accepts_server_channel_names_and_task_lookup(self):
        async def lookup(task_id):
            return {"id": task_id} if task_id == 7 else None

        configure(
            store=self.store,
            queue=self.queue,
            channel_names=("build_slack",),
            task_lookup=lookup,
        )
        channels = self.client.get("/api/automations/channels")
        self.assertEqual(channels.json(), {"channels": ["build_slack"]})
        response = self.create()
        self.assertEqual(response.status_code, 201, response.text)

    def test_request_body_is_bounded(self):
        response = self.client.post(
            "/api/automations",
            content=b"{" + b"x" * (33 * 1024) + b"}",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 413)

    def test_routes_fail_cleanly_before_configuration(self):
        previous = automation_api._services
        previous_error = automation_api._services_error
        automation_api._services = None
        automation_api._services_error = None
        try:
            response = self.client.get("/api/automations")
            self.assertEqual(response.status_code, 503)
        finally:
            automation_api._services = previous
            automation_api._services_error = previous_error

    def test_disable_exposes_the_runtime_failure_and_configure_recovers(self):
        automation_api.disable(
            "Le planificateur n'a pas pu redemarrer. Redemarrez Orchestrateur."
        )
        unavailable = self.client.get("/api/automations")
        self.assertEqual(unavailable.status_code, 503)
        self.assertIn("Redemarrez Orchestrateur", unavailable.json()["detail"])

        configure(self.store, self.queue, self.gateway)
        available = self.client.get("/api/automations")
        self.assertEqual(available.status_code, 200)


if __name__ == "__main__":
    unittest.main()
