import os
import unittest
from unittest.mock import patch

from fastapi import FastAPI

from auth_middleware import APIKeyAuthMiddleware, _rate_counters, _rate_ok, add_auth_middleware


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


async def _html_app(scope, receive, send):
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/html; charset=utf-8")],
    })
    await send({"type": "http.response.body", "body": b"<p>ok</p>"})


async def _strict_html_app(scope, receive, send):
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            (b"content-type", b"text/html"),
            (b"cache-control", b"private, no-store"),
            (b"content-security-policy", b"sandbox; default-src 'none'"),
            (b"referrer-policy", b"no-referrer"),
            (b"x-content-type-options", b"nosniff"),
            (b"x-frame-options", b"DENY"),
        ],
    })
    await send({"type": "http.response.body", "body": b"<p>isolated</p>"})


async def call_middleware(*, secret="", client="127.0.0.1", host="localhost", extra_headers=None,
                          query=b"", path="/api/tasks", method="POST", app=_ok_app,
                          include_headers=False):
    headers = [(b"host", host.encode("ascii"))]
    for key, value in (extra_headers or {}).items():
        headers.append((key.lower().encode("ascii"), value.encode("ascii")))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query,
        "headers": headers,
        "client": (client, 50000),
        "server": ("127.0.0.1", 8000),
    }
    request_sent = False

    async def receive():
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    messages = []

    async def send(message):
        messages.append(message)

    middleware = APIKeyAuthMiddleware(
        app,
        api_secret=secret,
        allowed_hosts=("localhost", "127.0.0.1", "::1", "agent.example"),
    )
    await middleware(scope, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    if include_headers:
        response_headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in start["headers"]
        }
        return start["status"], response_headers
    return start["status"]


class AuthMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _rate_counters.clear()

    async def test_no_key_allows_only_direct_loopback(self):
        self.assertEqual(await call_middleware(), 200)
        self.assertEqual(await call_middleware(client="192.168.1.10"), 403)
        self.assertEqual(await call_middleware(
            extra_headers={"CF-Connecting-IP": "203.0.113.5"}
        ), 403)

    async def test_bad_host_is_rejected_even_from_loopback(self):
        self.assertEqual(await call_middleware(host="evil.example"), 400)

    async def test_browser_mutations_require_the_same_origin(self):
        self.assertEqual(await call_middleware(extra_headers={
            "Origin": "http://127.0.0.1:5000",
            "Sec-Fetch-Site": "same-site",
        }), 403)
        self.assertEqual(await call_middleware(extra_headers={
            "Origin": "http://localhost",
            "Sec-Fetch-Site": "same-origin",
        }), 200)
        self.assertEqual(await call_middleware(extra_headers={
            "Origin": "null",
            "Sec-Fetch-Site": "cross-site",
        }), 403)

    async def test_configured_key_must_be_in_a_header(self):
        self.assertEqual(await call_middleware(
            secret="correct-key", query=b"api_key=correct-key"
        ), 401)
        self.assertEqual(await call_middleware(
            secret="correct-key", extra_headers={"X-API-Key": "correct-key"}
        ), 200)
        self.assertEqual(await call_middleware(
            secret="correct-key", extra_headers={"Authorization": "Bearer correct-key"}
        ), 200)

    async def test_invalid_attempts_do_not_spend_authenticated_quota(self):
        with patch("auth_middleware.RATE_LIMIT_REQUESTS", 1):
            self.assertEqual(await call_middleware(
                secret="correct-key", client="198.51.100.20",
                extra_headers={"X-API-Key": "wrong-key"},
            ), 403)
            self.assertEqual(await call_middleware(
                secret="correct-key", client="198.51.100.20",
                extra_headers={"X-API-Key": "another-wrong-key"},
            ), 429)
            self.assertEqual(await call_middleware(
                secret="correct-key", client="198.51.100.20",
                extra_headers={"X-API-Key": "correct-key"},
            ), 200)
            self.assertEqual(await call_middleware(
                secret="correct-key", client="198.51.100.20",
                extra_headers={"X-API-Key": "correct-key"},
            ), 429)

    async def test_public_shells_do_not_make_their_apis_public(self):
        for path in ("/chat", "/memory", "/skills", "/automations"):
            with self.subTest(path=path):
                self.assertEqual(await call_middleware(
                    client="198.51.100.20", path=path, method="GET"
                ), 200)
        self.assertEqual(await call_middleware(
            client="198.51.100.20", path="/api/chat", method="GET"
        ), 403)
        self.assertEqual(await call_middleware(
            client="198.51.100.20", path="/api/memory/status", method="GET"
        ), 403)
        self.assertEqual(await call_middleware(
            client="198.51.100.20", path="/api/skills", method="GET"
        ), 403)
        self.assertEqual(await call_middleware(
            client="198.51.100.20", path="/api/automations", method="GET"
        ), 403)

    async def test_static_html_receives_the_common_browser_security_headers(self):
        status, headers = await call_middleware(
            client="198.51.100.20",
            path="/static/control.html",
            method="GET",
            app=_html_app,
            include_headers=True,
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["referrer-policy"], "no-referrer")
        self.assertEqual(headers["x-content-type-options"], "nosniff")
        self.assertEqual(headers["x-frame-options"], "DENY")
        csp = headers["content-security-policy"]
        for directive in (
            "default-src 'self'",
            "connect-src 'self'",
            "img-src 'self' data: blob:",
            "style-src 'self' 'unsafe-inline'",
            "script-src 'self' 'unsafe-inline'",
            "object-src 'none'",
            "base-uri 'none'",
            "frame-ancestors 'none'",
            "form-action 'self'",
        ):
            with self.subTest(directive=directive):
                self.assertIn(directive, csp)

    async def test_common_headers_do_not_weaken_a_stricter_html_policy(self):
        status, headers = await call_middleware(
            client="198.51.100.20",
            path="/static/generated.html",
            method="GET",
            app=_strict_html_app,
            include_headers=True,
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["cache-control"], "private, no-store")
        self.assertEqual(
            headers["content-security-policy"], "sandbox; default-src 'none'"
        )

    def test_rate_limit_cache_stays_bounded(self):
        with patch("auth_middleware.RATE_LIMIT_MAX_CLIENTS", 3):
            for number in range(10):
                self.assertTrue(_rate_ok("198.51.100." + str(number), now=1.0))
        self.assertLessEqual(len(_rate_counters), 3)

    def test_configured_master_key_has_a_minimum_length(self):
        with patch.dict(os.environ, {"API_SECRET_KEY": "too-short"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "trop courte"):
                add_auth_middleware(FastAPI())


if __name__ == "__main__":
    unittest.main()
