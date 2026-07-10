import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StaticSecurityRegressionTests(unittest.TestCase):
    def test_master_key_is_not_put_in_sse_urls(self):
        files = (
            ROOT / "auth_middleware.py",
            ROOT / "api_control.py",
            ROOT / "static" / "workspace.html",
            ROOT / "static" / "control.html",
        )
        combined = "\n".join(path.read_text(encoding="utf-8") for path in files)
        self.assertNotIn("?api_key=", combined)
        self.assertNotIn("query_params.get(\"api_key\"", combined)

    def test_chat_escapes_content_before_markdown_formatting(self):
        chat = (ROOT / "static" / "chat.html").read_text(encoding="utf-8")
        self.assertIn("return escapeHtml(content)", chat)
        self.assertNotIn("content.replace(/\\n/g", chat)
        self.assertNotIn("cd.innerHTML = `<strong>Action requise", chat)

    def test_dashboard_reuses_the_configured_api_key(self):
        dashboard = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn("localStorage.getItem('ORCH_API_KEY')", dashboard)
        self.assertIn("headers.set('X-API-Key', key)", dashboard)
        self.assertIn("requestUrl.origin === window.location.origin", dashboard)

    def test_electron_uses_the_public_health_probe(self):
        main = (ROOT / "electron-app" / "main.js").read_text(encoding="utf-8")
        self.assertIn("BASE+'/health'", main)
        self.assertNotIn("BASE+'/api/stats'", main)

    def test_desktop_package_does_not_embed_runtime_secrets(self):
        package = (ROOT / "electron-app" / "package.json").read_text(encoding="utf-8")
        self.assertNotIn('".env",', package)
        self.assertNotIn('"*.db"', package)
        self.assertIn('"security_policy.py"', package)

    def test_tunnel_launcher_requires_a_strong_key_and_remote_host(self):
        launcher = (ROOT / "start_tunnel.bat").read_text(encoding="utf-8")
        self.assertIn("$key.Length -lt 24", launcher)
        self.assertIn("$remote.Count -eq 0", launcher)

    def test_git_token_is_not_embedded_in_the_clone_url(self):
        team = (ROOT / "team.py").read_text(encoding="utf-8")
        self.assertNotIn('"https://" + token + "@"', team)
        self.assertIn('"GIT_CONFIG_VALUE_0"', team)


if __name__ == "__main__":
    unittest.main()
