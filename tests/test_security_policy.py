import os
import tempfile
import unittest
from pathlib import Path

from security_policy import (
    build_child_environment,
    is_allowed_host,
    is_unproxied_local_request,
    parse_allowed_hosts,
    resolve_allowed_target_path,
    validate_generated_app,
    validate_github_repo_url,
)


class GeneratedAppPolicyTests(unittest.TestCase):
    def test_accepts_only_the_expected_flask_bundle(self):
        result = validate_generated_app({
            "name": "Demo <script>",
            "files": [
                {"filename": "app.py", "content": "print('ok')\n"},
                {"filename": "requirements.txt", "content": "# runtime\nFlask\n"},
            ],
        })
        self.assertEqual(result["name"], "Demo script")
        self.assertEqual(result["files"][1]["content"], "flask\n")

    def test_rejects_traversal_absolute_and_unknown_files(self):
        bad_names = (
            "../orchestrator.py",
            "..\\orchestrator.py",
            "/tmp/app.py",
            "C:\\temp\\app.py",
            "\\\\server\\share\\app.py",
            "templates/index.html",
        )
        for filename in bad_names:
            with self.subTest(filename=filename), self.assertRaises(ValueError):
                validate_generated_app({
                    "files": [{"filename": filename, "content": "x"}],
                })

    def test_rejects_arbitrary_dependencies(self):
        with self.assertRaisesRegex(ValueError, "Flask"):
            validate_generated_app({
                "files": [
                    {"filename": "app.py", "content": "print('ok')"},
                    {"filename": "requirements.txt", "content": "flask\nrequests"},
                ],
            })

    def test_requires_the_declared_requirements_file(self):
        with self.assertRaisesRegex(ValueError, "requirements.txt"):
            validate_generated_app({
                "files": [{"filename": "app.py", "content": "print('ok')"}],
            })

    def test_rejects_duplicate_files(self):
        with self.assertRaisesRegex(ValueError, "double"):
            validate_generated_app({
                "files": [
                    {"filename": "app.py", "content": "one"},
                    {"filename": "app.py", "content": "two"},
                ],
            })


class ProcessEnvironmentTests(unittest.TestCase):
    def test_child_environment_drops_common_secrets(self):
        clean = build_child_environment({
            "PATH": "safe",
            "ANTHROPIC_API_KEY": "secret-a",
            "CUSTOM_TOKEN": "secret-b",
            "DATABASE_URL": "secret-c",
            "NORMAL_SETTING": "kept",
        })
        self.assertEqual(clean["PATH"], "safe")
        self.assertEqual(clean["NORMAL_SETTING"], "kept")
        self.assertNotIn("ANTHROPIC_API_KEY", clean)
        self.assertNotIn("CUSTOM_TOKEN", clean)
        self.assertNotIn("DATABASE_URL", clean)
        self.assertNotIn("PYTHONNOUSERSITE", clean)


class HttpBoundaryPolicyTests(unittest.TestCase):
    def test_local_mode_rejects_proxy_markers(self):
        self.assertTrue(is_unproxied_local_request("127.0.0.1", {}))
        self.assertTrue(is_unproxied_local_request("::1", {}))
        self.assertFalse(is_unproxied_local_request("192.168.1.20", {}))
        self.assertFalse(is_unproxied_local_request(
            "127.0.0.1", {"CF-Connecting-IP": "203.0.113.9"}
        ))

    def test_host_allowlist_blocks_dns_rebinding(self):
        allowed = parse_allowed_hosts(None)
        self.assertTrue(is_allowed_host("localhost:8000", allowed))
        self.assertTrue(is_allowed_host("127.0.0.1:8000", allowed))
        self.assertFalse(is_allowed_host("evil.example:8000", allowed))
        self.assertTrue(is_allowed_host(
            "demo.trycloudflare.com", parse_allowed_hosts("*.trycloudflare.com")
        ))
        with self.assertRaises(ValueError):
            parse_allowed_hosts("*")


class RepositoryTargetPolicyTests(unittest.TestCase):
    def test_local_repository_must_be_inside_an_explicit_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repos"
            repo = root / "demo"
            outside = Path(tmp) / "outside"
            (repo / ".git").mkdir(parents=True)
            (outside / ".git").mkdir(parents=True)

            self.assertEqual(
                resolve_allowed_target_path(str(repo), [root.resolve()]),
                repo.resolve(),
            )
            with self.assertRaisesRegex(ValueError, "hors"):
                resolve_allowed_target_path(str(outside), [root.resolve()])

    def test_local_repository_is_disabled_without_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "demo"
            (repo / ".git").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "desactives"):
                resolve_allowed_target_path(str(repo), [])

    def test_unc_path_is_rejected_before_resolution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "UNC"):
                resolve_allowed_target_path(r"\\server\share\repo", [root])

    @unittest.skipUnless(os.name == "nt", "test specifique aux volumes Windows")
    def test_other_windows_volume_is_rejected_before_resolution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            other_drive = "Z:" if root.drive.upper() != "Z:" else "Y:"
            with self.assertRaisesRegex(ValueError, "hors"):
                resolve_allowed_target_path(other_drive + r"\repo", [root])

    def test_github_clone_url_is_strict(self):
        self.assertEqual(
            validate_github_repo_url("https://github.com/openai/openai-python"),
            "https://github.com/openai/openai-python.git",
        )
        for url in (
            "https://evil.example/owner/repo",
            "https://github.com.evil.example/owner/repo",
            "https://token@github.com/owner/repo",
            "https://github.com/owner/repo?x=1",
            "http://github.com/owner/repo",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                validate_github_repo_url(url)


if __name__ == "__main__":
    unittest.main()
