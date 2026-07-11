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

    def test_chat_does_not_swallow_server_error_events(self):
        chat = (ROOT / "static" / "chat.html").read_text(encoding="utf-8")
        control = (ROOT / "static" / "control.html").read_text(encoding="utf-8")
        self.assertIn("throw new Error(evt.message || 'Erreur du chat')", chat)
        self.assertIn("e && e.message ? e.message", chat)
        self.assertNotIn("} catch(e) {}\n      }\n    }\n  } catch(e)", chat)
        self.assertIn("OrchestratorChatStream.parsePayload(raw)", control)
        self.assertNotIn("}catch(_){ acc += raw", control)

    def test_dashboard_reuses_the_session_api_key(self):
        dashboard = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn("sessionStorage.getItem('ORCH_API_KEY')", dashboard)
        self.assertIn("headers.set('X-API-Key', key)", dashboard)
        self.assertIn("requestUrl.origin === window.location.origin", dashboard)

    def test_workspace_file_names_are_bound_as_text_not_inline_javascript(self):
        workspace = (ROOT / "static" / "workspace.html").read_text(encoding="utf-8")
        self.assertIn("item.textContent = path", workspace)
        self.assertIn("item.addEventListener('click'", workspace)
        self.assertNotIn('onclick="viewFile(\\\'', workspace)
        self.assertNotIn("esc(f).replace(/'/g", workspace)

    def test_api_key_is_never_persisted_in_local_storage(self):
        files = [ROOT / "api_control.py", *sorted((ROOT / "static").glob("*"))]
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in files
            if path.is_file() and path.suffix in {".html", ".js", ".py"}
        )
        self.assertNotIn("localStorage.getItem('ORCH_API_KEY')", text)
        self.assertNotIn("localStorage.setItem('ORCH_API_KEY'", text)
        self.assertIn("sessionStorage.getItem('ORCH_API_KEY')", text)
        cleanup = (ROOT / "static" / "session-auth.js").read_text(encoding="utf-8")
        self.assertIn("localStorage.removeItem('ORCH_API_KEY')", cleanup)
        self.assertNotIn("sessionStorage.setItem", cleanup)

        key_pages = (
            "automations.html", "chat.html", "control.html", "index.html",
            "memory.html", "skills.html", "workspace.html",
        )
        for name in key_pages:
            with self.subTest(name=name):
                page = (ROOT / "static" / name).read_text(encoding="utf-8")
                self.assertIn('/static/session-auth.js', page)

    def test_electron_uses_the_public_health_probe(self):
        main = (ROOT / "electron-app" / "main.js").read_text(encoding="utf-8")
        self.assertIn("BASE+'/health'", main)
        self.assertNotIn("BASE+'/api/stats'", main)
        self.assertIn("'X-Orchestrator-Instance':INSTANCE_TOKEN", main)
        self.assertIn("data.instance===true", main)

    def test_electron_releases_runtime_before_killing_windows_backend(self):
        main = (ROOT / "electron-app" / "main.js").read_text(encoding="utf-8")
        self.assertIn("/api/runtime/prepare-shutdown", main)
        self.assertIn("resp.statusCode>=200&&resp.statusCode<300", main)
        self.assertIn("req.on('error',()=>resolve(false))", main)
        self.assertIn("req.destroy();resolve(false)", main)
        guard = "if(!prepared&&!force){stopPromise=null;return false;}"
        self.assertIn(guard, main)
        self.assertLess(main.index("await prepareBackendShutdown()"), main.index(guard))
        self.assertLess(main.index(guard), main.index("child.kill('SIGTERM')"))
        self.assertNotIn("setInterval(async", main)
        self.assertIn("while(Date.now()<deadline)", main)
        self.assertIn("srv.on('error'", main)
        self.assertIn("closed=await waitForChildClose(child,5000)", main)
        self.assertIn("closed=await waitForChildClose(child,3000)", main)
        self.assertIn("stopPromise=null;\n      return false", main)
        self.assertLess(main.index("const stopped=!srv||await stopServer({force:true})"),
                        main.index("relaunching=true;app.relaunch()"))

    def test_packaged_backend_contains_claude_sdk_binary_data(self):
        spec = (ROOT / "orchestrator.spec").read_text(encoding="utf-8")
        self.assertIn('collect_data_files(', spec)
        self.assertIn('includes=["_bundled/*"]', spec)

    def test_generated_pages_keep_their_stricter_sandbox_csp(self):
        team = (ROOT / "team.py").read_text(encoding="utf-8")
        hardening = (ROOT / "electron-app" / "electron_hardening.js").read_text(encoding="utf-8")
        self.assertIn("sandbox allow-scripts; default-src 'none'", team)
        self.assertIn("const existingCsp", hardening)
        self.assertIn("if (!existingCsp || existingCsp.length === 0)", hardening)
        self.assertIn("const existingReferrerPolicy", hardening)
        self.assertIn("['no-referrer']", hardening)
        self.assertNotIn("strict-origin-when-cross-origin", hardening)

    def test_general_chat_cannot_execute_host_commands(self):
        chat_agent = (ROOT / "chat_agent.py").read_text(encoding="utf-8")
        self.assertNotIn('{"name": "run_command"', chat_agent)
        self.assertNotIn("safe_run_command_sync", chat_agent)

    def test_desktop_package_does_not_embed_runtime_secrets(self):
        package = (ROOT / "electron-app" / "package.json").read_text(encoding="utf-8")
        self.assertNotIn('".env",', package)
        self.assertNotIn('"*.db"', package)
        self.assertIn('"backend/orchestrator-backend/"', package)
        self.assertNotIn('"security_policy.py"', package)
        self.assertNotIn('"install_permissions.py"', package)

    def test_desktop_uses_the_packaged_backend_and_user_data(self):
        main = (ROOT / "electron-app" / "main.js").read_text(encoding="utf-8")
        self.assertIn("ORCHESTRATOR_DATA_DIR", main)
        self.assertIn("orchestrator-backend.exe", main)
        self.assertIn("windowsHide:true", main)

    def test_versions_are_synchronized_and_dependencies_are_pinned(self):
        version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
        package = (ROOT / "electron-app" / "package.json").read_text(encoding="utf-8")
        requirements = (ROOT / "requirements_orchestrator.txt").read_text(encoding="utf-8")
        self.assertIn('"version": "' + version + '"', package)
        for line in requirements.splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                self.assertIn("==", line)

    def test_tunnel_launcher_requires_a_strong_key_and_remote_host(self):
        launcher = (ROOT / "start_tunnel.bat").read_text(encoding="utf-8")
        self.assertIn("$key.Length -lt 24", launcher)
        self.assertIn("$remote.Count -eq 0", launcher)

    def test_git_token_is_not_embedded_in_the_clone_url(self):
        team = (ROOT / "team.py").read_text(encoding="utf-8")
        self.assertNotIn('"https://" + token + "@"', team)
        self.assertIn('"GIT_CONFIG_VALUE_0"', team)

    def test_workspace_has_real_tiered_install_decisions(self):
        workspace = (ROOT / "static" / "workspace.html").read_text(encoding="utf-8")
        self.assertIn('id="install-policy"', workspace)
        for value in ("blocked", "ask", "project", "user", "admin"):
            self.assertIn('value="' + value + '"', workspace)
        self.assertIn("/install-requests/", workspace)
        self.assertIn("allow_once", workspace)
        self.assertNotIn("allow_task", workspace)
        self.assertIn("textContent = 'Pourquoi", workspace)
        self.assertIn("sessionStorage.setItem(activeTaskStorageKey()", workspace)
        self.assertIn("install_policy: document.getElementById('install-policy').value", workspace)
        self.assertIn("after_id=", workspace)
        self.assertIn("failedSource.close()", workspace)
        self.assertIn("if (taskId && evtSource === failedSource) connectStream()", workspace)


if __name__ == "__main__":
    unittest.main()
