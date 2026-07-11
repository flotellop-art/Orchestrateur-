"""
App Creator - Orchestrateur principal
Pipeline garanti : description -> code -> verification -> demarrage -> navigateur
"""
import asyncio
import importlib.util
import hmac
import json
import logging
import os
import re
import sys
import traceback
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app_paths import APP_ROOT, DATA_ROOT, DB_PATH, PROJECTS_ROOT, STATIC_ROOT
from app_version import __version__
import team
import api_control
import managed_agents  # pont vers les Agents geres Anthropic
import chat_agent
import automation_api
import skills_api
from auth_middleware import add_auth_middleware
from patches.security.cors_config import add_cors_middleware
from security_policy import build_child_environment, validate_generated_app

load_dotenv(APP_ROOT / ".env", override=False)
load_dotenv(DATA_ROOT / ".env", override=False)

# ── Configuration ─────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
STATIC   = STATIC_ROOT
PROJECTS = PROJECTS_ROOT
PROJECTS.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
running_servers = {}   # app_id -> asyncio.Process


# ── Base de donnees ────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS apps (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL DEFAULT 'app',
                description TEXT NOT NULL,
                folder      TEXT NOT NULL,
                status      TEXT DEFAULT 'creating',
                port        INTEGER DEFAULT 3000,
                created_at  TEXT,
                error       TEXT
            )
        """)
        await db.commit()
        await db.execute("UPDATE apps SET status='stopped' WHERE status IN ('running','creating')")
        await db.commit()

async def db_create(description, folder, port):
    async with aiosqlite.connect(DB_PATH) as db:
        name_val = "app-" + datetime.utcnow().strftime("%H%M%S")
        cur = await db.execute(
            "INSERT INTO apps (name,description,folder,status,port,created_at) VALUES (?,?,?,?,?,?)",
            (name_val, description, folder, "creating", port, datetime.utcnow().isoformat())
        )
        await db.commit()
        return cur.lastrowid

# Colonnes modifiables de `apps` : empeche l'injection d'un nom de colonne
# arbitraire via les kwargs (les noms ne sont jamais parametrables en SQL).
_APPS_COLUMNS_ALLOWED = frozenset({
    "name", "description", "folder", "status", "port", "error",
})

async def db_update(app_id, **kwargs):
    invalid = set(kwargs) - _APPS_COLUMNS_ALLOWED
    if invalid:
        raise ValueError("Colonnes non autorisees pour UPDATE apps : " + ", ".join(sorted(invalid)))
    sets = ", ".join(k + "=?" for k in kwargs)
    vals = list(kwargs.values()) + [app_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE apps SET " + sets + " WHERE id=?", vals)
        await db.commit()

async def db_get(app_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM apps WHERE id=?", (app_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

async def db_list():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM apps ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]

async def db_delete(app_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM apps WHERE id=?", (app_id,))
        await db.commit()


# ── Utilitaires ────────────────────────────────────────────────────────────────
async def find_free_port(start=3000):
    import socket
    for port in range(start, start + 100):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    return start

async def wait_for_server(port, timeout=25):
    """Attend que le serveur reponde sur le port. Retourne True si ok."""
    import http.client, time
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/")
            resp = conn.getresponse()
            conn.close()
            if resp.status < 500:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False

def verify_python_syntax(code):
    """Verifie la syntaxe Python. Retourne l'erreur ou None si OK."""
    try:
        compile(code, "app.py", "exec", dont_inherit=True)
        return None
    except (SyntaxError, ValueError, TypeError) as exc:
        return "".join(traceback.format_exception_only(type(exc), exc)).strip()

def repair_json(s):
    """
    Repare un JSON qui contient des retours a la ligne litteraux dans les strings.
    Exemple : {"content":"ligne1\nligne2"} -> {"content":"ligne1\\nligne2"}
    """
    result = []
    in_string = False
    i = 0
    while i < len(s):
        ch = s[i]
        # Sequence d'echappement : backslash suivi du prochain caractere
        if ch == "\\" and in_string:
            result.append(ch)
            i += 1
            if i < len(s):
                result.append(s[i])
            i += 1
            continue
        # Guillemet : toggle in_string
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            i += 1
            continue
        # Dans une string : echapper les caracteres de controle
        if in_string:
            if ch == "\n":
                result.append("\\n")
            elif ch == "\r":
                result.append("\\r")
            elif ch == "\t":
                result.append("\\t")
            elif ord(ch) < 32:
                result.append("\\u{:04x}".format(ord(ch)))
            else:
                result.append(ch)
        else:
            result.append(ch)
        i += 1
    return "".join(result)


# ── Prompt systeme ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
Tu es un generateur d'applications web Python/Flask.
Reponds UNIQUEMENT avec un objet JSON valide, sans aucun texte avant ou apres.

Structure JSON attendue :
{
  "name": "nom-de-l-app",
  "files": [
    {"filename": "app.py", "content": "...code Python complet..."},
    {"filename": "requirements.txt", "content": "flask"}
  ]
}

Regles absolues :
1. Reponds UNIQUEMENT avec le JSON, rien d'autre.
2. app.py doit ecouter sur le PORT indique dans le message.
3. app.py doit avoir une route "/" qui retourne du HTML.
4. L'HTML doit etre inline dans app.py (pas de fichiers separes).
5. requirements.txt doit contenir uniquement "flask".
6. N'utilise aucune API externe ni cle API.
7. Le code doit fonctionner tel quel, sans modification.
8. Cree une interface belle adaptee a la demande.
9. Dans les valeurs "content" du JSON, les sauts de ligne du code doivent etre representes par \\n (backslash + n), pas par de vrais retours a la ligne."""


# ── Generation du code via Claude ─────────────────────────────────────────────
async def generate_app_code(description, port, retry_error=""):
    """
    Appelle Claude et retourne un dict {name, files}.
    Essaie de reparer le JSON si necessaire.
    """
    msg_user = "Port : {}\nDescription : {}".format(port, description)
    if retry_error:
        msg_user += "\n\nERREUR A CORRIGER :\n{}\nCorrige le code et renvoie le JSON complet.".format(retry_error)

    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": msg_user}],
    )
    raw = response.content[0].text.strip()

    # Extraire le JSON entre le premier { et le dernier }
    start = raw.find("{")
    end   = raw.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("Pas de JSON dans la reponse : " + raw[:200])

    candidate = raw[start : end + 1]

    # Tentative 1 : json.loads direct
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass

    # Tentative 2 : reparer les retours a la ligne litteraux
    try:
        repaired = repair_json(candidate)
        return json.loads(repaired)
    except json.JSONDecodeError as e:
        raise ValueError("JSON invalide apres reparation : {} | Debut : {}".format(str(e), candidate[:150]))


# ── Pipeline de creation (SSE) ─────────────────────────────────────────────────
async def creation_pipeline(app_id, description, folder, port):
    """Generateur SSE : envoie des evenements JSON au frontend."""

    def evt(etype, msg, **extra):
        data = {"type": etype, "msg": msg}
        data.update(extra)
        return "data: {}\n\n".format(json.dumps(data, ensure_ascii=False))

    # Etape 1 : Generation du code
    yield evt("step", "Reflexion sur votre application...")

    app_data = None
    last_error = ""
    for attempt in range(3):
        try:
            if attempt > 0:
                yield evt("step", "Correction en cours (tentative {}/3)...".format(attempt + 1))
            else:
                yield evt("step", "Ecriture du code...")
            app_data = validate_generated_app(
                await generate_app_code(description, port, last_error)
            )
            break
        except Exception as e:
            last_error = str(e)
            log.warning("Tentative {} echouee : {}".format(attempt + 1, e))
            if attempt == 2:
                await db_update(app_id, status="failed", error=str(e))
                yield evt("error", "Impossible de generer le code : " + str(e))
                return

    # Extraire app.py
    app_code = ""
    for f in app_data.get("files", []):
        if f.get("filename") == "app.py":
            app_code = f.get("content", "")
            break

    if not app_code:
        await db_update(app_id, status="failed", error="app.py absent de la reponse")
        yield evt("error", "Le code genere ne contient pas app.py.")
        return

    # Etape 2 : Verification syntaxique (max 2 corrections)
    yield evt("step", "Verification du code...")
    for attempt in range(3):
        syntax_error = verify_python_syntax(app_code)
        if syntax_error is None:
            break
        if attempt < 2:
            yield evt("step", "Correction d'une erreur de syntaxe ({}/2)...".format(attempt + 1))
            last_error = "Erreur de syntaxe dans app.py :\n" + syntax_error
            try:
                app_data = validate_generated_app(
                    await generate_app_code(description, port, last_error)
                )
                for f in app_data.get("files", []):
                    if f.get("filename") == "app.py":
                        app_code = f.get("content", "")
                        break
            except Exception as e:
                await db_update(app_id, status="failed", error=str(e))
                yield evt("error", str(e))
                return
        else:
            await db_update(app_id, status="failed", error=syntax_error)
            yield evt("error", "Erreur de syntaxe non corrigee : " + syntax_error[:200])
            return

    # Etape 3 : Ecriture des fichiers
    yield evt("step", "Sauvegarde des fichiers...")
    target = Path(folder)
    target.mkdir(parents=True, exist_ok=True)
    files_written = []
    for f in app_data["files"]:
        fp = target / f["filename"]
        fp.write_text(f["content"], encoding="utf-8")
        files_written.append(f["filename"])
    yield evt("files", "Fichiers : " + ", ".join(files_written), files=files_written)

    # Etape 4 : dependance fixe, installee avec l'orchestrateur lui-meme.
    # Le contenu genere ne pilote plus jamais pip a l'execution.
    if importlib.util.find_spec("flask") is None:
        await db_update(app_id, status="failed", error="Flask absent de l'environnement")
        yield evt("error", "Flask est absent. Reinstallez requirements_orchestrator.txt.")
        return

    # Etape 5 : Demarrage du serveur
    yield evt("step", "Demarrage de votre application...")
    if not team.sandbox_app_network_allowed():
        message = (
            "Apercu dynamique desactive : Docker bridge autorise aussi les sorties "
            "reseau. Activez ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK uniquement "
            "si vous acceptez ce risque."
        )
        await db_update(app_id, status="stopped", error=message)
        yield evt("error", message)
        return
    try:
        handle = await team.get_sandbox_runtime().launch(
            "app-" + str(app_id),
            target,
            ("python", "app.py"),
            env={"PORT": str(port), "PYTHONUNBUFFERED": "1"},
            network_enabled=True,
            published_ports={port: port},
            lifetime_seconds=3600,
        )
        running_servers[app_id] = handle
    except Exception as e:
        await db_update(app_id, status="failed", error=str(e))
        yield evt("error", "Impossible de demarrer : " + str(e))
        return

    # Etape 6 : Attente que le serveur reponde
    yield evt("step", "Verification que tout fonctionne...")
    ok = await wait_for_server(port, timeout=25)

    if not ok:
        await team.get_sandbox_runtime().stop("app-" + str(app_id), strict=False)
        running_servers.pop(app_id, None)
        err_msg = "Le serveur isole n'a pas repondu dans les delais."
        await db_update(app_id, status="failed", error=err_msg)
        yield evt("error", "L'application n'a pas demarree. " + err_msg[:150])
        return

    # Succes
    name = app_data.get("name", "app-" + str(app_id))
    await db_update(app_id, status="running", name=name)
    url = "http://localhost:{}".format(port)
    yield evt("done", "Votre application est prete !", url=url, port=port, name=name)


# ── FastAPI ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    await init_db()
    await team.init_team_db()
    await api_control.init_control_db()
    import memory
    await memory.init_memory_db()
    await team.start_runtime_services()
    log.info("App Creator demarre.")
    try:
        yield
    finally:
        for app_id in list(running_servers):
            with suppress(Exception):
                await team.get_sandbox_runtime().stop(
                    "app-" + str(app_id), strict=False
                )
        running_servers.clear()
        await team.stop_runtime_services()
        log.info("App Creator arrete.")

app = FastAPI(title="Orchestrateur", lifespan=lifespan)
# Sans cle API, seuls les appels directs depuis la machine locale sont acceptes.
# Les tunnels et acces reseau exigent API_SECRET_KEY.
add_auth_middleware(app)
# CORS restreint a localhost (configurable via CORS_ALLOWED_ORIGINS) ; l'UI etant
# servie en meme origine, cela n'affecte pas son fonctionnement.
add_cors_middleware(app)
app.include_router(team.router)
# Centre de controle : endpoints d'agregation/monitoring/SSE (chemins nouveaux).
# Inclus APRES team.router : pour GET /api/tasks, la route de team (liste) reste
# prioritaire ; control.html sait lire ce format.
app.include_router(api_control.router)
app.include_router(managed_agents.router)  # GET /api/managed-agents + /environment
app.include_router(chat_agent.router)
app.include_router(automation_api.router)
app.include_router(skills_api.router)
import memory as _memory_mod
if _memory_mod.router is not None:
    app.include_router(_memory_mod.router)


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health_check(request: Request):
    # Endpoint public (jamais protege) : verif d'etat (Electron, supervision).
    expected_instance = os.getenv("ORCHESTRATOR_INSTANCE_TOKEN", "")
    supplied_instance = request.headers.get("X-Orchestrator-Instance", "")
    instance_verified = bool(
        expected_instance
        and supplied_instance
        and hmac.compare_digest(expected_instance, supplied_instance)
    )
    return {
        "status": "ok",
        "version": __version__,
        # Le jeton n'est jamais renvoye au navigateur. Electron prouve qu'il
        # connait le secret ephemere fourni au processus enfant.
        "instance": instance_verified,
    }


@app.post("/api/runtime/prepare-shutdown")
async def prepare_runtime_shutdown(request: Request):
    """Libere baux et conteneurs avant l'arret force du processus Windows."""
    expected = os.getenv("ORCHESTRATOR_INSTANCE_TOKEN", "")
    supplied = request.headers.get("X-Orchestrator-Instance", "")
    if not expected or not supplied or not hmac.compare_digest(expected, supplied):
        raise HTTPException(404, "Endpoint indisponible")
    for app_id in list(running_servers):
        with suppress(Exception):
            await team.get_sandbox_runtime().stop("app-" + str(app_id), strict=False)
    running_servers.clear()
    await team.stop_runtime_services()
    return {"status": "ready"}


@app.get("/api/version")
async def version_info():
    return {"version": __version__, "stage": "beta"}

@app.get("/api/stats")
async def stats():
    apps = await db_list()
    return {
        "total":   len(apps),
        "running": sum(1 for a in apps if a["status"] == "running"),
        "stopped": sum(1 for a in apps if a["status"] == "stopped"),
        "failed":  sum(1 for a in apps if a["status"] == "failed"),
    }

@app.get("/api/apps")
async def list_apps():
    return await db_list()

@app.post("/api/create")
async def create_app(request: Request):
    body = await request.json()
    description = body.get("description", "").strip()
    if not description:
        raise HTTPException(400, "Description manquante")

    port   = await find_free_port(3000)
    folder = str(PROJECTS / "app_{}_{}".format(
        datetime.utcnow().strftime("%Y%m%d_%H%M%S"), uuid.uuid4().hex[:8]
    ))
    app_id = await db_create(description, folder, port)

    async def stream():
        async for chunk in creation_pipeline(app_id, description, folder, port):
            yield chunk

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.post("/api/apps/{app_id}/start")
async def start_app(app_id: int):
    a = await db_get(app_id)
    if not a:
        raise HTTPException(404, "Application introuvable")
    if a["status"] == "running" and app_id in running_servers:
        return {"status": "already_running", "url": "http://localhost:{}".format(a["port"])}
    if not (Path(a["folder"]) / "app.py").exists():
        raise HTTPException(400, "Fichiers introuvables")
    if not team.sandbox_app_network_allowed():
        raise HTTPException(
            409,
            "Apercu dynamique desactive tant qu'un reseau Docker interne "
            "sans sortie Internet n'est pas configure.",
        )

    try:
        handle = await team.get_sandbox_runtime().launch(
            "app-" + str(app_id),
            a["folder"],
            ("python", "app.py"),
            env={"PORT": str(a["port"]), "PYTHONUNBUFFERED": "1"},
            network_enabled=True,
            published_ports={int(a["port"]): int(a["port"])},
            lifetime_seconds=3600,
        )
    except Exception as exc:
        raise HTTPException(409, "Lancement isole refuse : " + str(exc)[:500])
    running_servers[app_id] = handle
    ok = await wait_for_server(a["port"], timeout=20)
    if ok:
        await db_update(app_id, status="running")
        return {"status": "running", "url": "http://localhost:{}".format(a["port"])}
    else:
        runtime = team.get_sandbox_runtime()
        cleanup = await runtime.stop("app-" + str(app_id), strict=False)
        running_servers.pop(app_id, None)
        await db_update(app_id, status="failed")
        if cleanup.failed:
            runtime.block_execution(
                "Le conteneur de l'application n'a pas pu etre nettoye."
            )
            raise HTTPException(500, "Le conteneur de l'application a resiste au nettoyage.")
        return {"status": "failed"}

@app.post("/api/apps/{app_id}/stop")
async def stop_app(app_id: int):
    app_record = await db_get(app_id)
    if not app_record:
        raise HTTPException(404, "Application introuvable")
    running_servers.pop(app_id, None)
    if app_record.get("status") != "running":
        await db_update(app_id, status="stopped")
        return {"status": "stopped"}
    runtime = team.get_sandbox_runtime()
    try:
        cleanup = await runtime.stop("app-" + str(app_id), strict=False)
    except team.SandboxError as exc:
        runtime.block_execution("Le nettoyage Docker de l'application est incomplet.")
        raise HTTPException(503, str(exc)[:500]) from None
    await db_update(app_id, status="stopped")
    if cleanup.failed:
        runtime.block_execution("Le nettoyage Docker de l'application est incomplet.")
        raise HTTPException(500, "Le conteneur de l'application a resiste au nettoyage.")
    return {"status": "stopped"}

@app.delete("/api/apps/{app_id}")
async def delete_app(app_id: int):
    await stop_app(app_id)
    a = await db_get(app_id)
    if a:
        folder = Path(a["folder"]).resolve(strict=False)
        projects_root = PROJECTS.resolve()
        if folder != projects_root and projects_root in folder.parents:
            if not await asyncio.to_thread(team._rmtree_force, folder):
                raise HTTPException(500, "Le dossier de l'application n'a pas pu etre supprime.")
        else:
            log.error("Suppression du dossier d'application refusee : %s", folder)
            raise HTTPException(400, "Dossier d'application invalide ; suppression refusee.")
    await db_delete(app_id)
    return {"deleted": True}

@app.get("/")
async def index():
    content = (STATIC / "index.html").read_bytes()
    return Response(
        content=content,
        media_type="text/html",
        headers={"Cache-Control": "no-store"},
    )

@app.get("/control")
async def control_center():
    # URL conviviale pour le centre de controle (= /static/control.html).
    content = (STATIC / "control.html").read_bytes()
    return Response(
        content=content,
        media_type="text/html",
        headers={"Cache-Control": "no-store"},
    )

@app.get("/memory")
async def memory_page():
    # URL conviviale pour la page de gestion des notes (= /static/memory.html).
    content = (STATIC / "memory.html").read_bytes()
    return Response(
        content=content,
        media_type="text/html",
        headers={"Cache-Control": "no-store"},
    )

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=os.getenv("ORCHESTRATOR_HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
