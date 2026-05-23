"""
App Creator - Orchestrateur principal
Pipeline garanti : description -> code -> verification -> demarrage -> navigateur
"""
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

import team

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
DB_PATH  = Path(__file__).parent / "apps.db"
STATIC   = Path(__file__).parent / "static"
PROJECTS = Path(__file__).parent / "projects"
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

async def db_update(app_id, **kwargs):
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
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", encoding="utf-8", delete=False) as f:
        f.write(code)
        tmp = f.name
    try:
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", tmp],
            capture_output=True, text=True, timeout=10
        )
        return r.stderr.replace(tmp, "app.py") if r.returncode != 0 else None
    finally:
        try:
            os.unlink(tmp)
        except Exception:
            pass

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
            app_data = await generate_app_code(description, port, last_error)
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
                app_data = await generate_app_code(description, port, last_error)
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
    for f in app_data.get("files", []):
        fp = target / f["filename"]
        fp.write_text(f["content"], encoding="utf-8")
        files_written.append(f["filename"])
    yield evt("files", "Fichiers : " + ", ".join(files_written), files=files_written)

    # Etape 4 : Installation des dependances
    req = target / "requirements.txt"
    if req.exists():
        yield evt("step", "Installation des composants...")
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q",
                cwd=str(target),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            await proc.wait()
        except Exception as e:
            yield evt("step", "Avertissement installation : " + str(e))

    # Etape 5 : Demarrage du serveur
    yield evt("step", "Demarrage de votre application...")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "app.py",
            cwd=str(target),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        running_servers[app_id] = proc
    except Exception as e:
        await db_update(app_id, status="failed", error=str(e))
        yield evt("error", "Impossible de demarrer : " + str(e))
        return

    # Etape 6 : Attente que le serveur reponde
    yield evt("step", "Verification que tout fonctionne...")
    ok = await wait_for_server(port, timeout=25)

    if not ok:
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=3)
            err_msg = out.decode("utf-8", errors="replace")[:300]
        except Exception:
            err_msg = "Le serveur n'a pas repondu dans les delais."
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
    log.info("App Creator demarre.")
    yield
    for proc in running_servers.values():
        try:
            proc.terminate()
        except Exception:
            pass
    log.info("App Creator arrete.")

app = FastAPI(title="App Creator", lifespan=lifespan)
app.include_router(team.router)


# ── Routes ─────────────────────────────────────────────────────────────────────
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
    folder = str(PROJECTS / "app_{}".format(datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
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

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "app.py",
        cwd=a["folder"],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    running_servers[app_id] = proc
    ok = await wait_for_server(a["port"], timeout=20)
    if ok:
        await db_update(app_id, status="running")
        return {"status": "running", "url": "http://localhost:{}".format(a["port"])}
    else:
        await db_update(app_id, status="failed")
        return {"status": "failed"}

@app.post("/api/apps/{app_id}/stop")
async def stop_app(app_id: int):
    proc = running_servers.pop(app_id, None)
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    await db_update(app_id, status="stopped")
    return {"status": "stopped"}

@app.delete("/api/apps/{app_id}")
async def delete_app(app_id: int):
    await stop_app(app_id)
    a = await db_get(app_id)
    if a:
        import shutil
        shutil.rmtree(a["folder"], ignore_errors=True)
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

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
