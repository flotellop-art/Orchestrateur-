"""
Espace de travail collaboratif multi-agents.

L'utilisateur fixe un objectif ; un agent "chef" autonome construit et pilote
son equipe (creation dynamique d'agents, delegation), les agents agissent
(recherche web, fichiers, commandes, tests) dans un dossier de travail dedie,
et la boucle tourne jusqu'a l'objectif ou un plafond d'iterations.

Reutilise les patterns de orchestrator.py (FastAPI, SSE, aiosqlite) et de
chat_agent.py (boucle d'agent + outils). Branche dans orchestrator.py via
include_router.
"""
import asyncio
import json
import logging
import os
import re
import shlex
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiosqlite
import anthropic
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

load_dotenv(Path(__file__).parent / ".env", override=False)
log = logging.getLogger(__name__)

# Configuration
DB_PATH = Path(__file__).parent / "apps.db"
STATIC = Path(__file__).parent / "static"
PROJECTS = Path(__file__).parent / "projects"
PROJECTS.mkdir(exist_ok=True)

# Modeles par defaut par provider (verifies via recherche web, mai 2026).
# Gemini 3.5 Pro n'est pas encore dispo (prevu juin 2026) -> tier Flash, stable.
# Un identifiant errone n'est pas bloquant : le provider retombe sur Claude (voir call_model).
DEFAULT_CLAUDE = "claude-opus-4-7"
DEFAULT_GEMINI = "gemini-3.5-flash"
DEFAULT_OPENAI = "gpt-5.5"
STABLE_CLAUDE = "claude-sonnet-4-6"  # repli eprouve si le modele Claude demande echoue


def _default_model(provider: str) -> str:
    return {"gemini": DEFAULT_GEMINI, "openai": DEFAULT_OPENAI}.get(provider, DEFAULT_CLAUDE)


MAX_AGENTS_CEILING = 8
MAX_ITER_CEILING = 40
WORKER_MAX_STEPS = 6
MAX_TOKENS = 16000  # sortie max par appel : assez large pour ecrire des fichiers entiers

COMMAND_WHITELIST = {
    "python", "python3", "pip", "pip3", "pytest",
    "ls", "cat", "echo", "pwd", "head", "tail", "wc",
    "node", "npm",
}

_client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# task_id -> {"task": asyncio.Task, "pause": asyncio.Event, "step": bool, "inbox": list}
running_tasks: dict[int, dict] = {}
# task_id -> {"proc": Process, "url": str|None, "output": list}
launched_apps: dict[int, dict] = {}

NL = chr(10)


def sse(data: dict) -> str:
    return "data: " + json.dumps(data, ensure_ascii=False) + NL + NL


# ── Base de donnees ─────────────────────────────────────────────────────────
async def init_team_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                objective     TEXT NOT NULL,
                folder        TEXT NOT NULL,
                status        TEXT DEFAULT 'idle',
                iteration     INTEGER DEFAULT 0,
                max_iterations INTEGER DEFAULT 15,
                max_agents    INTEGER DEFAULT 4,
                web_enabled   INTEGER DEFAULT 0,
                chef_model    TEXT DEFAULT 'claude-sonnet-4-6',
                created_at    TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS task_agents (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                name       TEXT NOT NULL,
                role       TEXT DEFAULT '',
                provider   TEXT DEFAULT 'claude',
                model      TEXT DEFAULT '',
                created_by TEXT DEFAULT 'chef',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL,
                iteration  INTEGER DEFAULT 0,
                agent      TEXT DEFAULT '',
                kind       TEXT DEFAULT 'info',
                content    TEXT DEFAULT '',
                created_at TEXT
            )
        """)
        # Les taches en cours ne survivent pas a un redemarrage du serveur.
        await db.execute("UPDATE tasks SET status='stopped' WHERE status IN ('running','paused')")
        await db.commit()


async def db_create_task(objective, folder, max_iterations, max_agents, web_enabled, chef_model):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO tasks (objective,folder,status,max_iterations,max_agents,web_enabled,chef_model,created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (objective, folder, "idle", max_iterations, max_agents,
             1 if web_enabled else 0, chef_model, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


async def db_get_task(task_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def db_list_tasks():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_update_task(task_id, **kwargs):
    sets = ", ".join(k + "=?" for k in kwargs)
    vals = list(kwargs.values()) + [task_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET " + sets + " WHERE id=?", vals)
        await db.commit()


async def db_delete_task(task_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        await db.execute("DELETE FROM task_agents WHERE task_id=?", (task_id,))
        await db.execute("DELETE FROM messages WHERE task_id=?", (task_id,))
        await db.commit()


async def db_add_agent(task_id, name, role, provider, model, created_by):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO task_agents (task_id,name,role,provider,model,created_by,created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (task_id, name, role, provider, model, created_by, datetime.utcnow().isoformat()),
        )
        await db.commit()


async def db_list_agents(task_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM task_agents WHERE task_id=? ORDER BY id", (task_id,)) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_get_agent(task_id, name):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM task_agents WHERE task_id=? AND name=?", (task_id, name)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def emit(task_id, iteration, agent, kind, payload: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO messages (task_id,iteration,agent,kind,content,created_at) VALUES (?,?,?,?,?,?)",
            (task_id, iteration, agent, kind, json.dumps(payload, ensure_ascii=False),
             datetime.utcnow().isoformat()),
        )
        await db.commit()


async def db_messages_after(task_id, after_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM messages WHERE task_id=? AND id>? ORDER BY id", (task_id, after_id)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ── Dispatch modele (Claude + Gemini) ───────────────────────────────────────
def _gemini_available() -> bool:
    return bool(os.getenv("GEMINI_API_KEY"))


async def _claude_request(model, system, messages) -> str:
    resp = await _client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        messages=messages,
    )
    return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")


async def _call_claude(model, system, messages, on_notice=None) -> str:
    target = model or DEFAULT_CLAUDE
    try:
        return await _claude_request(target, system, messages)
    except Exception as e:
        if target == STABLE_CLAUDE:
            raise  # le modele de repli a aussi echoue : on remonte l'erreur
        log.warning("[modele] Claude %s erreur: %s -> repli %s", target, e, STABLE_CLAUDE)
        if on_notice:
            await on_notice("Erreur Claude " + target + " (" + str(e)[:100]
                            + ") -> repli sur " + STABLE_CLAUDE + ".")
        return await _claude_request(STABLE_CLAUDE, system, messages)


async def _call_gemini(model, system, messages) -> str:
    from google import genai  # nouveau paquet google-genai, importe a la demande
    from google.genai import types
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    contents = [
        {"role": ("user" if m["role"] == "user" else "model"),
         "parts": [{"text": m["content"]}]}
        for m in messages
    ]
    resp = await client.aio.models.generate_content(
        model=model or DEFAULT_GEMINI,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=MAX_TOKENS,
        ),
    )
    return resp.text or ""


def _openai_available() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


async def _call_openai(model, system, messages) -> str:
    from openai import AsyncOpenAI  # importe a la demande
    oai = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    msgs = [{"role": "system", "content": system}]
    for m in messages:
        role = m["role"] if m["role"] in ("user", "assistant") else "user"
        msgs.append({"role": role, "content": m["content"]})
    resp = await oai.chat.completions.create(model=model or DEFAULT_OPENAI, messages=msgs)
    return resp.choices[0].message.content or ""


async def call_model(provider, model, system, messages, on_notice=None) -> str:
    log.info("[modele] appel %s (%s)", provider, model or _default_model(provider))
    if provider == "gemini":
        if not _gemini_available():
            if on_notice:
                await on_notice("Gemini indisponible (GEMINI_API_KEY manquante) -> repli sur Claude.")
            return await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice)
        try:
            out = await _call_gemini(model, system, messages)
            log.info("[modele] Gemini OK (%d caracteres)", len(out))
            return out
        except Exception as e:
            log.warning("[modele] Gemini erreur: %s", e)
            if on_notice:
                await on_notice("Erreur Gemini (" + str(e)[:120] + ") -> repli sur Claude.")
            return await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice)
    if provider == "openai":
        if not _openai_available():
            if on_notice:
                await on_notice("OpenAI indisponible (OPENAI_API_KEY manquante) -> repli sur Claude.")
            return await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice)
        try:
            out = await _call_openai(model, system, messages)
            log.info("[modele] OpenAI OK (%d caracteres)", len(out))
            return out
        except Exception as e:
            log.warning("[modele] OpenAI erreur: %s", e)
            if on_notice:
                await on_notice("Erreur OpenAI (" + str(e)[:120] + ") -> repli sur Claude.")
            return await _call_claude(DEFAULT_CLAUDE, system, messages, on_notice)
    return await _call_claude(model or DEFAULT_CLAUDE, system, messages, on_notice)


# ── Extraction JSON (calque de repair_json d'orchestrator.py) ────────────────
def _repair_json(s: str) -> str:
    result = []
    in_string = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "\\" and in_string:
            result.append(ch)
            i += 1
            if i < len(s):
                result.append(s[i])
            i += 1
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            i += 1
            continue
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


def _extract_json(text: str) -> Optional[dict]:
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = text[start:end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(_repair_json(candidate))
    except json.JSONDecodeError:
        return None


# ── Outils (cadres au dossier de la tache) ──────────────────────────────────
def _safe_path(folder: str, rel: str) -> Path:
    base = Path(folder).resolve()
    p = (base / (rel or "")).resolve()
    if p != base and base not in p.parents:
        raise ValueError("Chemin hors du dossier de travail: " + str(rel))
    return p


def _list_files(folder: str) -> list[str]:
    base = Path(folder)
    if not base.exists():
        return []
    out = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            out.append(str(p.relative_to(base)))
    return out[:200]


async def _run_cmd(folder: str, cmd: str, timeout: int = 60) -> str:
    try:
        parts = shlex.split(cmd or "")
    except ValueError:
        return "Commande invalide (guillemets non fermes)."
    if not parts:
        return "Commande vide."
    if parts[0] not in COMMAND_WHITELIST:
        return "Commande refusee (hors liste blanche): " + parts[0]
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *parts, cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (out.decode("utf-8", errors="replace") or "(vide)")[:2500]
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        return "TIMEOUT apres " + str(timeout) + "s."
    except FileNotFoundError:
        return "Programme introuvable: " + parts[0]


async def _run_pytest(folder: str, timeout: int = 120) -> str:
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pytest", "-q", cwd=folder,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (out.decode("utf-8", errors="replace") or "(vide)")[:2500]
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        return "TIMEOUT pytest apres " + str(timeout) + "s."


async def web_search(query: str) -> str:
    try:
        from claude_agent_sdk import query as sdk_query, ClaudeAgentOptions, ResultMessage
    except ImportError:
        return "Recherche web indisponible (claude-agent-sdk non installe)."
    out = []
    try:
        async for msg in sdk_query(
            prompt="Recherche sur le web et resume de facon concise : " + query,
            options=ClaudeAgentOptions(
                allowed_tools=["WebSearch", "WebFetch"],
                permission_mode="dontAsk",
                model="claude-haiku-4-5",
                max_budget_usd=0.30,
                max_turns=8,
            ),
        ):
            if isinstance(msg, ResultMessage) and msg.subtype == "success":
                out.append(msg.result)
    except Exception as e:
        return "Erreur recherche web: " + str(e)[:200]
    return (NL.join(out) or "Aucun resultat.")[:3000]


# ── MCP : serveurs OFFICIELS uniquement (liste blanche) ──────────────────────
# Verifie cote serveur : un agent ne peut activer qu'un serveur de cette liste
# (serveurs de reference du projet Model Context Protocol).
OFFICIAL_MCP = {
    # Python : auto-installables via pip + Python courant -> AUCUN prerequis pour l'utilisateur.
    "fetch": {"kind": "python", "pip": "mcp-server-fetch", "module": "mcp_server_fetch"},
    "git": {"kind": "python", "pip": "mcp-server-git", "module": "mcp_server_git",
            "args": ["--repository", "{folder}"]},
    "time": {"kind": "python", "pip": "mcp-server-time", "module": "mcp_server_time"},
    # Node : necessitent npx (Node.js), non auto-installable de facon fiable.
    "filesystem": {"kind": "npx", "package": "@modelcontextprotocol/server-filesystem",
                   "args": ["{folder}"]},
    "memory": {"kind": "npx", "package": "@modelcontextprotocol/server-memory"},
    "sequentialthinking": {"kind": "npx", "package": "@modelcontextprotocol/server-sequential-thinking"},
    "everything": {"kind": "npx", "package": "@modelcontextprotocol/server-everything"},
}


async def _pip_install(pkg):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "pip", "install", "-q", pkg,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), timeout=240)
    if proc.returncode != 0:
        raise RuntimeError("pip install " + pkg + " a echoue : "
                           + out.decode("utf-8", "replace")[-300:])


async def _ensure_mcp_command(name, folder):
    """Construit la commande de lancement, en installant le serveur si besoin (Python)."""
    spec = OFFICIAL_MCP[name]
    args = [a.replace("{folder}", folder) for a in spec.get("args", [])]
    if spec["kind"] == "python":
        import importlib.util
        try:
            present = importlib.util.find_spec(spec["module"]) is not None
        except Exception:
            present = False
        if not present:
            await _pip_install(spec["pip"])   # les agents installent eux-memes
        return [sys.executable, "-m", spec["module"]] + args
    return ["npx", "-y", spec["package"]] + args   # kind == "npx"


class MCPServer:
    """Client MCP minimal (JSON-RPC 2.0 sur stdio, messages delimites par newline)."""

    def __init__(self, name, command):
        self.name = name
        self.command = command
        self.proc = None
        self.tools = []
        self._id = 0

    async def _send(self, method, params=None, notification=False):
        self._id += 1
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notification:
            msg["id"] = self._id
        self.proc.stdin.write((json.dumps(msg) + NL).encode("utf-8"))
        await self.proc.stdin.drain()
        if notification:
            return None
        want = self._id
        while True:
            line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=30)
            if not line:
                raise RuntimeError("serveur MCP ferme")
            try:
                resp = json.loads(line)
            except json.JSONDecodeError:
                continue
            if resp.get("id") == want:
                return resp

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await self._send("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "orchestrateur", "version": "1.0"},
        })
        await self._send("notifications/initialized", notification=True)
        resp = await self._send("tools/list", {})
        self.tools = (resp.get("result") or {}).get("tools", [])

    async def call(self, tool, arguments):
        resp = await self._send("tools/call", {"name": tool, "arguments": arguments or {}})
        if "error" in resp:
            return "Erreur MCP : " + json.dumps(resp["error"], ensure_ascii=False)
        result = resp.get("result", {})
        parts = [c.get("text", "") for c in result.get("content", []) if c.get("type") == "text"]
        return (NL.join(parts) or json.dumps(result, ensure_ascii=False))[:3000]

    async def stop(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
            except Exception:
                pass


async def _tool_add_mcp(task_id, folder, server):
    server = (server or "").strip()
    if server not in OFFICIAL_MCP:
        return ("Serveur MCP refuse : '" + server + "' n'est pas un serveur OFFICIEL. "
                "Serveurs autorises : " + ", ".join(sorted(OFFICIAL_MCP)) + ".")
    ctrl = running_tasks.get(task_id)
    if ctrl is None:
        return "Tache non active."
    registry = ctrl.setdefault("mcp", {})
    if server in registry:
        return ("Serveur MCP '" + server + "' deja actif. Outils : "
                + ", ".join(t.get("name", "") for t in registry[server].tools))
    try:
        command = await _ensure_mcp_command(server, folder)
    except Exception as e:
        return "Echec preparation MCP '" + server + "' : " + str(e)[:250]
    s = MCPServer(server, command)
    try:
        await asyncio.wait_for(s.start(), timeout=90)
    except FileNotFoundError:
        return ("Le serveur MCP '" + server + "' necessite Node.js (npx), absent. "
                "Prefere un serveur Python qui s'installe seul : fetch, git ou time.")
    except Exception as e:
        return "Echec demarrage MCP '" + server + "' : " + str(e)[:200]
    registry[server] = s
    return ("Serveur MCP officiel '" + server + "' demarre. Outils : "
            + ", ".join(t.get("name", "") for t in s.tools)
            + ". Appelle-les avec mcp_call.")


async def _tool_mcp_call(task_id, server, tool, arguments):
    registry = (running_tasks.get(task_id) or {}).get("mcp", {})
    s = registry.get((server or "").strip())
    if not s:
        return "Serveur MCP '" + str(server) + "' non demarre. Fais d'abord add_mcp."
    try:
        return await asyncio.wait_for(s.call(tool, arguments), timeout=60)
    except Exception as e:
        return "Erreur mcp_call : " + str(e)[:200]


async def _stop_mcp(task_id):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        return
    for s in list(ctrl.get("mcp", {}).values()):
        try:
            await s.stop()
        except Exception:
            pass
    ctrl["mcp"] = {}


async def execute_tool(task_id: int, folder: str, web_enabled: bool, act: dict) -> str:
    tool = act.get("tool")
    try:
        if tool == "write_file":
            p = _safe_path(folder, act.get("path", ""))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(act.get("content", ""), encoding="utf-8")
            return "Fichier ecrit: " + str(act.get("path"))
        if tool == "read_file":
            p = _safe_path(folder, act.get("path", ""))
            if not p.exists():
                return "Introuvable: " + str(act.get("path"))
            return p.read_text(encoding="utf-8", errors="replace")[:5000]
        if tool == "run_command":
            return await _run_cmd(folder, act.get("cmd", ""))
        if tool == "run_tests":
            return await _run_pytest(folder)
        if tool == "web_search":
            if not web_enabled:
                return "Recherche web desactivee pour cette tache."
            return await web_search(act.get("query", ""))
        if tool == "add_mcp":
            return await _tool_add_mcp(task_id, folder, act.get("server", ""))
        if tool == "mcp_call":
            return await _tool_mcp_call(task_id, act.get("server", ""),
                                        act.get("name") or act.get("tool_name") or "",
                                        act.get("arguments") or {})
        return "Outil inconnu: " + str(tool)
    except Exception as e:
        return "Erreur outil " + str(tool) + ": " + str(e)


# ── Prompts systeme ─────────────────────────────────────────────────────────
CHEF_SYSTEM = """Tu es le CHEF d'une equipe d'agents IA autonomes.

OBJECTIF GLOBAL (fixe par l'utilisateur) :
{objective}

Tu construis et diriges l'equipe toi-meme. Tu peux creer jusqu'a {max_agents} agents.
A CHAQUE TOUR, reponds UNIQUEMENT avec un objet JSON (aucun texte autour) decrivant ta PROCHAINE action :

- Creer un agent :
  {{"thought":"...","action":"create_agent","name":"NomCourt","role":"role / specialite","provider":"claude"}}
  ("provider" peut etre "claude", "gemini" ou "openai")

- Confier une sous-tache a un agent existant :
  {{"thought":"...","action":"assign_task","agent":"NomDeLAgent","instruction":"consigne precise et autonome"}}

- Lancer un CHALLENGE (verification croisee entre les trois modeles) :
  {{"thought":"...","action":"challenge","instruction":"consigne precise","rounds":2}}
  Un Producteur (Claude par defaut) realise le travail, puis les DEUX autres modeles (Gemini et ChatGPT)
  le critiquent chacun de leur cote ; il n'est valide que si les deux critiques approuvent, sinon il
  corrige sur plusieurs tours. Utilise-le pour les livrables importants ou sensibles aux erreurs.

- Terminer (objectif atteint) :
  {{"thought":"...","action":"finish","final":"resume du resultat livre"}}

Regles :
- Decompose l'objectif, cree les roles utiles (ex: chercheur, codeur, redacteur, critique), delegue, fais iterer puis termine.
- Un seul JSON, une seule action par tour.
- Cree un agent AVANT de lui assigner une tache.
- Les agents ecrivent leurs livrables dans des fichiers du dossier partage.
- Pour un livrable important ou sensible aux erreurs, prefere "challenge" a un simple "assign_task" afin que les deux modeles se confrontent.
- Quand le travail est valide, utilise "finish"."""

WORKER_SYSTEM = """Tu es l'agent << {name} >>. Ton role : {role}.
Tu travailles dans un dossier de travail partage avec ton equipe. Recherche web disponible : {web}.

Reponds UNIQUEMENT avec un objet JSON decrivant tes actions pour avancer sur la mission confiee :
{{"thought":"ton raisonnement",
  "actions":[
    {{"tool":"write_file","path":"relatif.ext","content":"..."}},
    {{"tool":"read_file","path":"..."}},
    {{"tool":"run_command","cmd":"python script.py"}},
    {{"tool":"run_tests"}},
    {{"tool":"web_search","query":"..."}},
    {{"tool":"add_mcp","server":"filesystem"}},
    {{"tool":"mcp_call","server":"filesystem","name":"list_directory","arguments":{{}}}}
  ],
  "done": false,
  "report":"(quand done=true) resume clair de ce que tu as produit"}}

Regles :
- Ecris tes livrables dans des fichiers (write_file), chemins relatifs au dossier de travail.
- Commandes autorisees (run_command) : {whitelist}.
- Tu peux installer des paquets Python via run_command (ex: "pip install requests").
- Outils MCP OFFICIELS uniquement : active un serveur avec add_mcp (autorises : {mcp_servers}),
  puis utilise ses outils via mcp_call. Tout serveur non officiel est refuse automatiquement.
  fetch/git/time s'installent tout seuls (Python) ; filesystem/memory/sequentialthinking/everything
  necessitent Node.js. Tu peux aussi installer des paquets toi-meme via run_command (pip install ...).
- Quand ta mission est accomplie : "done": true et un "report". Sinon "done": false avec des actions.
- Un seul JSON par reponse, aucun texte autour."""


# ── Controle de la boucle ────────────────────────────────────────────────────
async def _wait_if_paused(task_id):
    ctrl = running_tasks.get(task_id)
    if ctrl:
        await ctrl["pause"].wait()


async def _is_stopped(task_id) -> bool:
    t = await db_get_task(task_id)
    return (t is None) or (t["status"] == "stopped")


# ── Moteur : un worker execute une sous-tache ───────────────────────────────
async def _run_worker(task_id, iteration, agent, instruction, folder, web_enabled,
                      worker_histories, notice) -> str:
    name = agent["name"]
    hist = worker_histories.setdefault(name, [])
    hist.append({"role": "user", "content":
                 "Mission du chef : " + instruction + NL + NL + "Reponds en JSON avec tes actions."})
    system = WORKER_SYSTEM.format(
        name=name, role=agent["role"],
        web=("oui" if web_enabled else "non"),
        whitelist=", ".join(sorted(COMMAND_WHITELIST)),
        mcp_servers=", ".join(sorted(OFFICIAL_MCP)),
    )
    last_report = "(aucun rapport)"
    for _step in range(WORKER_MAX_STEPS):
        await _wait_if_paused(task_id)
        if await _is_stopped(task_id):
            break
        raw = await call_model(agent["provider"], agent["model"], system, hist, on_notice=notice)
        hist.append({"role": "assistant", "content": raw})
        data = _extract_json(raw) or {}

        thought = data.get("thought", "")
        if thought:
            await emit(task_id, iteration, name, "agent_message", {"kind": "thought", "content": thought})

        results = []
        touched = False
        for act in data.get("actions", []) or []:
            tool = act.get("tool")
            await emit(task_id, iteration, name, "tool_call",
                       {"tool": tool, "input": {k: v for k, v in act.items() if k != "tool"}})
            out = await execute_tool(task_id, folder, web_enabled, act)
            await emit(task_id, iteration, name, "tool_result", {"tool": tool, "output": out[:1500]})
            results.append("[" + str(tool) + "] " + out)
            if tool in ("write_file", "run_command", "run_tests"):
                touched = True
        if touched:
            await emit(task_id, iteration, name, "files_changed", {"list": _list_files(folder)})

        if data.get("done"):
            last_report = data.get("report") or thought or "Termine."
            await emit(task_id, iteration, name, "agent_message", {"kind": "result", "content": last_report})
            return last_report

        if results:
            hist.append({"role": "user", "content":
                         ("Resultats :" + NL + NL.join(results))[:4000]
                         + NL + NL + "Continue, ou termine avec done:true et un report."})
        else:
            hist.append({"role": "user", "content":
                         "Aucune action executee. Propose des actions, ou termine avec done:true + report."})
    return last_report


# ── Moteur : challenge (critique croisee entre les deux modeles) ─────────────
CRITIC_SYSTEM = """Tu es un CRITIQUE adversarial. Ton seul but : trouver les ERREURS, oublis,
cas limites non geres, et ecarts par rapport a la consigne dans le travail d'un autre agent
(qui tourne sur un modele different du tien). Ne sois pas complaisant.

Tu recois la consigne, le rapport du producteur et le contenu des fichiers produits.
Reponds UNIQUEMENT avec un objet JSON :
{"verdict":"approved" ou "needs_changes",
 "issues":["probleme concret et verifiable 1","probleme 2"],
 "comment":"synthese courte de ton evaluation"}

N'approuve ("approved") que si le travail repond vraiment a la consigne sans erreur visible.
Sinon "needs_changes" avec des points precis et actionnables."""


async def _ensure_agent(task_id, name, role, provider, model, iteration):
    existing = await db_get_agent(task_id, name)
    if existing:
        return existing
    await db_add_agent(task_id, name, role, provider, model, "chef")
    await emit(task_id, iteration, "chef", "agent_created",
               {"name": name, "role": role, "model": provider + ":" + model})
    return await db_get_agent(task_id, name)


async def _run_critic(task_id, iteration, critic_agent, instruction, producer_report, folder, notice):
    name = critic_agent["name"]
    parts, budget = [], 0
    for f in _list_files(folder):
        try:
            content = _safe_path(folder, f).read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        snippet = content[:1500]
        budget += len(snippet)
        parts.append("### " + f + NL + snippet)
        if budget > 8000:
            break
    files_blob = (NL + NL).join(parts) or "(aucun fichier)"
    user = ("Consigne de la sous-tache :" + NL + instruction
            + NL + NL + "Rapport du producteur :" + NL + (producer_report or "(aucun)")
            + NL + NL + "Fichiers produits :" + NL + files_blob
            + NL + NL + "Evalue de facon critique. Reponds en JSON.")
    raw = await call_model(critic_agent["provider"], critic_agent["model"],
                           CRITIC_SYSTEM, [{"role": "user", "content": user}], on_notice=notice)
    data = _extract_json(raw) or {}
    verdict = "approved" if data.get("verdict") == "approved" else "needs_changes"
    issues = data.get("issues") or []
    comment = data.get("comment", "")
    await emit(task_id, iteration, name, "critique",
               {"verdict": verdict, "issues": issues, "comment": comment})
    return verdict, issues, comment


PROVIDER_LABEL = {"claude": "Claude", "gemini": "Gemini", "openai": "ChatGPT"}


async def _run_challenge(task_id, iteration, instruction, folder, web_enabled, rounds,
                         worker_histories, notice,
                         producer_provider="claude", critic_providers=None):
    rounds = max(1, min(int(rounds or 2), 4))
    if not critic_providers:
        critic_providers = [p for p in ("claude", "gemini", "openai") if p != producer_provider]
    seen = set()
    critic_providers = [p for p in critic_providers if not (p in seen or seen.add(p))]

    prod = await _ensure_agent(
        task_id, "Producteur (" + PROVIDER_LABEL.get(producer_provider, producer_provider) + ")",
        "produit et corrige la solution",
        producer_provider, _default_model(producer_provider), iteration)
    critics = []
    for cp in critic_providers:
        critics.append(await _ensure_agent(
            task_id, "Critique (" + PROVIDER_LABEL.get(cp, cp) + ")",
            "cherche les erreurs et conteste le travail",
            cp, _default_model(cp), iteration))

    last_issues, final_report, last_comments = [], "", []
    for r in range(1, rounds + 1):
        await _wait_if_paused(task_id)
        if await _is_stopped(task_id):
            break
        await emit(task_id, iteration, "chef", "challenge_round", {"round": r, "total": rounds})

        prod_instruction = instruction
        if last_issues:
            prod_instruction = (instruction + NL + NL
                                + "Les critiques ont souleve ces points, corrige-les :" + NL
                                + NL.join("- " + str(i) for i in last_issues))
        final_report = await _run_worker(task_id, iteration, prod, prod_instruction,
                                         folder, web_enabled, worker_histories, notice)

        round_issues, all_approved, last_comments = [], True, []
        for crit in critics:
            await _wait_if_paused(task_id)
            if await _is_stopped(task_id):
                all_approved = False
                break
            verdict, issues, comment = await _run_critic(
                task_id, iteration, crit, instruction, final_report, folder, notice)
            last_comments.append(crit["name"] + " : " + (comment or verdict))
            if verdict != "approved":
                all_approved = False
                round_issues.extend(issues)
        last_issues = round_issues
        if all_approved:
            return ("Challenge approuve a l'unanimite au round " + str(r) + "/" + str(rounds)
                    + " par " + ", ".join(c["name"] for c in critics)
                    + ". Resultat : " + final_report[:700])
    return ("Challenge termine apres " + str(rounds) + " round(s) sans approbation unanime. "
            + "Avis : " + (" | ".join(last_comments) or "n/a")
            + ". Points restants : " + ("; ".join(str(i) for i in last_issues) if last_issues else "aucun")
            + ". Resultat : " + final_report[:600])


# ── Moteur : la boucle du chef ───────────────────────────────────────────────
async def _run_task(task_id):
    try:
        task = await db_get_task(task_id)
        folder = task["folder"]
        Path(folder).mkdir(parents=True, exist_ok=True)
        objective = task["objective"]
        max_iter = task["max_iterations"]
        max_agents = task["max_agents"]
        web_enabled = bool(task["web_enabled"])
        chef_model = task["chef_model"]

        async def notice(msg):
            await emit(task_id, 0, "systeme", "info", {"msg": msg})

        chef_system = CHEF_SYSTEM.format(objective=objective, max_agents=max_agents)
        chef_messages = [{"role": "user", "content":
                          "Demarre. Quelle est ta premiere action (create_agent, assign_task ou finish) ? Reponds en JSON."}]
        worker_histories: dict[str, list] = {}

        await db_update_task(task_id, status="running")
        iteration = 0
        while iteration < max_iter:
            await _wait_if_paused(task_id)
            if await _is_stopped(task_id):
                break

            ctrl = running_tasks.get(task_id)
            if ctrl and ctrl.get("inbox"):
                pending, ctrl["inbox"] = ctrl["inbox"], []
                for um in pending:
                    chef_messages.append({"role": "user", "content":
                        "Nouvelle consigne de l'utilisateur (integre-la a l'objectif courant) : " + um})

            iteration += 1
            await db_update_task(task_id, iteration=iteration)
            await emit(task_id, iteration, "chef", "turn_start", {"iteration": iteration})

            team = await db_list_agents(task_id)
            files = _list_files(folder)
            ctx = ("Equipe actuelle : "
                   + (", ".join(a["name"] + " (" + a["role"] + ")" for a in team) or "(vide)")
                   + NL + "Fichiers du dossier : " + (", ".join(files) or "(aucun)")
                   + NL + "Prochaine action ? Reponds en JSON.")
            chef_messages.append({"role": "user", "content": ctx})
            raw = await call_model("claude", chef_model, chef_system, chef_messages, on_notice=notice)
            chef_messages.append({"role": "assistant", "content": raw})

            decision = _extract_json(raw) or {}
            action = decision.get("action", "")
            thought = decision.get("thought", "")
            await emit(task_id, iteration, "chef", "chef",
                       {"decision": action or "?", "thought": thought,
                        "instruction": decision.get("instruction")})

            if action == "finish" or decision.get("done"):
                final = decision.get("final") or thought or "Objectif traite."
                await emit(task_id, iteration, "chef", "done", {"summary": final})
                await db_update_task(task_id, status="done")
                return

            if action == "create_agent":
                if len(team) >= max_agents:
                    chef_messages.append({"role": "user", "content":
                                          "Refuse : plafond de " + str(max_agents)
                                          + " agents atteint. Delegue a un agent existant ou termine."})
                    continue
                name = (decision.get("name") or ("agent" + str(len(team) + 1))).strip()
                role = decision.get("role", "")
                provider = decision.get("provider", "claude")
                if provider not in ("claude", "gemini", "openai"):
                    provider = "claude"
                model = decision.get("model") or _default_model(provider)
                if await db_get_agent(task_id, name):
                    chef_messages.append({"role": "user", "content":
                                          "Un agent nomme '" + name + "' existe deja. Choisis un autre nom ou delegue-lui."})
                    continue
                await db_add_agent(task_id, name, role, provider, model, "chef")
                await emit(task_id, iteration, "chef", "agent_created",
                           {"name": name, "role": role, "model": provider + ":" + model})
                chef_messages.append({"role": "user", "content": "Agent '" + name + "' cree."})
                continue

            if action == "assign_task":
                target = (decision.get("agent") or "").strip()
                instruction = decision.get("instruction", "")
                agent = await db_get_agent(task_id, target)
                if not agent:
                    chef_messages.append({"role": "user", "content":
                                          "Agent '" + target + "' introuvable. Cree-le d'abord (create_agent)."})
                    continue
                report = await _run_worker(task_id, iteration, agent, instruction,
                                           folder, web_enabled, worker_histories, notice)
                chef_messages.append({"role": "user", "content":
                                      "Rapport de " + target + " : " + report[:2000]})
                continue

            if action == "challenge":
                instruction = decision.get("instruction", "")
                if not instruction:
                    chef_messages.append({"role": "user", "content":
                                          "challenge requiert un champ 'instruction'."})
                    continue
                report = await _run_challenge(
                    task_id, iteration, instruction, folder, web_enabled,
                    decision.get("rounds", 2), worker_histories, notice,
                    decision.get("producer_provider", "claude"),
                    decision.get("critic_providers"))
                chef_messages.append({"role": "user", "content":
                                      "Resultat du challenge : " + report[:2000]})
                continue

            chef_messages.append({"role": "user", "content":
                                  "Action non reconnue. Utilise create_agent, assign_task, challenge ou finish."})

        if not await _is_stopped(task_id):
            await emit(task_id, iteration, "chef", "done",
                       {"summary": "Limite de " + str(max_iter) + " iterations atteinte."})
            await db_update_task(task_id, status="failed")
    except asyncio.CancelledError:
        await db_update_task(task_id, status="stopped")
        raise
    except Exception as e:
        log.exception("Task %s crashed", task_id)
        await emit(task_id, 0, "systeme", "error", {"msg": str(e)[:300]})
        await db_update_task(task_id, status="failed")
    finally:
        await _stop_mcp(task_id)
        running_tasks.pop(task_id, None)


# ── API ──────────────────────────────────────────────────────────────────────
class AgentSeed(BaseModel):
    name: str
    role: str = ""
    model: str = "claude"  # "claude" ou "gemini"


class TaskCreate(BaseModel):
    objective: str
    chef_model: str = DEFAULT_CLAUDE
    agents: list[AgentSeed] = []
    web_enabled: bool = True
    max_iterations: int = 15
    max_agents: int = 4


router = APIRouter()


@router.get("/workspace")
async def workspace_page():
    return FileResponse(STATIC / "workspace.html", headers={"Cache-Control": "no-store"})


@router.post("/api/tasks")
async def create_task(body: TaskCreate):
    objective = body.objective.strip()
    if not objective:
        raise HTTPException(400, "Objectif manquant")
    max_iter = max(1, min(body.max_iterations, MAX_ITER_CEILING))
    max_agents = max(1, min(body.max_agents, MAX_AGENTS_CEILING))
    folder = str(PROJECTS / ("task_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
    Path(folder).mkdir(parents=True, exist_ok=True)
    task_id = await db_create_task(objective, folder, max_iter, max_agents,
                                   body.web_enabled, body.chef_model or DEFAULT_CLAUDE)
    # Amorcage optionnel d'agents par l'utilisateur
    for a in body.agents:
        name = (a.name or "").strip()
        if not name:
            continue
        provider = a.model if a.model in ("claude", "gemini", "openai") else "claude"
        await db_add_agent(task_id, name, a.role, provider, _default_model(provider), "user")
    return {"id": task_id, "folder": folder}


@router.post("/api/tasks/{task_id}/start")
async def start_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    if task_id in running_tasks:
        return {"status": "already_running"}
    pause = asyncio.Event()
    pause.set()  # set = en marche ; clear = en pause
    running_tasks[task_id] = {"pause": pause, "step": False, "inbox": [], "mcp": {}}
    coro = _run_task(task_id)
    running_tasks[task_id]["task"] = asyncio.create_task(coro)
    return {"status": "running"}


@router.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: int):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    ctrl["pause"].clear()
    await db_update_task(task_id, status="paused")
    await emit(task_id, 0, "systeme", "paused", {})
    return {"status": "paused"}


@router.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: int):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    ctrl["pause"].set()
    await db_update_task(task_id, status="running")
    await emit(task_id, 0, "systeme", "resumed", {})
    return {"status": "running"}


@router.post("/api/tasks/{task_id}/step")
async def step_task(task_id: int):
    """Avance la tache si elle est en pause : on debloque brievement la boucle."""
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active")
    pause = ctrl["pause"]
    pause.set()
    await asyncio.sleep(0.05)
    pause.clear()
    await db_update_task(task_id, status="paused")
    return {"status": "stepped"}


@router.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: int):
    await db_update_task(task_id, status="stopped")
    ctrl = running_tasks.get(task_id)
    if ctrl:
        ctrl["pause"].set()  # debloque la boucle pour qu'elle constate l'arret
    await _stop_launched(task_id)
    await _stop_mcp(task_id)
    await emit(task_id, 0, "systeme", "stopped", {})
    return {"status": "stopped"}


class TaskMessage(BaseModel):
    text: str


@router.post("/api/tasks/{task_id}/message")
async def task_message(task_id: int, body: TaskMessage):
    ctrl = running_tasks.get(task_id)
    if not ctrl:
        raise HTTPException(404, "Tache non active (terminee ou arretee)")
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(400, "Message vide")
    ctrl.setdefault("inbox", []).append(text)
    t = await db_get_task(task_id)
    await emit(task_id, t["iteration"] if t else 0, "utilisateur", "user_message", {"content": text})
    return {"queued": True}


# ── Lancement de l'app produite ──────────────────────────────────────────────
WEB_ENTRYPOINTS = ["app.py", "main.py", "server.py", "run.py"]


async def _stop_launched(task_id):
    info = launched_apps.pop(task_id, None)
    if info and info.get("proc") and info["proc"].returncode is None:
        try:
            info["proc"].terminate()
        except Exception:
            pass


async def _drain_proc(task_id, proc):
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            info = launched_apps.get(task_id)
            if info is None:
                break
            info["output"].append(line.decode("utf-8", "replace"))
            info["output"] = info["output"][-200:]
    except Exception:
        pass


async def _launch_python_app(task_id, base, name):
    await _stop_launched(task_id)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, name, cwd=str(base),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    launched_apps[task_id] = {"proc": proc, "url": None, "output": []}
    url, lines = None, []
    for _ in range(15):
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=0.5)
        except asyncio.TimeoutError:
            break
        if not line:
            break
        text = line.decode("utf-8", "replace")
        lines.append(text)
        m = re.search(r"https?://[^\s'\"]+", text)
        if m:
            url = m.group(0)
            break
        m2 = re.search(r"port[^\d]{0,6}(\d{2,5})", text, re.IGNORECASE)
        if m2:
            url = "http://localhost:" + m2.group(1)
            break
    launched_apps[task_id]["output"] = lines
    launched_apps[task_id]["url"] = url
    asyncio.create_task(_drain_proc(task_id, proc))
    return {"type": "python", "entry": name, "url": url,
            "running": proc.returncode is None, "output": "".join(lines)[-1500:]}


@router.post("/api/tasks/{task_id}/launch")
async def launch_app(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    base = Path(t["folder"])
    for name in WEB_ENTRYPOINTS:
        if (base / name).exists():
            return await _launch_python_app(task_id, base, name)
    htmls = [f for f in _list_files(t["folder"]) if f.endswith(".html")]
    if htmls:
        target = "index.html" if "index.html" in htmls else htmls[0]
        return {"type": "static", "url": "/api/tasks/" + str(task_id) + "/app/" + target}
    return {"type": "none", "message": "Aucun point d'entree lancable (app.py/main.py/index.html)."}


@router.post("/api/tasks/{task_id}/launch/stop")
async def launch_stop(task_id: int):
    await _stop_launched(task_id)
    return {"stopped": True}


@router.get("/api/tasks/{task_id}/app/{path:path}")
async def serve_app(task_id: int, path: str):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    try:
        p = _safe_path(t["folder"], path)
    except ValueError:
        raise HTTPException(400, "Chemin invalide")
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "Fichier introuvable")
    return FileResponse(p)


@router.get("/api/tasks")
async def list_tasks():
    return await db_list_tasks()


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    t["agents"] = await db_list_agents(task_id)
    t["active"] = task_id in running_tasks
    return t


@router.delete("/api/tasks/{task_id}")
async def delete_task(task_id: int):
    await stop_task(task_id)
    t = await db_get_task(task_id)
    if t:
        shutil.rmtree(t["folder"], ignore_errors=True)
    await db_delete_task(task_id)
    return {"deleted": True}


@router.get("/api/tasks/{task_id}/files")
async def task_files(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    return {"files": _list_files(t["folder"])}


@router.get("/api/tasks/{task_id}/file")
async def task_file(task_id: int, path: str = Query(...)):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")
    try:
        p = _safe_path(t["folder"], path)
    except ValueError:
        raise HTTPException(400, "Chemin invalide")
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "Fichier introuvable")
    return {"path": path, "content": p.read_text(encoding="utf-8", errors="replace")[:50000]}


@router.get("/api/tasks/{task_id}/stream")
async def stream_task(task_id: int):
    t = await db_get_task(task_id)
    if not t:
        raise HTTPException(404, "Tache introuvable")

    async def gen() -> AsyncGenerator[str, None]:
        last_id = 0
        while True:
            rows = await db_messages_after(task_id, last_id)
            for r in rows:
                last_id = r["id"]
                payload = {"type": r["kind"], "agent": r["agent"],
                           "iteration": r["iteration"], "id": r["id"]}
                try:
                    payload.update(json.loads(r["content"]))
                except Exception:
                    payload["content"] = r["content"]
                yield sse(payload)
            cur = await db_get_task(task_id)
            if cur and cur["status"] in ("done", "stopped", "failed"):
                tail = await db_messages_after(task_id, last_id)
                if not tail:
                    yield sse({"type": "end", "status": cur["status"]})
                    return
            await asyncio.sleep(0.5)

    return StreamingResponse(
        gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
