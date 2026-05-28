import asyncio
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator

import anthropic
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path, override=True)
log = logging.getLogger(__name__)


def _get_client() -> anthropic.AsyncAnthropic:
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY manquant. Verifiez le fichier .env")
    return anthropic.AsyncAnthropic(api_key=key)
conversations: dict[str, list] = {}
pending_actions: dict[str, dict] = {}
STATIC_DIR = Path(__file__).parent / "static"

TOOLS = [
    {"name": "get_current_time", "description": "Retourne la date et l heure actuelles.", "input_schema": {"type": "object", "properties": {}, "required": []}},
    {"name": "send_email", "description": "Envoie un email. Action irreversible - necessite confirmation.", "input_schema": {"type": "object", "properties": {"to": {"type": "string"}, "subject": {"type": "string"}, "body": {"type": "string"}}, "required": ["to", "subject", "body"]}},
    {"name": "read_emails", "description": "Lit les emails recents.", "input_schema": {"type": "object", "properties": {"query": {"type": "string"}, "max_results": {"type": "integer", "default": 5}}, "required": []}},
    {"name": "create_calendar_event", "description": "Cree un evenement calendrier. Action irreversible - necessite confirmation.", "input_schema": {"type": "object", "properties": {"title": {"type": "string"}, "date": {"type": "string"}, "duration": {"type": "integer", "default": 60}}, "required": ["title", "date"]}},
    {"name": "search_web", "description": "Recherche sur le web.", "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
    {"name": "read_file", "description": "Lit un fichier local.", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}},
    {"name": "write_file", "description": "Ecrit dans un fichier. Action irreversible - necessite confirmation.", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}, "content": {"type": "string"}}, "required": ["path", "content"]}},
    {"name": "run_command", "description": "Execute une commande systeme (liste blanche). Action irreversible - necessite confirmation.", "input_schema": {"type": "object", "properties": {"cmd": {"type": "string"}}, "required": ["cmd"]}},
]

IRREVERSIBLE_TOOLS = {"send_email", "create_calendar_event", "write_file", "run_command"}
COMMAND_WHITELIST = ["dir", "ls", "echo", "whoami", "pwd", "python --version", "python -V", "pip list", "git status", "git log --oneline", "git branch", "ipconfig", "hostname"]

NL = chr(10)


def sse(data: dict) -> str:
    return "data: " + json.dumps(data, ensure_ascii=False) + NL + NL


async def execute_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "get_current_time":
        now = datetime.now()
        days = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
        months = ["janvier", "fevrier", "mars", "avril", "mai", "juin", "juillet", "aout", "septembre", "octobre", "novembre", "decembre"]
        return "Nous sommes " + days[now.weekday()] + " " + str(now.day) + " " + months[now.month - 1] + " " + str(now.year) + ", il est " + now.strftime("%H:%M:%S") + "."
    elif tool_name == "send_email":
        to = tool_input.get("to", "")
        subject = tool_input.get("subject", "")
        log.info("[SIMULE] send_email -> " + to)
        return "Email envoye a " + to + " - Sujet: " + subject
    elif tool_name == "read_emails":
        query = tool_input.get("query", "")
        max_r = tool_input.get("max_results", 5)
        suffix = (" pour: " + query) if query else ""
        return "[SIMULE] " + str(max_r) + " emails" + suffix + NL + "1. jean.dupont@example.com | RDV demain 14h" + NL + "2. facture@fournisseur.fr | Facture #2024-892"
    elif tool_name == "create_calendar_event":
        return "Evenement cree: " + tool_input.get("title", "") + " le " + tool_input.get("date", "")
    elif tool_name == "search_web":
        query = tool_input.get("query", "")
        return "[SIMULE] Resultats pour: " + query + NL + "1. Premier resultat pertinent" + NL + "2. Article connexe"
    elif tool_name == "read_file":
        path = tool_input.get("path", "")
        try:
            p = Path(path)
            if not p.exists():
                return "Fichier introuvable: " + path
            c = p.read_text(encoding="utf-8", errors="replace")
            if len(c) > 5000:
                c = c[:5000] + "... (tronque)"
            return "Contenu de " + path + ":" + NL + c
        except Exception as e:
            return "Erreur: " + str(e)
    elif tool_name == "write_file":
        path = tool_input.get("path", "")
        content = tool_input.get("content", "")
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")
            return "Fichier ecrit: " + path
        except Exception as e:
            return "Erreur: " + str(e)
    elif tool_name == "run_command":
        cmd = tool_input.get("cmd", "").strip()
        if not any(cmd.startswith(w) for w in COMMAND_WHITELIST):
            return "Commande refusee: " + cmd
        try:
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            return (r.stdout or r.stderr or "(vide)").strip()[:2000]
        except Exception as e:
            return "Erreur: " + str(e)
    return "Outil inconnu: " + tool_name


def describe_action(tool_name: str, tool_input: dict) -> str:
    if tool_name == "send_email":
        return "Envoyer un email a **" + tool_input.get("to", "") + "** - sujet: " + tool_input.get("subject", "")
    elif tool_name == "create_calendar_event":
        return "Creer: **" + tool_input.get("title", "") + "** le " + tool_input.get("date", "")
    elif tool_name == "write_file":
        return "Ecrire dans: **" + tool_input.get("path", "") + "** (" + str(len(tool_input.get("content", ""))) + " chars)"
    elif tool_name == "run_command":
        return "Executer: " + tool_input.get("cmd", "")
    return "Action: " + tool_name


SYSTEM_PROMPT = (
    "Tu es un assistant IA personnel auto-heberge qui tourne sur le PC Windows de l utilisateur." + NL
    + "Tu peux effectuer de vraies actions: emails, calendrier, recherche web, fichiers locaux, commandes." + NL + NL
    + "Regles:" + NL
    + "- Tu reponds TOUJOURS en francais." + NL
    + "- Sois concis et efficace." + NL
    + "- Pour l heure et la date, utilise toujours l outil get_current_time." + NL
    + "- Les actions irreversibles necessitent confirmation - le systeme gere cela." + NL
    + "- En cas d erreur, explique et propose une alternative."
)


async def _run_loop(session_id: str, messages: list) -> AsyncGenerator[str, None]:
    for iteration in range(10):
        text_output = ""
        tool_use_data: dict = {}
        try:
            async with _get_client().messages.stream(
                model="claude-opus-4-8",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            ) as stream:
                async for event in stream:
                    if event.type == "content_block_start":
                        b = event.content_block
                        if b.type == "tool_use":
                            tool_use_data[event.index] = {"id": b.id, "name": b.name, "raw": ""}
                    elif event.type == "content_block_delta":
                        d = event.delta
                        if d.type == "text_delta":
                            text_output += d.text
                            yield sse({"type": "text_delta", "text": d.text})
                        elif d.type == "input_json_delta" and event.index in tool_use_data:
                            tool_use_data[event.index]["raw"] += d.partial_json
                final_msg = await stream.get_final_message()
                stop_reason = final_msg.stop_reason
        except anthropic.APIError as e:
            yield sse({"type": "error", "message": "Erreur API: " + str(e)})
            return
        except Exception as e:
            log.exception("Loop error")
            yield sse({"type": "error", "message": str(e)})
            return

        tool_uses = []
        for idx in sorted(tool_use_data.keys()):
            t = tool_use_data[idx]
            try:
                inp = json.loads(t["raw"]) if t["raw"] else {}
            except Exception:
                inp = {}
            tool_uses.append({"id": t["id"], "name": t["name"], "input": inp})

        ac: list = []
        if text_output:
            ac.append({"type": "text", "text": text_output})
        for t in tool_uses:
            ac.append({"type": "tool_use", "id": t["id"], "name": t["name"], "input": t["input"]})
        if ac:
            messages.append({"role": "assistant", "content": ac})

        if stop_reason == "end_turn" or not tool_uses:
            yield sse({"type": "done"})
            return

        tool_results = []
        paused = False
        for tu in tool_uses:
            name = tu["name"]
            tid = tu["id"]
            tinput = tu["input"]
            if name in IRREVERSIBLE_TOOLS:
                action_id = str(uuid.uuid4())
                desc = describe_action(name, tinput)
                pending_actions[action_id] = {
                    "session_id": session_id,
                    "tool_id": tid,
                    "tool_name": name,
                    "tool_input": tinput,
                    "messages": messages,
                    "remaining_tools": tool_uses[tool_uses.index(tu) + 1:],
                    "tool_results_so_far": tool_results,
                }
                yield sse({"type": "confirmation_needed", "action_id": action_id, "description": desc, "tool_name": name})
                paused = True
                break
            yield sse({"type": "tool_executing", "tool_name": name})
            try:
                result = await execute_tool(name, tinput)
            except Exception as e:
                result = "Erreur: " + str(e)
            tool_results.append({"type": "tool_result", "tool_use_id": tid, "content": result})
            yield sse({"type": "tool_result", "tool_name": name, "result": result[:300]})
        if paused:
            return
        messages.append({"role": "user", "content": tool_results})
    yield sse({"type": "error", "message": "Limite iterations atteinte."})


async def agent_stream(session_id: str, user_message: str) -> AsyncGenerator[str, None]:
    if session_id not in conversations:
        conversations[session_id] = []
    conversations[session_id].append({"role": "user", "content": user_message})
    async for ev in _run_loop(session_id, conversations[session_id]):
        yield ev


async def resume_after_confirmation(action_id: str, confirmed: bool) -> AsyncGenerator[str, None]:
    if action_id not in pending_actions:
        yield sse({"type": "error", "message": "Action introuvable ou expiree."})
        return
    action = pending_actions.pop(action_id)
    session_id = action["session_id"]
    messages = action["messages"]
    tool_results = action["tool_results_so_far"]
    if not confirmed:
        tool_results.append({"type": "tool_result", "tool_use_id": action["tool_id"], "content": "Action annulee par l utilisateur."})
        for rem in action["remaining_tools"]:
            tool_results.append({"type": "tool_result", "tool_use_id": rem["id"], "content": "Annulee."})
        messages.append({"role": "user", "content": tool_results})
        async for ev in _run_loop(session_id, messages):
            yield ev
        return
    yield sse({"type": "tool_executing", "tool_name": action["tool_name"]})
    try:
        result = await execute_tool(action["tool_name"], action["tool_input"])
    except Exception as e:
        result = "Erreur: " + str(e)
    tool_results.append({"type": "tool_result", "tool_use_id": action["tool_id"], "content": result})
    yield sse({"type": "tool_result", "tool_name": action["tool_name"], "result": result[:300]})
    for rem in action["remaining_tools"]:
        r_name = rem["name"]
        r_id = rem["id"]
        r_input = rem["input"]
        if r_name in IRREVERSIBLE_TOOLS:
            new_id = str(uuid.uuid4())
            desc = describe_action(r_name, r_input)
            idx = action["remaining_tools"].index(rem)
            pending_actions[new_id] = {
                "session_id": session_id,
                "tool_id": r_id,
                "tool_name": r_name,
                "tool_input": r_input,
                "messages": messages,
                "remaining_tools": action["remaining_tools"][idx + 1:],
                "tool_results_so_far": tool_results,
            }
            yield sse({"type": "confirmation_needed", "action_id": new_id, "description": desc, "tool_name": r_name})
            return
        yield sse({"type": "tool_executing", "tool_name": r_name})
        try:
            r_res = await execute_tool(r_name, r_input)
        except Exception as e:
            r_res = "Erreur: " + str(e)
        tool_results.append({"type": "tool_result", "tool_use_id": r_id, "content": r_res})
    messages.append({"role": "user", "content": tool_results})
    async for ev in _run_loop(session_id, messages):
        yield ev


class ChatMessage(BaseModel):
    message: str
    session_id: str = "default"


class ConfirmAction(BaseModel):
    confirmed: bool


router = APIRouter()


@router.get("/chat")
async def chat_page():
    return FileResponse(STATIC_DIR / "chat.html")


@router.post("/api/chat")
async def post_chat(body: ChatMessage):
    async def gen():
        async for ev in agent_stream(body.session_id, body.message):
            yield ev
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/api/chat/history")
async def get_history(session_id: str = Query(default="default")):
    msgs = conversations.get(session_id, [])
    history = []
    for m in msgs:
        c = m["content"]
        if isinstance(c, str):
            history.append({"role": m["role"], "text": c})
        elif isinstance(c, list):
            for b in c:
                if isinstance(b, dict) and b.get("type") == "text":
                    history.append({"role": m["role"], "text": b["text"]})
    return {"session_id": session_id, "messages": history, "sessions": list(conversations.keys())}


@router.delete("/api/chat/history")
async def clear_history(session_id: str = Query(default="default")):
    conversations.pop(session_id, None)
    return {"cleared": session_id}


@router.post("/api/chat/confirm/{action_id}")
async def confirm_action(action_id: str, body: ConfirmAction):
    if action_id not in pending_actions:
        raise HTTPException(status_code=404, detail="Action introuvable ou expiree")

    async def gen():
        async for ev in resume_after_confirmation(action_id, body.confirmed):
            yield ev
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
