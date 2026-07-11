"""memory.py — Memoire long-terme evoluee pour les agents internes.

3 etages :
  - _user        : tes notes/preferences (chargees au demarrage de chaque tache)
  - role:<slug>  : memoire dediee a un role d'agent (ex. role:security-auditor)
  - _shared      : leconage commun (toutes taches, tous agents)

Recherche : semantique (embeddings OpenAI 256-dim) si OPENAI_API_KEY dispo,
sinon fallback LIKE (idem ancien comportement).

Auto-resume : a la fin de chaque tache, un appel Haiku tire 1-3 lecons et les
range dans _shared. Permet a l'app de s'ameliorer toute seule au fil du temps.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import struct
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

from app_paths import DB_PATH

EMBED_MODEL = os.getenv("MEMORY_EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = int(os.getenv("MEMORY_EMBED_DIM", "256"))
SUMMARY_MODEL = os.getenv("MEMORY_SUMMARY_MODEL", "claude-haiku-4-5")
SIMILARITY_THRESHOLD = float(os.getenv("MEMORY_THRESHOLD", "0.32"))
MAX_RECALL = int(os.getenv("MEMORY_MAX_RECALL", "5"))


# ── Embeddings ────────────────────────────────────────────────────────────────
_oai_client = None
_oai_lock = asyncio.Lock()


async def _get_openai():
    global _oai_client
    if _oai_client is None:
        async with _oai_lock:
            if _oai_client is None:
                from openai import AsyncOpenAI
                _oai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _oai_client


def embeddings_available() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


async def embed(text: str) -> Optional[list[float]]:
    """Renvoie le vecteur d'embedding ou None si indisponible."""
    if not text or not embeddings_available():
        return None
    try:
        client = await _get_openai()
        resp = await client.embeddings.create(
            model=EMBED_MODEL, input=text, dimensions=EMBED_DIM,
        )
        return list(resp.data[0].embedding)
    except Exception as e:
        log.warning("[memoire] embedding KO : %s", str(e)[:120])
        return None


def _pack(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _unpack(blob: bytes) -> list[float]:
    return list(struct.unpack(f"{len(blob)//4}f", blob))


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na ** 0.5 * nb ** 0.5)


# ── Schema (cree dans apps.db) ────────────────────────────────────────────────
async def init_memory_db(db_path: Optional[Path] = None) -> None:
    """Cree la table knowledge_v2 si elle n'existe pas. Idempotent."""
    import aiosqlite
    path = db_path or DB_PATH
    async with aiosqlite.connect(str(path), timeout=30.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_v2 (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                content       TEXT NOT NULL,
                namespace     TEXT NOT NULL DEFAULT '_shared',
                source        TEXT,
                embedding     BLOB,
                created_at    TEXT NOT NULL,
                used_count    INTEGER DEFAULT 0
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_kv2_ns ON knowledge_v2(namespace)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_kv2_created ON knowledge_v2(created_at)")
        await db.commit()


# ── API memoire ────────────────────────────────────────────────────────────────
def role_namespace(name_or_role: str) -> str:
    """Transforme un nom/role d'agent en cle de namespace stable."""
    slug = re.sub(r"[^a-z0-9]+", "-", (name_or_role or "").lower()).strip("-")
    return "role:" + (slug or "anon")


async def remember(content: str, namespace: str = "_shared",
                   source: Optional[str] = None) -> Optional[int]:
    """Enregistre une lecon. Calcule l'embedding (best-effort)."""
    import aiosqlite
    if not content or not content.strip():
        return None
    content = content.strip()
    vec = await embed(content)
    blob = _pack(vec) if vec else None
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        cur = await db.execute(
            "INSERT INTO knowledge_v2 (content, namespace, source, embedding, created_at) "
            "VALUES (?,?,?,?,?)",
            (content, namespace, source, blob, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return cur.lastrowid


async def recall(query: str, namespaces: Optional[list[str]] = None,
                 k: int = MAX_RECALL) -> list[dict]:
    """Recherche dans la memoire (semantique si dispo, sinon LIKE)."""
    import aiosqlite
    namespaces = namespaces or ["_shared"]
    if not namespaces:
        return []
    qvec = await embed(query) if query else None
    placeholders = ",".join("?" * len(namespaces))
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        if qvec:
            # Recherche semantique : pull all dans les namespaces, score, top-k
            async with db.execute(
                f"SELECT id, content, source, namespace, embedding "
                f"FROM knowledge_v2 WHERE namespace IN ({placeholders}) "
                f"AND embedding IS NOT NULL",
                namespaces,
            ) as cur:
                rows = await cur.fetchall()
            scored = []
            for r in rows:
                vec = _unpack(r["embedding"])
                score = _cosine(qvec, vec)
                if score >= SIMILARITY_THRESHOLD:
                    scored.append((score, {
                        "id": r["id"], "content": r["content"],
                        "source": r["source"], "namespace": r["namespace"],
                        "score": round(score, 3),
                    }))
            scored.sort(key=lambda x: x[0], reverse=True)
            return [r for _, r in scored[:k]]
        else:
            # Fallback : LIKE simple
            like = "%" + (query or "") + "%"
            async with db.execute(
                f"SELECT id, content, source, namespace "
                f"FROM knowledge_v2 WHERE namespace IN ({placeholders}) "
                f"AND content LIKE ? ORDER BY id DESC LIMIT ?",
                namespaces + [like, k],
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]


async def list_namespace(namespace: str, limit: int = 100) -> list[dict]:
    """Liste les entrees d'un namespace (pour l'UI)."""
    import aiosqlite
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, content, source, created_at FROM knowledge_v2 "
            "WHERE namespace=? ORDER BY id DESC LIMIT ?",
            (namespace, limit),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def delete_entry(entry_id: int) -> bool:
    import aiosqlite
    async with aiosqlite.connect(str(DB_PATH), timeout=30.0) as db:
        cur = await db.execute("DELETE FROM knowledge_v2 WHERE id=?", (entry_id,))
        await db.commit()
        return cur.rowcount > 0


# ── Auto-resume de fin de tache ────────────────────────────────────────────────
_SUMMARY_SYS = """Tu resumes le travail d'une equipe d'agents en 1 a 3 LECONS reutilisables
pour de futures taches similaires. Chaque lecon doit etre :
- COURTE (1-2 lignes max)
- CONCRETE (un fait, une astuce, un piege a eviter)
- GENERALISABLE (ne mentionne pas le nom de la tache courante)

Reponds UNIQUEMENT en JSON : {"lessons":["lecon 1","lecon 2","lecon 3"]}.
Si rien de reutilisable n'a ete appris, renvoie {"lessons":[]}."""


# ── API HTTP : gestion des notes utilisateur ──────────────────────────────────
try:
    from fastapi import APIRouter, HTTPException
    from pydantic import BaseModel

    router = APIRouter()

    class NoteIn(BaseModel):
        content: str

    @router.get("/api/memory/notes")
    async def http_list_notes():
        """Liste les notes utilisateur (namespace _user)."""
        return {"notes": await list_namespace("_user", limit=200)}

    @router.post("/api/memory/notes")
    async def http_add_note(body: NoteIn):
        """Ajoute une note utilisateur."""
        content = (body.content or "").strip()
        if len(content) < 3:
            raise HTTPException(400, "Note trop courte.")
        if len(content) > 4000:
            raise HTTPException(400, "Note trop longue (>4000 caracteres).")
        rid = await remember(content, namespace="_user", source="user-note")
        if not rid:
            raise HTTPException(500, "Echec d'enregistrement.")
        return {"id": rid, "embedded": embeddings_available()}

    @router.delete("/api/memory/notes/{note_id}")
    async def http_delete_note(note_id: int):
        ok = await delete_entry(note_id)
        if not ok:
            raise HTTPException(404, "Note introuvable.")
        return {"deleted": note_id}

    @router.get("/api/memory/list")
    async def http_list(namespace: str = "_shared", limit: int = 100):
        return {"items": await list_namespace(namespace, limit=limit)}

    @router.get("/api/memory/search")
    async def http_search(q: str, ns: str = "_shared,_user", k: int = 8):
        return {"results": await recall(q, namespaces=ns.split(","), k=k)}

    @router.get("/api/memory/status")
    async def http_status():
        return {
            "embeddings": embeddings_available(),
            "embed_model": EMBED_MODEL if embeddings_available() else None,
            "embed_dim": EMBED_DIM,
            "summary_model": SUMMARY_MODEL,
        }
except ImportError:
    router = None  # fastapi pas dispo (tests purs)


async def summarize_and_save(task_id: int, objective: str, messages_blob: str,
                             anthropic_client) -> int:
    """Genere 1-3 lecons via Haiku et les range dans _shared. Retourne le nb enregistre."""
    if not messages_blob or not messages_blob.strip():
        return 0
    try:
        prompt = (
            "Objectif de la tache :" + chr(10) + (objective or "(non specifie)")
            + chr(10) + chr(10) + "Extraits du travail :" + chr(10)
            + messages_blob[:8000]
            + chr(10) + chr(10) + "Tire 1 a 3 lecons reutilisables. Reponds en JSON."
        )
        resp = await anthropic_client.messages.create(
            model=SUMMARY_MODEL,
            max_tokens=512,
            system=_SUMMARY_SYS,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
        try:
            start = text.index("{")
            end = text.rindex("}") + 1
            data = json.loads(text[start:end])
        except Exception:
            return 0
        saved = 0
        for lesson in (data.get("lessons") or [])[:3]:
            if lesson and isinstance(lesson, str) and len(lesson.strip()) > 12:
                await remember(lesson.strip(), namespace="_shared",
                               source="task:" + str(task_id))
                saved += 1
        return saved
    except Exception as e:
        log.warning("[memoire] auto-resume KO : %s", str(e)[:200])
        return 0
