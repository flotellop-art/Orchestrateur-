# -*- mode: python ; coding: utf-8 -*-
"""Bundle PyInstaller du serveur FastAPI utilise par Electron."""

from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files

ROOT = Path(SPEC).resolve().parent
SDK_DATAS = collect_data_files(
    "claude_agent_sdk", includes=["_bundled/*"]
)

# Les imports des fournisseurs sont visibles statiquement dans le code, meme
# lorsqu'ils sont places dans une fonction. Les collecter recursivement
# embarquerait leurs suites de tests et des options facultatives (voix/numpy),
# ce qui alourdirait fortement l'installateur et pourrait casser le build.
hiddenimports = [
    "uvicorn.logging",
    "uvicorn.loops.auto",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.websockets.auto",
    "uvicorn.lifespan.on",
]

a = Analysis(
    [str(ROOT / "orchestrator.py")],
    pathex=[str(ROOT)],
    binaries=[],
    datas=[
        (str(ROOT / "static"), "static"),
        (str(ROOT / "VERSION"), "."),
        (str(ROOT / ".env.example"), "."),
    ] + SDK_DATAS,
    hiddenimports=sorted(set(hiddenimports)),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tkinter"],
    noarchive=False,
    optimize=1,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="orchestrator-backend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name="orchestrator-backend",
)
