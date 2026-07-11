@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo [ERREUR] Environnement Python absent. Lancez install_desktop.bat.
    exit /b 1
)
echo Orchestrateur demarre sur http://localhost:8000
start http://localhost:8000
".venv\Scripts\python.exe" orchestrator.py
exit /b %ERRORLEVEL%
