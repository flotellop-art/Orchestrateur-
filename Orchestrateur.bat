@echo off
title Multi-Agent Orchestrator
set NODE_ENV=development
cd /d "%~dp0"

echo ============================================
echo   Multi-Agent Orchestrator
echo ============================================
echo.

:: --- [1/3] Mise a jour du code (best effort : ne bloque pas le lancement) ---
echo [1/3] Recherche de mises a jour...
where git >nul 2>nul
if %ERRORLEVEL%==0 (
    git pull --rebase --autostash
    if errorlevel 1 echo      ^(mise a jour ignoree : hors ligne ou conflit -- lancement du code actuel^)
) else (
    echo      ^(git introuvable : mise a jour automatique desactivee^)
)

:: --- [2/3] Dependances Electron a jour (rapide si rien n'a change) ---
echo.
echo [2/3] Verification des dependances...
cd /d "%~dp0electron-app"
call npm install --include=dev --no-audit --no-fund

:: --- [3/3] Lancement de l'application ---
echo.
echo [3/3] Lancement...
:: Tuer un ancien Electron et liberer le port 8000
taskkill /F /IM electron.exe /T >nul 2>&1
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8000 " ^| findstr "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
ping -n 2 127.0.0.1 >nul 2>&1
node_modules\.bin\electron.cmd .
