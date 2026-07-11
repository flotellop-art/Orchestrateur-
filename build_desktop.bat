@echo off
title Multi-Agent Orchestrator - Build Desktop
echo.
echo ===================================================
echo   Build de l'application Desktop (.exe)
echo ===================================================
echo.

cd /d "%~dp0"

echo [1/3] Construction du serveur Python autonome...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build_backend.ps1"
if errorlevel 1 ( echo ERREUR build backend & pause & exit /b 1 )

cd /d "%~dp0electron-app"

echo [2/3] Installation des dependances desktop verrouillees...
call npm ci
if errorlevel 1 ( echo ERREUR npm ci & pause & exit /b 1 )

echo.
echo [3/3] Build de l'executable...
call npx electron-builder --win
if errorlevel 1 ( echo ERREUR build & pause & exit /b 1 )

echo.
echo ===================================================
echo BUILD REUSSI!
echo L'executable se trouve dans: electron-app\dist\
echo ===================================================
pause
