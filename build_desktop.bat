@echo off
title Multi-Agent Orchestrator - Build Desktop
echo.
echo ===================================================
echo   Build de l'application Desktop (.exe)
echo ===================================================
echo.

cd /d "%~dp0electron-app"

echo [1/2] Installation des dependances...
call npm install
if errorlevel 1 ( echo ERREUR npm install & pause & exit /b 1 )

echo.
echo [2/2] Build de l'executable...
call npx electron-builder --win
if errorlevel 1 ( echo ERREUR build & pause & exit /b 1 )

echo.
echo ===================================================
echo BUILD REUSSI!
echo L'executable se trouve dans: electron-app\dist\
echo ===================================================
pause
