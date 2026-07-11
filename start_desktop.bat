@echo off
setlocal
set NODE_ENV=development
cd /d "%~dp0electron-app"
if not exist "node_modules\.bin\electron.cmd" (
    echo [ERREUR] Dependances Electron absentes. Lancez install_desktop.bat.
    exit /b 1
)
call npm start
exit /b %ERRORLEVEL%
