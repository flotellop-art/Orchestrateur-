@echo off
set NODE_ENV=development

:: Tuer les anciens processus
taskkill /F /IM electron.exe /T >nul 2>&1

:: Liberer le port 8000 si occupe
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8000 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)

:: Attendre que le port soit libre (ping = delai fiable sans console)
ping -n 3 127.0.0.1 >nul 2>&1

:: Lancer Electron
cd /d "%~dp0electron-app"
node_modules\.bin\electron.cmd .
