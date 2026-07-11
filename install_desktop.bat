@echo off
setlocal
echo ==========================================
echo   Preparation du developpement Orchestrateur
echo ==========================================
echo.

:: Forcer mode development pour inclure les devDependencies
set NODE_ENV=development

:: Verifier Node.js
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] Node.js n'est pas installe ou pas dans le PATH.
    echo Telechargez-le sur https://nodejs.org
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('node --version 2^>nul') do set NODE_VER=%%i
echo [OK] Node.js detecte : %NODE_VER%

where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] npm n'est pas disponible dans le PATH.
    pause
    exit /b 1
)

:: Verifier Python
python --version >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] Python n'est pas installe ou pas dans le PATH.
    pause
    exit /b 1
)
echo [OK] Python detecte.

:: Installer les dependances Python
echo.
echo [INSTALL] Installation des dependances Python...
cd /d "%~dp0"
python -m pip install -r requirements-dev.txt
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] Installation Python impossible.
    pause
    exit /b 1
)

:: Installer les dependances npm
echo.
echo [INSTALL] Installation des dependances npm (Electron)...
cd /d "%~dp0electron-app"
call npm ci
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] Installation Electron impossible.
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   Installation terminee avec succes !
echo ==========================================
echo.
echo Pour lancer l'application : start_desktop.bat
echo.
pause
