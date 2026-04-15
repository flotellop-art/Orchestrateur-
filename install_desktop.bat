@echo off
echo ==========================================
echo   Installation - Multi-Agent Desktop App
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

for /f "tokens=*" %%i in ('"C:\Program Files\nodejs\node.exe" --version 2^>nul') do set NODE_VER=%%i
echo [OK] Node.js detecte : %NODE_VER%

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
python -m pip install -r requirements_orchestrator.txt -q

:: Installer les dependances npm
echo.
echo [INSTALL] Installation des dependances npm (Electron)...
cd /d "%~dp0electron-app"
"C:\Program Files\nodejs\npm.cmd" install --include=dev

:: Generer les icones
echo.
echo [ICONS] Generation des icones...
python generate_icon.py

echo.
echo ==========================================
echo   Installation terminee avec succes !
echo ==========================================
echo.
echo Pour lancer l'application : start_desktop.bat
echo.
pause
