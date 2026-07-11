@echo off
:: setup_autostart.bat
:: Cree une tache planifiee Windows pour lancer l'orchestrateur au login (sans fenetre visible)

setlocal
set "TASK_NAME=OrchestrateurDesktop"
set "SCRIPT_PATH=%~dp0start_desktop.bat"
set "AUTOSTART_DIR=%LOCALAPPDATA%\Orchestrateur"
set "VBS_PATH=%AUTOSTART_DIR%\start_silent.vbs"

if not exist "%SCRIPT_PATH%" (
    echo [ERREUR] Script de lancement introuvable : %SCRIPT_PATH%
    exit /b 1
)
if not exist "%AUTOSTART_DIR%" mkdir "%AUTOSTART_DIR%"

echo Creation de la tache planifiee "%TASK_NAME%"...

:: Supprimer si elle existe deja
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

:: Creer un wrapper VBScript pour lancer sans fenetre
echo Set oShell = CreateObject("WScript.Shell") > "%VBS_PATH%"
echo oShell.Run "cmd /c ""%SCRIPT_PATH%""", 0, False >> "%VBS_PATH%"

:: Creer la tache planifiee : au login de l'utilisateur, lancer le VBScript
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "wscript.exe \"%VBS_PATH%\"" ^
  /sc ONLOGON ^
  /ru "%USERNAME%" ^
  /rl LIMITED ^
  /f

if %ERRORLEVEL% == 0 (
    echo.
    echo [OK] Tache planifiee creee avec succes.
    echo      L'orchestrateur demarrera automatiquement a chaque login Windows.
    echo.
    echo Pour supprimer la tache : schtasks /delete /tn "%TASK_NAME%" /f
) else (
    echo.
    echo [ERREUR] Impossible de creer la tache. Essayez en tant qu'administrateur.
)
pause
