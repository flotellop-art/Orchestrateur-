@echo off
:: setup_autostart.bat
:: Cree une tache planifiee Windows pour lancer l'orchestrateur au login (sans fenetre visible)

set TASK_NAME=ClaudeOrchestrator
set SCRIPT_PATH=C:\Users\Tellop\claude-managed-agents\start_orchestrator.bat

echo Creation de la tache planifiee "%TASK_NAME%"...

:: Supprimer si elle existe deja
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

:: Creer un wrapper VBScript pour lancer sans fenetre
set VBS_PATH=C:\Users\Tellop\claude-managed-agents\start_silent.vbs
echo Set oShell = CreateObject("WScript.Shell") > "%VBS_PATH%"
echo oShell.Run "cmd /c ""%SCRIPT_PATH%""", 0, False >> "%VBS_PATH%"

:: Creer la tache planifiee : au login de l'utilisateur, lancer le VBScript
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "wscript.exe \"%VBS_PATH%\"" ^
  /sc ONLOGON ^
  /ru "%USERNAME%" ^
  /rl HIGHEST ^
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
