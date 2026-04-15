@echo off
title Cloudflare Tunnel - agent.tryarty.com
echo ============================================
echo   Cloudflare Tunnel pour Claude Agents
echo   Expose: http://localhost:8002
echo ============================================
echo.

REM Verifier que cloudflared est installe
where cloudflared >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] cloudflared n'est pas installe ou pas dans le PATH.
    echo Installez-le via : winget install Cloudflare.cloudflared
    pause
    exit /b 1
)

REM Verifier si un tunnel nomme est configure (agent.tryarty.com)
if exist "%USERPROFILE%\.cloudflared\config.yml" (
    echo [INFO] Configuration trouvee - Lancement du tunnel permanent ^(agent.tryarty.com^)
    echo.
    cloudflared tunnel run agent-tryarty
) else (
    echo [INFO] Pas de tunnel permanent configure.
    echo [INFO] Lancement d'un tunnel temporaire ^(trycloudflare.com^)...
    echo.
    echo Pour configurer un tunnel permanent sur agent.tryarty.com,
    echo suivez les instructions dans TUNNEL_SETUP.md
    echo.
    cloudflared tunnel --url http://localhost:8002 --no-autoupdate
)

pause
