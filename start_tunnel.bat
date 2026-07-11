@echo off
setlocal EnableExtensions
title Tunnel Cloudflare - Orchestrateur
echo ============================================
echo   Tunnel Cloudflare pour Orchestrateur
echo   Expose: http://localhost:8000
echo ============================================
echo.
set "TUNNEL_NAME=%~1"

REM Refuser toute exposition distante sans cle API.
if not exist "%~dp0.env" (
    echo [ERREUR] Fichier .env absent. Le tunnel exige API_SECRET_KEY.
    echo Copiez .env.example vers .env puis configurez une cle forte.
    pause
    exit /b 1
)
set "ORCH_ENV_FILE=%~dp0.env"
powershell.exe -NoProfile -NonInteractive -Command "$lines=Get-Content -LiteralPath $env:ORCH_ENV_FILE; $key=(($lines | Where-Object {$_ -match '^API_SECRET_KEY='} | Select-Object -Last 1) -replace '^API_SECRET_KEY=','').Trim(); $hosts=(($lines | Where-Object {$_ -match '^ORCHESTRATOR_ALLOWED_HOSTS='} | Select-Object -Last 1) -replace '^ORCHESTRATOR_ALLOWED_HOSTS=','').Trim(); if([string]::IsNullOrWhiteSpace($key) -or $key.Length -lt 24){exit 1}; $remote=@($hosts -split ',' | ForEach-Object {$_.Trim()} | Where-Object {$_ -and $_ -notin @('localhost','127.0.0.1','::1')}); if($remote.Count -eq 0){exit 2}"
set "SECURITY_CHECK=%ERRORLEVEL%"
if "%SECURITY_CHECK%"=="1" (
    echo [ERREUR] API_SECRET_KEY doit contenir au moins 24 caracteres.
    pause
    exit /b 1
)
if "%SECURITY_CHECK%"=="2" (
    echo [ERREUR] ORCHESTRATOR_ALLOWED_HOSTS doit contenir le domaine du tunnel.
    pause
    exit /b 1
)
if not "%SECURITY_CHECK%"=="0" (
    echo [ERREUR] Verification de securite impossible.
    pause
    exit /b 1
)

REM Verifier que cloudflared est installe
where cloudflared >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERREUR] cloudflared n'est pas installe ou pas dans le PATH.
    echo Installez-le via : winget install Cloudflare.cloudflared
    pause
    exit /b 1
)

REM Utiliser le tunnel du fichier de configuration, ou le nom passe en argument.
if exist "%USERPROFILE%\.cloudflared\config.yml" (
    echo [INFO] Configuration Cloudflare trouvee - lancement du tunnel permanent.
    echo.
    if defined TUNNEL_NAME (
        cloudflared tunnel run "%TUNNEL_NAME%"
    ) else (
        cloudflared tunnel run
    )
) else (
    echo [INFO] Pas de tunnel permanent configure.
    echo [INFO] Lancement d'un tunnel temporaire ^(trycloudflare.com^)...
    echo.
    echo Pour configurer un tunnel permanent avec votre propre domaine,
    echo suivez les instructions dans TUNNEL_SETUP.md
    echo.
    cloudflared tunnel --url http://localhost:8000 --no-autoupdate
)

pause
