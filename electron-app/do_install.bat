@echo off
cd /d "C:\Users\Tellop\claude-managed-agents\electron-app"
echo Dossier courant: %CD%
echo Installation des dependances npm...
"C:\Program Files\nodejs\npm.cmd" install
echo Code retour: %ERRORLEVEL%
echo Fin de l'installation.
