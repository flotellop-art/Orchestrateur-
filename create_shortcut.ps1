# Cree (ou met a jour) un raccourci sur le Bureau vers le lanceur auto-update.
# Le chemin est detecte automatiquement (emplacement de ce script) : aucun chemin
# code en dur, le dossier peut donc etre deplace/renomme sans rien casser.
$root = Split-Path -Parent $MyInvocation.MyCommand.Definition
$WshShell = New-Object -ComObject WScript.Shell
$Desktop = [System.Environment]::GetFolderPath('Desktop')
$Shortcut = $WshShell.CreateShortcut("$Desktop\Multi-Agent Orchestrator.lnk")
$Shortcut.TargetPath = Join-Path $root "Orchestrateur.bat"
$Shortcut.WorkingDirectory = $root
$icon = Join-Path $root "electron-app\assets\icon.ico"
if (Test-Path $icon) { $Shortcut.IconLocation = $icon }
$Shortcut.Description = "Multi-Agent Orchestrator (mise a jour auto + lancement)"
$Shortcut.WindowStyle = 1
$Shortcut.Save()
Write-Host "Raccourci cree sur le Bureau : Multi-Agent Orchestrator"
Write-Host ("Cible : " + $Shortcut.TargetPath)
