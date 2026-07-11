$Root = [System.IO.Path]::GetFullPath($PSScriptRoot)
$WshShell = New-Object -ComObject WScript.Shell
$Desktop = [System.Environment]::GetFolderPath('Desktop')
$Shortcut = $WshShell.CreateShortcut("$Desktop\Multi-Agent Orchestrator.lnk")
$Shortcut.TargetPath = Join-Path $Root "start_desktop.bat"
$Shortcut.WorkingDirectory = $Root
$Shortcut.IconLocation = Join-Path $Root "electron-app\assets\icon.ico"
$Shortcut.Description = "Multi-Agent Orchestrator"
$Shortcut.WindowStyle = 1
$Shortcut.Save()
Write-Host "Raccourci cree sur le Bureau."
