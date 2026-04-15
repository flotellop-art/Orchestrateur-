$WshShell = New-Object -ComObject WScript.Shell
$Desktop = [System.Environment]::GetFolderPath('Desktop')
$Shortcut = $WshShell.CreateShortcut("$Desktop\Multi-Agent Orchestrator.lnk")
$Shortcut.TargetPath = "C:\Users\Tellop\claude-managed-agents\start_desktop.bat"
$Shortcut.WorkingDirectory = "C:\Users\Tellop\claude-managed-agents"
$Shortcut.IconLocation = "C:\Users\Tellop\claude-managed-agents\electron-app\assets\icon.ico"
$Shortcut.Description = "Multi-Agent Orchestrator"
$Shortcut.WindowStyle = 1
$Shortcut.Save()
Write-Host "Raccourci mis a jour"
