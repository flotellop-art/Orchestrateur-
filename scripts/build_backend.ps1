param(
    [string]$Python = ""
)

$ErrorActionPreference = "Stop"
$Root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$Backend = [System.IO.Path]::GetFullPath((Join-Path $Root "electron-app\backend"))

if (-not $Backend.StartsWith($Root, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Le dossier de sortie du backend sort du depot."
}

if (-not $Python) {
    $VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
    $Python = if (Test-Path -LiteralPath $VenvPython) { $VenvPython } else { "python" }
}

& $Python -c "import PyInstaller" 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller manque. Installez requirements-dev.txt avant le build."
}

if (Test-Path -LiteralPath $Backend) {
    Remove-Item -LiteralPath $Backend -Recurse -Force
}
New-Item -ItemType Directory -Path $Backend -Force | Out-Null

Push-Location $Root
try {
    & $Python -m PyInstaller --noconfirm --clean `
        --distpath $Backend `
        --workpath (Join-Path $Root "build\pyinstaller") `
        (Join-Path $Root "orchestrator.spec")
    if ($LASTEXITCODE -ne 0) { throw "La construction PyInstaller a echoue." }
} finally {
    Pop-Location
}

$Exe = Join-Path $Backend "orchestrator-backend\orchestrator-backend.exe"
if ($IsWindows -and -not (Test-Path -LiteralPath $Exe)) {
    throw "Executable backend introuvable apres le build."
}

Write-Host "Backend autonome construit dans $Backend"
