param(
    [string]$Tag = "orchestrator-sandbox:0.2.0"
)

$ErrorActionPreference = "Stop"
$Root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$Dockerfile = Join-Path $Root "sandbox\Dockerfile"

& docker version --format "{{.Server.Version}}" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Docker est absent ou arrêté. Démarrez Docker Desktop puis réessayez."
}

& docker build --pull --tag $Tag --file $Dockerfile (Join-Path $Root "sandbox")
if ($LASTEXITCODE -ne 0) { throw "La construction de l'image isolée a échoué." }

Write-Host "Image isolée prête : $Tag"
