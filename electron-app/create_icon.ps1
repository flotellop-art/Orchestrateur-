# Script pour créer une icône SVG simple et la convertir
# Nécessite Inkscape ou ImageMagick pour la conversion PNG->ICO

# Crée une icône SVG simple
$svg = @'
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="256" height="256">
  <rect width="256" height="256" rx="32" fill="#2b2b2b"/>
  <circle cx="128" cy="96" r="40" fill="#E8825A"/>
  <rect x="48" y="156" width="160" height="12" rx="6" fill="#E8825A" opacity="0.8"/>
  <rect x="64" y="180" width="128" height="10" rx="5" fill="#555"/>
  <rect x="80" y="202" width="96" height="10" rx="5" fill="#444"/>
</svg>
'@

$svgPath = Join-Path $PSScriptRoot "assets\icon.svg"
New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot "assets") | Out-Null
$svg | Out-File -FilePath $svgPath -Encoding UTF8
Write-Host "SVG créé: $svgPath"
Write-Host "Pour convertir en ICO, utilisez: https://convertio.co/svg-ico/"
