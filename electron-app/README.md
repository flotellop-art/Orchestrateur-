# Multi-Agent Orchestrator - Application Desktop

Application desktop construite avec Electron qui encapsule le serveur FastAPI Python.

## Structure

```
claude-managed-agents/
├── electron-app/          # Wrapper Electron
│   ├── main.js            # Process principal Electron
│   ├── preload.js         # Bridge securise renderer/main
│   ├── package.json       # Dependances Node.js
│   └── assets/            # Icones
│       └── icon.svg       # Icone source
├── orchestrator.py        # Serveur FastAPI
├── chat_agent.py          # Agent de chat
├── run.py                 # Point d entree serveur
├── install_desktop.bat    # Installation
├── start_desktop.bat      # Lancement en dev
└── build_desktop.bat      # Build executable
```

## Installation rapide (Windows)

### 1. Prerequis
- Node.js >= 18 (https://nodejs.org)
- Python >= 3.9 avec les dependances (requirements_orchestrator.txt)

### 2. Installer et lancer
```bat
install_desktop.bat    # Installe les dependances npm
start_desktop.bat      # Lance l app en mode developpement
```

### 3. Build executable .exe
```bat
build_desktop.bat      # Cree un .exe dans electron-app/dist/
```

## Fonctionnement

Au demarrage, Electron:
1. Affiche un splash screen
2. Cherche si le serveur Python tourne deja (GET http://127.0.0.1:8000/health)
3. Si non: lance automatiquement `python orchestrator.py`
4. Attend que le serveur soit pret (max 30s)
5. Ouvre la fenetre principale sur http://127.0.0.1:8000 (tableau de bord multi-agents)

## Icone

L icone SVG est dans `electron-app/assets/icon.svg`.
Pour la production, convertissez-la:
- Windows: icon.ico (256x256)
- macOS: icon.icns
- Linux: icon.png (512x512)

Outil en ligne: https://convertio.co/svg-ico/

## Raccourcis clavier

| Raccourci | Action |
|-----------|--------|
| Ctrl+1 | Tableau de bord |
| Ctrl+2 | Espace agents |
| Ctrl+3 | Chat |
| Ctrl+R | Recharger |
| Ctrl+Q | Quitter |
| F12 | DevTools |
| F11 | Plein ecran |
