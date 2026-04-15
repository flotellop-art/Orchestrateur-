# Orchestrateur — Shell Electron (Phase 3)

Shell desktop qui embarque l'interface **Arty** (tryarty.com) dans une fenêtre
Electron et pilote le backend **FastAPI local** (`orchestrator.py`, port 8000).

## Rôle

1. Au démarrage, le shell ping `GET http://127.0.0.1:8000/api/stats`.
   - Si un Orchestrateur répond déjà → il est réutilisé tel quel.
   - Sinon, `orchestrator.py` est lancé en sous-processus Python.
2. Un splash local (`splash.html`) s'affiche tant que le backend n'a pas
   répondu (plafond 25 s).
3. Dès que le heartbeat passe, la fenêtre charge `ARTY_URL`
   (par défaut `https://tryarty.com`).
4. Arty détecte l'Orchestrateur via `/api/stats` et synchronise la clé
   Anthropic via `/api/set-key` (cf. Phase 1, PR #1).
5. À la fermeture de la fenêtre, le sous-processus Python est stoppé
   proprement (SIGTERM côté Unix, `taskkill /T` sous Windows).

## Garanties de sécurité

La fenêtre charge un domaine distant (Arty) ; les options Electron sont donc
durcies :

| Option                       | Valeur        |
|------------------------------|---------------|
| `nodeIntegration`            | `false`       |
| `contextIsolation`           | `true`        |
| `sandbox`                    | `true`        |
| `setPermissionRequestHandler`| tout refuse   |

La navigation est restreinte à une liste d'hôtes (`tryarty.com`,
`appfacade.pages.dev`, `127.0.0.1`, `localhost`). Tout autre lien s'ouvre
dans le navigateur système via `shell.openExternal`.

Le preload expose uniquement trois champs figés à Arty :

```js
window.orchestrateur = {
  isElectronShell: true,
  origin: 'http://127.0.0.1:8000',
  version: '0.3.0',
};
```

Aucune API Node, aucun accès disque : l'intégration passe par les mêmes
fetch HTTP que la version web d'Arty.

## Utilisation

```bash
cd electron-app
npm install
npm start              # lance Arty (tryarty.com)
npm run start:dev      # mode développement (logs Python visibles)
```

Variables d'environnement :

| Variable         | Défaut                | Rôle                                  |
|------------------|-----------------------|---------------------------------------|
| `ARTY_URL`       | `https://tryarty.com` | URL chargée dans la fenêtre           |
| `ORCHESTRATEUR_DEV` | `0`                | `1` pour logs Python détaillés        |

Exemples :

```bash
# Pointer vers l'env de staging Cloudflare Pages d'Arty :
ARTY_URL=https://appfacade.pages.dev npm start

# Pointer vers Arty en dev local :
ARTY_URL=http://localhost:5173 npm start
```

## Prérequis

- Node.js 18+
- Python 3.10+ avec `fastapi` + `uvicorn` (lus par `orchestrator.py`)
- Un `.venv` à la racine du repo est détecté automatiquement
  (`.venv/bin/python` ou `.venv\Scripts\python.exe`).

## Dépendance Phase 1

Ce shell suppose que `orchestrator.py` expose au minimum :

- `GET /api/stats`   — heartbeat
- `POST /api/set-key` — injection de la clé Anthropic

Ces endpoints sont fournis par la PR Phase 1 (`orchestrator.py`). Si
`orchestrator.py` est absent, le shell affiche Arty sans backend local ;
Arty basculera alors sur son mode web classique.
