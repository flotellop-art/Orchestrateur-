# Application desktop Orchestrateur

Ce dossier contient l'enveloppe Electron de l'application. En développement,
elle lance `orchestrator.py` avec le Python du dépôt. Dans un paquet installé,
elle lance le serveur autonome placé dans `resources/backend/`.

## Développement Windows

Prérequis : Python 3.12 et Node.js 24. Depuis la racine du dépôt :

```powershell
python -m pip install --require-hashes -r requirements-lock.txt
cd electron-app
npm ci
npm start
```

Le script `start_desktop.bat`, à la racine, effectue seulement le lancement. Il
ne ferme aucun autre processus Electron et ne tue aucun programme attaché à un
port.

## Construction

Depuis la racine du dépôt :

```powershell
.\build_desktop.bat
```

Les sorties se trouvent dans `electron-app/dist/` :

- `Multi-Agent Orchestrator Setup <version>.exe` : installateur NSIS ;
- `Multi-Agent Orchestrator <version>.exe` : version portable.

Le serveur Python, la licence, le numéro de version et les fichiers nécessaires
à la construction du bac à sable sont ajoutés avec `extraResources`. Docker et
l'image Docker déjà construite ne sont pas inclus.

## Démarrage et données

Electron :

1. choisit un port libre sur `127.0.0.1` ;
2. crée un jeton d'instance aléatoire ;
3. lance le serveur avec son propre dossier de données utilisateur ;
4. attend une réponse `/health` qui correspond à la version et au jeton ;
5. ouvre ensuite la fenêtre principale.

La base, les projets et le fichier `.env` de l'application installée restent
dans le dossier `userData` d'Electron, jamais dans les ressources immuables.

## Sécurité de la fenêtre

La fenêtre utilise l'isolation de contexte, le bac à sable Electron et une
politique de navigation limitée à l'origine locale choisie au démarrage. Les
liens externes sont ouverts séparément et les fonctions système exposées à la
page sont réduites au strict nécessaire.

## Raccourcis

| Raccourci | Action |
| --- | --- |
| `Ctrl+1` | tableau de bord |
| `Ctrl+2` | chat |
| `Ctrl+R` | recharger |
| `Ctrl+Q` | quitter |
| `F11` | plein écran |
| `F12` | outils de développement |

Le guide complet de construction et de publication se trouve dans
[`../docs/PACKAGING.md`](../docs/PACKAGING.md).
