# Construire l'application Windows autonome

## Ce que contient le paquet

L'installateur et la version portable embarquent :

- le serveur Orchestrateur construit avec PyInstaller ;
- les pages de l'interface ;
- l'exécutable fourni par le SDK Claude ;
- le numéro de version et la licence MIT ;
- le Dockerfile du bac à sable et son script de construction.

La personne qui lance l'application n'a pas besoin d'installer Python. En
revanche, Docker Desktop et l'image `orchestrator-sandbox:0.2.0` restent des
prérequis séparés pour exécuter le code des agents de façon isolée. L'image
Docker n'est pas embarquée dans l'installateur.

## Prérequis de construction

- Windows 10 ou 11 ;
- Python 3.12 ;
- Node.js 24 et npm ;
- les dépendances verrouillées de `requirements-dev.txt` ;
- Docker uniquement si vous voulez aussi construire ou essayer le bac à sable.

## Construction locale

Depuis la racine du dépôt :

```powershell
python -m pip install -r requirements-dev.txt
cd electron-app
npm ci
cd ..
.\build_desktop.bat
```

Le script construit d'abord `orchestrator-backend.exe`, puis demande à
electron-builder de créer deux fichiers dans `electron-app/dist/` : un
installateur NSIS et une version portable.

Pour préparer aussi l'isolation Docker :

```powershell
.\scripts\build_sandbox.ps1
```

## Organisation à l'exécution

- ressources du programme : dossier `resources` de l'application ;
- serveur autonome : `resources/backend/orchestrator-backend/` ;
- Dockerfile : `resources/sandbox/Dockerfile` ;
- outil de construction Docker : `resources/tools/build_sandbox.ps1` ;
- base, projets et configuration : dossier `userData` d'Electron ;
- variable transmise au serveur : `ORCHESTRATOR_DATA_DIR`.

Electron choisit un port local libre à chaque démarrage et vérifie le numéro de
version ainsi qu'un jeton d'instance aléatoire avant d'afficher le serveur. Il
ne se rattache donc pas à un ancien serveur qui occuperait un port fixe.

Il ne faut jamais ajouter `.env`, `apps.db`, les projets utilisateur ou les
jetons à `extraResources`.

## Vérifications avant publication

La vérification minimale du serveur autonome est :

```powershell
.\scripts\build_backend.ps1 -Python python
python scripts\smoke_backend.py `
  electron-app\backend\orchestrator-backend\orchestrator-backend.exe
```

Le second script démarre le binaire dans un dossier temporaire, vérifie
`/health`, puis arrête le processus. Le travail de publication vérifie aussi :

- toute la suite de tests ;
- la présence et une taille minimale des deux exécutables Windows ;
- la présence du serveur, de la licence, du numéro de version et des fichiers
  Docker dans le paquet décompressé ;
- les empreintes SHA-256 de chaque exécutable.

## Publier une version

1. Mettre le même numéro dans `VERSION` et `electron-app/package.json`.
2. Mettre à jour `CHANGELOG.md`.
3. Fusionner une révision dont les tests passent.
4. Créer et pousser le tag exact, par exemple `v0.2.0-beta.1`.

Le travail `.github/workflows/release.yml` refuse un tag qui ne correspond pas
à `VERSION`. Il publie les exécutables et `SHA256SUMS.txt` dans la même commande
de création de version GitHub. Un tag contenant `-` est publié comme
**préversion** et ne remplace pas la dernière version stable.

## Avertissement sur la bêta

Le flux actuel ne signe pas les exécutables Windows. SmartScreen peut donc
afficher un avertissement. Avant une diffusion stable, il faudra ajouter un
certificat de signature de code conservé dans les secrets GitHub et signer les
deux exécutables. Pour la bêta, comparez toujours l'empreinte du fichier reçu à
`SHA256SUMS.txt` publié sur GitHub.

L'installateur rend le serveur autonome, pas tous les outils imaginables d'un
projet. Le mode `local` reste destiné au développement depuis les sources ; la
version installée doit utiliser Docker pour l'exécution de code généré.
