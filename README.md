# Orchestrateur

Orchestrateur est un atelier local où une équipe d'agents IA construit une
application, analyse un dépôt ou produit un livrable sous le contrôle de
l'utilisateur.

La version actuelle est une **bêta**. Elle convient à des projets et consignes
de confiance. Pour exécuter du code inconnu, utilisez le mode Docker isolé.

## Les cinq priorités de la bêta

1. **Isoler le code des agents** : les commandes, tests et applications d'une
   tâche Docker s'exécutent dans des conteneurs limités, sans retour silencieux
   vers Windows.
2. **Conserver les bonnes méthodes** : un agent peut proposer une compétence,
   mais seule une personne peut la relire et l'activer.
3. **Ne pas perdre le travail** : la file est enregistrée sur disque et reprend
   les tâches interrompues après un redémarrage.
4. **Automatiser sans exposer les secrets** : les tâches récurrentes utilisent
   uniquement des canaux Telegram, Slack ou Discord préparés par
   l'administrateur.
5. **Distribuer une vraie application** : tests automatiques, numéro de version,
   licence, serveur autonome et installateur Windows sont réunis dans le dépôt.

## Ce que fait le produit

- un chef crée des spécialistes et répartit le travail ;
- les agents Claude, Gemini et OpenAI peuvent travailler en parallèle ;
- une vérification croisée fait relire un résultat par plusieurs modèles ;
- les décisions, outils, fichiers, coûts et demandes d'accès restent visibles ;
- les tâches peuvent être mises en pause, reprises, limitées par un budget et
  récupérées après un redémarrage ;
- les bonnes méthodes deviennent des compétences réutilisables après validation
  humaine ;
- les tâches récurrentes peuvent envoyer leur résultat vers Telegram, Slack ou
  Discord ;
- l'application simple crée et lance une petite application Flask à partir
  d'une description en français.

## Sécurité en quelques mots

Le mode recommandé exécute le code de chaque tâche dans Docker avec :

- un utilisateur non administrateur ;
- le réseau coupé par défaut ;
- des limites de mémoire, processeur et nombre de processus ;
- un système de fichiers racine en lecture seule ;
- uniquement le dossier de la tâche monté en écriture.

Si Docker manque ou est arrêté, le mode isolé échoue clairement. Orchestrateur
ne revient pas silencieusement à une exécution locale. Le mode local reste
disponible pour le développement et doit être considéré comme non isolé.

Les commandes et tests ordinaires n'ont pas de réseau. Deux exceptions sont
séparées : l'installation approuvée de paquets et l'aperçu d'une application
web. Cette dernière reste désactivée par défaut, car publier un port avec le
réseau Docker `bridge` donne aussi un accès sortant au conteneur.

Les installations demandées par les agents disposent de cinq niveaux : tout
refuser, demander, projet automatique, compte utilisateur et administrateur.
Les accès sensibles demandent un nouvel accord à chaque fois.

Consultez [SECURITY.md](SECURITY.md),
[les autorisations d'installation](docs/INSTALL_PERMISSIONS.md) et
[l'architecture](docs/ARCHITECTURE_V2.md) avant un déploiement distant.

## Démarrage pour contribuer

Prérequis : Python 3.12, Node.js 24 et, pour l'isolation, Docker Desktop ou
Docker Engine.

```powershell
git clone https://github.com/flotellop-art/Orchestrateur-.git
cd Orchestrateur-
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
.\scripts\build_sandbox.ps1
.venv\Scripts\python orchestrator.py
```

Ouvrez ensuite <http://127.0.0.1:8000>.

La construction de l'image Docker nécessite que Docker soit démarré. Elle crée
l'image locale `orchestrator-sandbox:0.2.0`; aucun agent ne peut choisir ou
télécharger lui-même une autre image.

Les principales pages sont :

| Page | Usage |
| --- | --- |
| `/` | création rapide d'une application |
| `/workspace` | équipe multi-agents et fichiers |
| `/control` | activité, coûts et supervision |
| `/memory` | notes et souvenirs persistants |
| `/skills` | compétences proposées et approuvées |
| `/automations` | tâches programmées et notifications |

## Configuration

Copiez `.env.example` vers `.env` dans le dépôt en développement. Dans
l'application installée, placez `.env` dans le dossier de données utilisateur.

Variables importantes :

| Variable | Rôle |
| --- | --- |
| `ANTHROPIC_API_KEY` | modèles Claude |
| `OPENAI_API_KEY` | modèles OpenAI et recherche mémoire facultative |
| `GEMINI_API_KEY` | modèles Gemini |
| `API_SECRET_KEY` | accès distant, 24 caractères minimum |
| `ORCHESTRATOR_ALLOWED_HOSTS` | domaines distants explicitement admis |
| `ORCHESTRATOR_TARGET_ROOTS` | dossiers locaux lisibles en mode audit |
| `ORCHESTRATOR_DEFAULT_EXECUTION` | `docker` recommandé ou `local` non isolé |
| `ORCHESTRATOR_SANDBOX_IMAGE` | image Docker approuvée par l'administrateur |
| `ORCHESTRATOR_SANDBOX_ALLOW_INSTALL_NETWORK` | réseau temporaire pour les installations Python/npm approuvées |
| `ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK` | aperçu web et sorties réseau de l'application, désactivés par défaut |
| `ORCHESTRATOR_QUEUE_WORKERS` | nombre de tâches traitées en parallèle |
| `ORCHESTRATOR_MESSAGING_CHANNELS` | noms des canaux et noms des variables qui contiennent leurs secrets |
| `ORCHESTRATOR_MESSAGING_ALLOWED_CHANNELS` | canaux que les planifications ont le droit d'utiliser |

Les jetons et webhooks restent dans des variables séparées (`TELEGRAM_BOT_TOKEN`,
`SLACK_WEBHOOK_URL`, etc.). Les URL et secrets ne peuvent pas être fournis par
un agent ou par une planification. Un exemple complet se trouve dans
[docs/AUTOMATIONS.md](docs/AUTOMATIONS.md).

## Application desktop autonome

L'installateur embarque le serveur Python construit avec PyInstaller. La
personne qui installe Orchestrateur n'a donc pas besoin d'installer Python.

```powershell
python -m pip install -r requirements-dev.txt
cd electron-app
npm ci
cd ..
.\build_desktop.bat
```

Les exécutables sont produits dans `electron-app/dist/`. Docker reste un
prérequis séparé pour choisir l'exécution réellement isolée. L'installateur
n'embarque pas le moteur Docker ni une image Docker déjà construite :
l'administrateur doit démarrer Docker puis construire l'image fournie. Le script
se trouve dans `resources/tools/build_sandbox.ps1` à l'intérieur du dossier de
l'application installée.

Le serveur autonome n'exige pas Python pour démarrer. En revanche, le mode
`local` et les outils externes d'un projet ne sont pas rendus autonomes par
l'installateur ; la version installée doit donc utiliser le mode Docker pour
exécuter du code généré.

Les binaires de cette bêta ne sont pas encore signés. Windows peut afficher un
avertissement SmartScreen : vérifiez le fichier `SHA256SUMS.txt` publié avec la
version avant de lancer l'installateur.

Voir [le guide de construction](docs/PACKAGING.md).

## Tests

```powershell
.venv\Scripts\python -m pytest -q
```

GitHub vérifie la suite sur Windows et Linux et construit aussi le serveur
Windows autonome. La stratégie complète se trouve dans
[docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md).

## État du projet

Version : `0.2.0-beta.1`.

Les changements sont consignés dans [CHANGELOG.md](CHANGELOG.md). Une version
stable demandera encore des essais réels de Docker Desktop, des installateurs
et des migrations depuis d'anciennes bases, ainsi qu'une signature de code pour
les binaires Windows.

## Licence

MIT — voir [LICENSE](LICENSE).
