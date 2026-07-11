# Exécution isolée des tâches

Le mode `docker` est maintenant relié au moteur de tâches. Les commandes, les
tests, les installations Python/npm et les applications produits par un agent
sont lancés par `sandbox_runtime.py`. Le mode `local` reste proposé uniquement
pour le développement et n'apporte aucune isolation.

## Règle principale

Il n'existe aucun repli vers la machine hôte. Si Docker, le moteur Linux,
l'image approuvée ou une protection demandée manquent, la tâche Docker est
refusée avec une erreur visible.

```text
demande de l'agent
        |
        v
validation de la tâche, du dossier et de la commande
        |
        v
vérification de Docker et de l'image locale approuvée
        |
        v
conteneur limité, identifié par des labels Orchestrateur
        |
        +---- délai, pause ou arrêt ----> arrêt puis suppression
        |
        v
résultat borné puis suppression du conteneur
```

Une commande ou une suite de tests utilise un conteneur court. Une application
web utilise un conteneur suivi jusqu'à son arrêt ou jusqu'à sa durée maximale.
Les appels aux fournisseurs de modèles restent naturellement dans le serveur :
seul le code du projet est exécuté dans Docker.

## Protections appliquées

- image présente localement, jamais téléchargée à la demande d'un agent
  (`--pull=never`) ;
- conteneurs Linux et Docker 23.0 minimum (requis notamment pour le profil
  `seccomp=builtin`) ;
- utilisateur numérique non administrateur ;
- seul le dossier de la tâche est monté en lecture/écriture dans `/workspace` ;
- racine du conteneur en lecture seule ;
- petits espaces temporaires sans exécution, sans bit SUID et sans périphérique ;
- aucun réseau pour les commandes et tests ordinaires ;
- capacités Linux supprimées et `no-new-privileges` activé ;
- limites de processeur, mémoire, processus et fichiers ouverts ;
- espace IPC séparé, aucun redémarrage automatique et processus `init` ;
- aucune copie générale de l'environnement de l'hôte ;
- exécutable et arguments validés séparément, sans interpolation par un shell ;
- délais et sorties bornés, puis arrêt gracieux et forcé si nécessaire ;
- récupération au démarrage des conteneurs survivants portant les labels de
  cette installation ;
- outils MCP refusés en mode Docker, car ils s'exécuteraient sinon sur l'hôte.

Docker conserve son profil seccomp. Aucun socket Docker, dossier système, secret
de l'hôte ou périphérique n'est monté. Le dossier de travail reste modifiable :
il faut donc conserver les sources importantes ailleurs ou sous Git.

## Construire l'image approuvée

L'image attendue par défaut est `orchestrator-sandbox:0.2.0`. Elle contient
Python 3.12, pytest, Flask, Node.js et npm. Elle n'est pas téléchargée et elle
n'est pas incorporée comme image prête à l'emploi dans l'installateur.

Depuis le dépôt :

```powershell
.\scripts\build_sandbox.ps1
```

Le script vérifie Docker puis construit `sandbox/Dockerfile`. Dans l'application
Windows installée, les mêmes fichiers se trouvent dans `resources/sandbox/` et
le script dans `resources/tools/build_sandbox.ps1`.

Une organisation peut construire et contrôler sa propre image, puis imposer une
référence immuable :

```text
ORCHESTRATOR_SANDBOX_IMAGE=registre/interne/orchestrator@sha256:...
```

Ne donnez jamais à un agent la possibilité de modifier cette variable ou de
choisir une image. Sous Windows, Docker Desktop doit utiliser les conteneurs
Linux.

## Réseau

Le réseau est séparé en deux autorisations d'administrateur :

- `ORCHESTRATOR_SANDBOX_ALLOW_INSTALL_NETWORK=true` permet seulement aux
  installations Python/npm déjà autorisées par la politique de la tâche de
  joindre leur registre ;
- `ORCHESTRATOR_SANDBOX_ALLOW_APP_NETWORK=true` permet de publier l'aperçu
  d'une application sur `127.0.0.1`.

Les commandes et tests suivants restent sans réseau. Ces deux variables sont
indépendantes : autoriser une installation ne donne pas Internet à l'application
ensuite exécutée.

Une limite demeure : Docker `bridge` est nécessaire pour publier un port local
et autorise aussi les sorties réseau du conteneur. L'aperçu dynamique est donc
désactivé par défaut. Activez-le seulement pour une application de confiance.

## Autres réglages

- `ORCHESTRATOR_DOCKER_BIN` : chemin du client Docker ;
- `ORCHESTRATOR_SANDBOX_IMAGE` : image locale approuvée ;
- `ORCHESTRATOR_SANDBOX_NAMESPACE` : nom stable de l'installation ;
- `ORCHESTRATOR_SANDBOX_MEMORY` : mémoire maximale, `1g` par défaut ;
- `ORCHESTRATOR_SANDBOX_CPUS` : part de processeur, `1.0` par défaut ;
- `ORCHESTRATOR_SANDBOX_PIDS` : nombre de processus, `128` par défaut ;
- `ORCHESTRATOR_SANDBOX_TIMEOUT` : durée maximale ordinaire, 300 secondes par
  défaut.

## Vérification

Les tests habituels simulent Docker :

```powershell
python -m pytest -q tests/test_sandbox_runtime.py
```

Après avoir construit l'image, un contrôle facultatif lance un vrai conteneur :

```powershell
$env:ORCHESTRATOR_DOCKER_SMOKE = "1"
python -m pytest -q tests/test_sandbox_runtime.py
```

## Limites connues

- Docker protège la machine hôte, mais son démon devient lui-même une dépendance
  de sécurité à maintenir à jour.
- Le conteneur peut supprimer ou altérer les fichiers de sa propre tâche.
- Une application avec réseau peut joindre Internet tant qu'un réseau sortant
  plus fin ou un proxy dédié n'est pas ajouté.
- WinGet agit hors du conteneur après un accord humain ponctuel ; cette opération
  possède donc les droits du compte Windows concerné.
- Les images internes devraient être contrôlées par empreinte et, à terme, par
  signature.
