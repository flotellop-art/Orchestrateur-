# Sécurité de l'Orchestrateur

L'Orchestrateur peut écrire et lancer du code sur la machine qui l'héberge. Sa
clé API donne donc un accès d'administration complet et ne doit jamais être
partagée avec un utilisateur non fiable.

## Configuration sûre par défaut

Sans `API_SECRET_KEY`, les opérations sensibles n'acceptent que les connexions
directes provenant de `127.0.0.1` ou `::1`. Les requêtes transmises par un proxy
ou un tunnel sont refusées. Le serveur écoute par défaut sur `127.0.0.1`.

Pour un accès distant :

1. générer une clé avec `python -c "import secrets; print(secrets.token_urlsafe(32))"` ;
2. définir `API_SECRET_KEY` dans `.env` ;
3. ajouter le domaine exact dans `ORCHESTRATOR_ALLOWED_HOSTS` ;
4. transmettre la clé uniquement avec `X-API-Key` ou `Authorization: Bearer`.

Les clés dans les paramètres d'URL ne sont pas acceptées.

## Audit d'un dépôt local

Les chemins locaux sont désactivés tant que `ORCHESTRATOR_TARGET_ROOTS` est
vide. Cette variable contient les dossiers parents dans lesquels l'application
peut lire un dépôt Git. Exemple Windows :

```dotenv
ORCHESTRATOR_TARGET_ROOTS=C:\Users\vous\repos;D:\travail
```

Les URL distantes sont limitées aux dépôts `https://github.com/...` et le jeton
Git n'est pas placé dans la ligne de commande ni dans l'URL du dépôt.

## Frontière d'exécution

Le mode `docker` exécute les commandes, tests et applications du projet dans un
conteneur non administrateur. Il ne monte que le dossier de la tâche, coupe le
réseau ordinaire, impose des limites de mémoire, processeur, processus et durée,
et échoue si Docker ou l'image approuvée manque. Les clés API du serveur ne sont
pas transmises au conteneur et les outils MCP sont refusés dans ce mode.

Le mode `local` n'offre pas ces protections et reste réservé au développement.
Docker réduit fortement l'impact du code hostile, mais ce n'est pas une machine
virtuelle : maintenez Docker à jour et traitez les fichiers produits comme non
fiables. L'aperçu d'une application web utilise le réseau Docker `bridge`, qui
permet aussi des sorties Internet ; il est désactivé par défaut.

## Installations demandées par les agents

Dans l'espace de travail multi-agents, les installations passent par une
demande structurée. La politique choisie pour la tâche distingue les
dépendances du projet, les applications du compte courant et les modifications
de tout l'ordinateur. Les deux derniers niveaux demandent un accord visible à
chaque fois. Aucun accord manuel n'est mémorisé. Les demandes expirent et sont
annulées à l'arrêt de la tâche.

Ce contrôle est une règle de consentement distincte de l'isolation. En mode
Docker, Python et npm restent dans le conteneur et écrivent leurs fichiers dans
le dossier de la tâche. Une installation WinGet agit en revanche sur Windows,
hors du conteneur, uniquement après l'accord ponctuel exigé par son niveau. En
mode `local`, le code possède les droits du compte qui lance Orchestrateur. Le
détail du fonctionnement et de ces limites se trouve dans
[`docs/INSTALL_PERMISSIONS.md`](docs/INSTALL_PERMISSIONS.md).

## Clé dans l'interface

La clé saisie dans **Contrôle > Réglages** reste uniquement dans la session de
l'onglet. Elle n'est pas écrite dans le stockage persistant du navigateur et
disparaît quand l'onglet est fermé. Au chargement de l'interface, une éventuelle
clé laissée dans le stockage persistant par une ancienne version est supprimée
sans être recopiée. Les pages `/chat`, `/memory`, `/skills` et
`/automations` sont des coquilles publiques sans donnée sensible ; leurs
routes `/api/...` restent protégées.

L'activation ou le refus d'une compétence exige toujours une
`API_SECRET_KEY` forte configurée sur le serveur, même depuis la machine
locale. La session de revue à usage unique complète cette clé mais ne la
remplace pas.

## Critères de contrôle

- une requête distante sans clé reçoit un refus ;
- un faux en-tête `Host` est rejeté, même depuis la machine locale ;
- une clé placée dans `?api_key=` ne permet pas l'accès ;
- un fichier généré hors de `app.py` et `requirements.txt` est rejeté ;
- une dépendance générée autre que Flask est rejetée ;
- un chemin local hors des racines déclarées est rejeté ;
- les processus enfants ne reçoivent pas les clés et jetons du serveur.
- les formes directes usuelles de pip ou npm sont réorientées vers le courtier ;
- une demande expirée, rejouée ou liée à une autre tâche est refusée ;
- aucun accord manuel ne peut être mémorisé ;
- le build ne lance pas d'installation issue de `requirements.txt`.
- une tâche Docker ne revient jamais silencieusement au mode local ;
- les commandes et tests Docker ordinaires n'ont pas de réseau ;
- les conteneurs suivis sont arrêtés à l'arrêt du serveur et récupérés au
  démarrage suivant.
