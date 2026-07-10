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

## Limite importante

Les clés API sont retirées de l'environnement des programmes générés, mais ces
programmes ne sont pas encore isolés du disque et du réseau par le système
d'exploitation. N'utilisez pas l'Orchestrateur pour exécuter le contenu d'un
dépôt ou d'une consigne non fiable. L'étape de durcissement suivante doit lancer
chaque projet dans un conteneur ou un compte restreint, avec un dossier monté,
le réseau coupé par défaut et des limites de mémoire, CPU et durée.

## Installations demandées par les agents

Dans l'espace de travail multi-agents, les installations passent par une
demande structurée. La politique choisie pour la tâche distingue les
dépendances du projet, les applications du compte courant et les modifications
de tout l'ordinateur. Les deux derniers niveaux demandent un accord visible à
chaque fois. Aucun accord manuel n'est mémorisé. Les demandes expirent et sont
annulées à l'arrêt de la tâche.

Ce contrôle est une règle de consentement, pas encore une isolation du système
d'exploitation. Tant que le code des agents ne tourne pas dans un conteneur ou
sous un compte restreint, un programme généré peut tenter de contourner le
courtier, appeler lui-même l'API locale ou modifier la base. Le détail du
fonctionnement et de cette limite se trouve dans
[`docs/INSTALL_PERMISSIONS.md`](docs/INSTALL_PERMISSIONS.md).

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
