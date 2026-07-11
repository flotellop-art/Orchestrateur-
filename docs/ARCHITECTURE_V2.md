# Architecture 0.2

## But

Orchestrateur reste un atelier visuel multi-agents, mais l'execution de code,
la reprise apres panne et les extensions apprises sont maintenant separees du
moteur de conversation. Cette separation evite qu'une simple boucle Python en
memoire porte seule toutes les responsabilites.

## Vue d'ensemble

```text
Interface desktop / navigateur
            |
         API locale
            |
   +--------+---------+------------------+
   |                  |                  |
File SQLite      Compétences        Automatisations
durable          validées           et notifications
   |
Moteur chef + agents
   |
Courtier d'outils
   |
Sandbox Docker par tâche
   |
Dossier de travail monté, ressources limitées, réseau coupé
```

## Decisions principales

### Exécution

- `docker` est le mode recommandé et le seul annoncé comme isolé.
- L'absence de Docker provoque un refus clair ; il n'existe aucun retour
  silencieux vers le compte Windows.
- `local` reste disponible pour le développement, avec un avertissement
  explicite. Il n'est pas adapté à du contenu non fiable.
- Le conteneur perd ses privilèges, tourne sans administrateur, possède un
  système de fichiers racine en lecture seule, reçoit uniquement le dossier de
  la tâche et dispose de limites mémoire, CPU et processus.
- Le réseau est coupé par défaut. Une installation approuvée peut recevoir un
  accès réseau temporaire ; le programme exécuté ensuite n'en hérite pas.
- L'aperçu d'une application possède une autorisation réseau séparée et reste
  désactivé par défaut, car Docker `bridge` autorise aussi les sorties Internet.

### Tâches durables

- SQLite est la source de vérité, pas la mémoire du processus FastAPI.
- Chaque travail possède un état, un bail limité dans le temps, un nombre de
  tentatives et une date de prochaine tentative.
- Au démarrage, un travail resté `running` est remis en file. La reprise
  reconstruit le contexte à partir de la base et des fichiers déjà produits.
- L'ajout en file est idempotent : deux clics ne créent pas deux exécutions.

### Compétences

- Une compétence est uniquement un document de procédure ; elle n'exécute pas
  de code et ne contient pas de secret.
- Un agent peut proposer une compétence, jamais l'activer lui-même.
- Un humain doit l'approuver. Seules les compétences `active` sont injectées
  dans une nouvelle tâche.
- Les noms et tailles sont bornés, et les exports utilisent un chemin sûr.

### Automatisations et messageries

- Une planification crée une tâche normale dans la même file durable.
- Telegram, Slack et Discord sont uniquement des sorties de notification dans
  cette version. Ils ne peuvent pas donner directement une commande à un agent.
- Les adresses et jetons sont configurés par l'administrateur du serveur. Un
  agent ou une requête de planification ne peut fournir aucune URL arbitraire.
- Les erreurs affichées ne contiennent jamais les secrets des canaux.

### Données et paquet desktop

- Les ressources immuables restent dans le paquet.
- La base, les projets et la configuration sont placés dans le dossier de
  données Electron via `ORCHESTRATOR_DATA_DIR`.
- Le serveur Python est empaqueté avec PyInstaller. L'utilisateur final n'a
  plus besoin d'installer Python pour lancer Orchestrateur.
- Le Dockerfile est embarqué, mais pas le moteur ni l'image Docker. Un
  administrateur doit construire l'image approuvée séparément.
- Les exécutables de la bêta ne sont pas encore signés ; les empreintes
  SHA-256 accompagnent donc chaque publication.

## Limites à réévaluer

- Docker Desktop reste nécessaire pour la vraie isolation sur Windows.
- Le dossier de la tâche est volontairement modifiable par son conteneur ; il
  faut donc traiter ses fichiers comme non fiables à la sortie.
- Les applications Windows installées avec WinGet agissent hors du conteneur,
  après un accord ponctuel. Un composant administrateur dédié serait préférable
  avant une version stable.
- La file SQLite convient à une machine. Une exécution sur plusieurs machines
  demanderait une base partagée et un système de messages externe.
- Les notifications sont sortantes uniquement ; un futur canal entrant devra
  posséder une authentification, une liste d'utilisateurs et une séparation des
  droits par profil.
