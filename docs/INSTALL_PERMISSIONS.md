# Autorisations d'installation des agents

## Objectif

Les agents peuvent demander une dépendance ou une application sans recevoir un
droit général d'installation. Ils donnent uniquement le gestionnaire, le nom,
la version exacte, la portée et une raison. L'Orchestrateur valide la demande,
calcule son niveau d'accès et construit lui-même la commande.

## Niveaux proposés au démarrage d'une tâche

| Choix | Dépendance du projet | Application du compte | Tout l'ordinateur |
| --- | --- | --- | --- |
| Aucune installation | refusée | refusée | refusée |
| Demander à chaque fois | accord demandé | refusée | refusée |
| Projet automatique | automatique | refusée | refusée |
| Compte utilisateur | automatique | accord demandé | refusée |
| Administrateur | automatique | accord demandé | accord demandé à chaque fois |

Chaque demande manuelle ne vaut qu'une fois. Aucun accord n'est mémorisé : une
nouvelle application du compte ou de la machine entraîne une nouvelle question.

## Installations prises en charge

- Python : version exacte demandée à PyPI dans `.orchestrator/python`, avec des
  paquets binaires uniquement et sans toucher au Python de l'Orchestrateur. Une
  politique globale imposée par l'administrateur de la machine peut rediriger
  cet index.
- npm : version exacte dans un espace privé de la tâche, avec configuration
  vierge, registre officiel imposé et scripts toujours refusés.
- winget : identifiant et version exacts depuis la source officielle Windows,
  pour le compte courant ou toute la machine.

Après un succès, `orchestrator-dependencies.json` enregistre les dépendances
directes et applications demandées afin que l'export reste compréhensible.

Les URL, chemins locaux, options libres, versions flottantes et sources
arbitraires sont refusés. `run_command` réoriente les formes usuelles de pip et
npm vers le courtier. Le bouton « Build & Test » réutilise uniquement les
dépendances déjà approuvées et n'installe plus `requirements.txt`. Les serveurs
MCP ne sont plus téléchargés silencieusement.

## Parcours d'une demande

1. L'agent appelle `install_package` avec des champs structurés.
2. Le serveur crée un plan immuable et son empreinte SHA-256.
3. La politique de la tâche refuse, autorise ou met le plan en attente.
4. En cas d'attente, l'espace de travail affiche l'agent, sa raison, le paquet,
   la version, la source, la destination et la commande construite.
5. L'utilisateur refuse ou autorise une fois.
6. Le serveur exécute sans shell, journalise le résultat puis reprend l'agent.

Une demande expire après cinq minutes. Un redémarrage ou l'arrêt de la tâche
annule les demandes en attente. Une seule installation s'exécute à la fois par
tâche et dix demandes au maximum peuvent attendre.

## Contrat HTTP et stockage

- `GET /api/tasks/{id}/install-requests?status=pending` recharge les demandes.
- `POST /api/tasks/{id}/install-requests/{request_id}/decision` accepte
  `allow_once` ou `deny`.
- `tasks.install_policy` contient la limite choisie.
- `install_requests` conserve localement le plan, la décision, l'expiration et
  le résultat jusqu'à la suppression de la tâche. Ce journal n'est pas un audit
  inviolable.

La mise à jour d'une demande est atomique : une seconde réponse, une réponse
ancienne ou une réponse destinée à une autre tâche est refusée.

## Limite de sécurité connue

Ce mécanisme rend les demandes visibles et évite les installations accidentelles,
mais ce n'est pas encore une frontière système infranchissable. Un agent peut
écrire un programme Python ou Node puis l'exécuter ; ce programme tourne encore
avec les droits du compte qui héberge l'Orchestrateur et peut tenter de
télécharger, installer, appeler lui-même les routes HTTP locales ou modifier la
base. Sans clé API, un programme hostile possédant les mêmes droits peut même
tenter d'approuver sa propre demande. Le système protège donc les erreurs et les
agents coopératifs, pas du code hostile.

Une source officielle ne garantit pas qu'un paquet ou son éditeur est fiable.
Une application winget est installée pour l'utilisateur mais n'est pas lancée
automatiquement par l'agent. Une installation machine peut afficher l'accord
natif Windows ou échouer si l'élévation n'est pas disponible.

Pour rendre ces niveaux impossibles à contourner, une étape future devra lancer
le code des agents dans un conteneur ou sous un compte Windows restreint. Le
courtier d'installation devra rester en dehors de cet espace isolé et être le
seul composant autorisé à installer. Les installations administrateur devront
alors passer par un petit composant dédié et par l'autorisation native Windows.

## Points à réévaluer

- ajout de Homebrew, Flatpak ou apt sur d'autres systèmes ;
- vérification de l'éditeur, de la taille et des dépendances avant accord ;
- désinstallation et retour arrière ;
- remplacement de l'arrêt « au mieux » par un Job Object Windows dédié ;
- quotas globaux de disque, de temps et d'installations simultanées.
