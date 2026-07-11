# Compétences réutilisables des agents

Ce composant permet à un agent de proposer une méthode de travail apprise au
cours d'une tâche. Une proposition ne devient utilisable qu'après sa lecture et
son activation explicite par une personne.

## Cycle de vie

1. L'agent propose un nom, un résumé, des instructions et quelques tags.
2. Orchestrator lie la proposition à la tâche et à l'identité fiable de l'agent.
3. La proposition est enregistrée avec le statut `pending`.
4. L'interface humaine affiche le texte complet.
5. Une personne l'active (`active`) ou la refuse (`rejected`).
6. La recherche ne consulte que les compétences `active`.

Les trois statuts sont conservés dans SQLite. Une proposition refusée ne peut
pas être réactivée. Deux contenus identiques proposés en parallèle produisent la
même ligne, et les décisions concurrentes sont traitées dans une transaction.

## Contrat Python

Toutes les fonctions sont asynchrones, sauf le rendu Markdown :

```python
from agent_skills import (
    approve_skill,
    init_agent_skills_db,
    propose_skill,
    search_skills,
)

await init_agent_skills_db()

proposal = await propose_skill(
    task_id=task_id,
    agent=trusted_agent_name,
    name="Secure Review",
    summary="Relire un changement en recherchant les risques importants.",
    instructions="""Lire les fichiers concernés, vérifier les limites puis
exécuter les tests pertinents et résumer les résultats.""",
    tags=["security", "review"],
)

# Cet appel doit être réservé à une route authentifiée de l'interface humaine.
active = await approve_skill(
    proposal.id,
    approved_by=current_user_id,
    human_confirmed=True,
)

matches = await search_skills("revue de sécurité", limit=5)
```

API publique :

- `init_agent_skills_db` crée le schéma de façon idempotente ;
- `propose_skill` crée une proposition liée à une tâche existante ;
- `approve_skill` exige `human_confirmed=True` et l'identité du relecteur ;
- `reject_skill` exige également une action humaine et une raison ;
- `get_skill` et `list_skills` alimentent l'écran de validation ;
- `search_skills` renvoie uniquement des contenus actifs, avec un classement
  local et déterministe ;
- `export_skill_md` écrit éventuellement `<racine>/<nom>/SKILL.md`.

Pour remplacer une version déjà active, une personne doit approuver la nouvelle
proposition avec `replace_existing=True`. L'ancienne version devient alors
`rejected` dans la même transaction.

## Protections

- Le module ne lance aucune commande et n'interprète jamais les instructions.
- Les noms, identités et tags suivent un format ASCII borné, sans chemin.
- Les tailles sont bornées en caractères et en octets UTF-8.
- Les caractères de contrôle, le HTML brut et les liens actifs tels que
  `javascript:` ou `data:` sont refusés.
- Les propositions en attente ou refusées n'entrent jamais dans la recherche.
- L'export choisit lui-même le nom `SKILL.md`, reste sous la racine fournie,
  refuse les liens symboliques concernés et remplace le fichier atomiquement.
- SQLite utilise WAL, une attente bornée et des transactions d'écriture pour
  les propositions et les décisions concurrentes.

Le booléen `human_confirmed` est une garde contre les branchements accidentels,
pas une preuve cryptographique. L'orchestrateur doit donc ne jamais exposer
`approve_skill`, `reject_skill`, `replace_existing` ou `overwrite` comme outils
appelables par un agent. Ces appels doivent provenir d'une route authentifiée et
d'un clic humain. De même, le champ `agent` doit être rempli par le contexte de
la tâche, jamais recopié depuis une sortie libre du modèle.

## Utilisation dans Orchestrateur

- Le registre est initialisé après la base des tâches.
- L'outil agent expose uniquement `propose_skill`; l'identité et la tâche sont
  ajoutées par le serveur et ne viennent pas du texte du modèle.
- La page `/skills` liste les propositions et réserve l'approbation ou le refus
  à une action humaine authentifiée.
- Au début d'une tâche, seules les compétences actives correspondant à
  l'objectif sont transmises au chef d'équipe.
- L'export `SKILL.md` reste une opération séparée et facultative ; le registre
  SQLite suffit au fonctionnement normal.

Si le catalogue devient très volumineux, la recherche en mémoire pourra être
remplacée par un index lexical. Il faudra conserver le même ordre de départage
stable et ne jamais inclure les statuts `pending` ou `rejected`.
