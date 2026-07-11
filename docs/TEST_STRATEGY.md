# Stratégie de vérification

## Principe

Les tests rapides couvrent les décisions de sécurité et les transitions de
données. Quelques tests d'intégration vérifient ensuite le serveur et le paquet
desktop. Docker et les services de messagerie sont simulés dans les tests
ordinaires afin que la suite reste reproductible.

## Couverture attendue

| Zone | Vérifications principales |
| --- | --- |
| Sandbox | arguments Docker, utilisateur non administrateur, limites, réseau coupé, montage limité, arrêt et délai dépassé |
| File durable | ajout idempotent, réservation atomique, bail expiré, reprise après panne, nouvelle tentative, pause et annulation |
| Compétences | validation obligatoire, contenu borné, recherche active seulement, rejet, export sans sortie de dossier |
| Planifications | calcul de la prochaine date, fuseau UTC, activation, désactivation, absence de doublon |
| Messageries | destination issue de la configuration, taille bornée, délai, erreurs nettoyées de tout secret |
| API | authentification, validation des corps, codes d'erreur et transitions visibles |
| Interface | aucune insertion HTML non échappée, restauration après rechargement, décisions accessibles au clavier |
| Paquet | lancement sans Python système, réponse de `/health`, CLI Claude et ressources Docker présentes, deux exécutables et empreintes SHA-256 |

## Niveaux

1. **Unitaires** : logique pure, validations et transitions SQLite.
2. **Intégration** : routes FastAPI avec base temporaire et exécuteurs simulés.
3. **Fumée** : backend PyInstaller démarré sur Windows puis appel de `/health`.
4. **Manuels avant version stable** : Docker Desktop réel, installation Python et npm dans une tâche jetable, aperçu d'une application, notification vers des canaux de test et mise à jour depuis un ancien profil.

## Cas de panne obligatoires

- arrêt du serveur pendant une tâche ;
- arrêt pendant une demande d'installation ;
- bail de file expiré ;
- Docker absent ou arrêté ;
- conteneur dépassant mémoire, durée ou nombre de processus ;
- notification distante lente ou en erreur ;
- compétence malformée ou tentative de chemin `../` ;
- double clic sur démarrer, approuver ou exécuter maintenant.

## Seuil de livraison

- toute la suite Python et JavaScript passe sur Windows et Linux ;
- le build autonome répond à `/health` sans interpréteur Python externe ;
- l'installateur et la version portable contiennent le serveur, le CLI Claude,
  la licence, `VERSION` et les fichiers de construction Docker ;
- aucune erreur dans `git diff --check` ;
- aucune demande sensible ne devient automatiquement permanente ;
- les limites encore connues sont décrites dans `SECURITY.md` et dans la PR.
