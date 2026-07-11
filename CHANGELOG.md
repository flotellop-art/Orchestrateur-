# Journal des versions

Ce projet suit le format `MAJEUR.MINEUR.CORRECTIF`. Les versions contenant
`beta` restent destinees aux essais et aux revues de securite.

## 0.2.0-beta.1 - en cours

Les cinq priorites de cette beta sont maintenant presentes :

1. execution Docker des commandes, tests, installations Python/npm et
   applications, avec ressources limitees, reseau coupe et aucun retour
   silencieux vers l'hote ;
2. competences reutilisables proposees par les agents puis relues et activees
   uniquement par une personne ;
3. file SQLite durable, baux de travail, tentatives bornees, pause et reprise
   apres redemarrage ;
4. taches programmees Docker et notifications sortantes Telegram, Slack et
   Discord, sans exposer les webhooks ou jetons aux agents ;
5. numero de version unique, licence MIT, tests Windows/Linux, serveur
   PyInstaller, installateur NSIS, version portable et publication GitHub avec
   empreintes SHA-256.

Autres changements : autorisations d'installation par niveau, durcissement de
l'acces HTTP et de la fenetre Electron, nettoyage des conteneurs survivants et
documentation de construction. Les executables de cette beta restent non
signes.

La revue de securite a egalement renforce l'approbation humaine des
competences, les courses pause/reprise/arret, les notifications durables, la
fermeture du runtime Docker, les en-tetes des pages web et le verrouillage par
empreinte des dependances de construction.

## 0.1.0 - prototype initial

- creation d'applications Flask ;
- espace multi-agents avec chef, delegation parallele et critique croisee ;
- memoire locale, controle des couts et tableau de supervision.
