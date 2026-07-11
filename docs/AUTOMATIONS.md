# File durable, planifications et notifications

Ces briques sont reliees au moteur au demarrage d'Orchestrateur. La page
`/automations` cree et supervise les planifications ; les workers et le
planificateur utilisent la meme base SQLite que les taches.

## Garanties

- La file est stockee dans SQLite et l'ajout d'un travail est idempotent.
- Un worker obtient un bail temporaire. Apres un crash et l'expiration du bail,
  le travail est remis en attente avec un retard progressif.
- Une pause ou une annulation retire immediatement le bail. Le worker doit
  verifier `lease_is_active` et interrompre proprement le travail en cours.
- Chaque occurrence planifiee possede une cle stable. Un redemarrage du
  planificateur ne cree donc pas un second travail pour la meme occurrence.
- Une occurrence cree une nouvelle tache Docker a partir du modele. L'historique
  d'une execution ne remplace donc pas celui du modele ni d'une autre occurrence.
- Les calculs de calendrier sont exclusivement en UTC et ne dependent ni du
  fuseau de la machine ni du changement d'heure.
- Un agent choisit seulement un nom de canal et un texte. Il ne peut fournir ni
  webhook, ni jeton, ni identifiant Telegram.

L'execution est **au moins une fois** : un worker peut s'arreter apres une action
externe mais avant d'enregistrer sa reussite. Les actions non idempotentes
doivent donc utiliser leur propre cle de deduplication. Telegram, Slack et
Discord ne garantissent pas cette deduplication ; une notification peut etre
repetee dans ce cas precis.

## Initialisation cote serveur

```python
from automations import AutomationStore
from durable_queue import DurableQueue

queue = DurableQueue(DB_PATH)
automations = AutomationStore(DB_PATH)
await queue.init()
await automations.init()
await queue.recover_expired_leases()
```

Le meme fichier SQLite peut contenir les deux tables. Au demarrage, il ne faut
pas reprendre de force les baux encore valides : ils peuvent appartenir a un
autre processus actif. Ils seront repris automatiquement a leur expiration.

## Boucle worker

```python
lease = await queue.claim(worker_id="worker:main", lease_seconds=60)
if lease:
    try:
        result = await execute_job(lease.job.kind, lease.job.payload)
        await queue.complete(lease, result=result)
    except RetryableProviderError:
        await queue.fail(lease, error_code="provider_busy")
```

Pendant un travail long, renouveler le bail avec `heartbeat`. Avant chaque
effet important, verifier `lease_is_active`. Les codes d'echec doivent etre des
identifiants courts comme `provider_busy`, jamais le texte brut d'une exception
qui pourrait contenir une cle.

## Planifications disponibles

```python
from automations import Schedule

Schedule.interval(3600, anchor=utc_datetime)
Schedule.daily(hour=8, minute=30)
Schedule.weekly(weekday=0, hour=9)  # lundi, 0 ; dimanche, 6
```

La premiere occurrence est toujours strictement posterieure a la date de
reference. Les rythmes pris en charge sont intervalle, quotidien et
hebdomadaire. Cron n'est volontairement pas interprete afin d'eviter une
syntaxe ambigue ou une nouvelle dependance.

Le planificateur appelle periodiquement :

```python
jobs = await automations.dispatch_due(queue, limit=100)
```

Plusieurs occurrences manquees sont regroupees en un seul rattrapage afin
d'eviter une rafale de taches apres un long arret. Une planification mise en
pause saute les anciennes occurrences lors de sa reprise. Une planification
annulee ne peut pas etre reactivee. L'annulation arrete les occurrences futures ;
les travaux deja mis en file doivent etre annules separement si l'utilisateur
veut aussi les interrompre.

Seules les taches modele en mode Docker peuvent etre programmees. A la fin,
Orchestrateur place un resume du resultat dans la file de notification du canal
choisi. Une erreur d'envoi ne donne jamais acces au webhook ou au jeton.

## Configuration des notifications

La configuration principale reference des **noms de variables de secrets**, et
non les secrets eux-memes :

```text
ORCHESTRATOR_MESSAGING_CHANNELS={"build_slack":{"type":"slack","webhook_env":"BUILD_SLACK_WEBHOOK"},"ops_telegram":{"type":"telegram","token_env":"OPS_TELEGRAM_TOKEN","chat_id":"-1001234567890"}}
ORCHESTRATOR_MESSAGING_ALLOWED_CHANNELS=build_slack,ops_telegram
BUILD_SLACK_WEBHOOK=https://hooks.slack.com/services/...
OPS_TELEGRAM_TOKEN=...
```

Pour Discord, utiliser `{"type":"discord","webhook_env":"..."}`. Seuls les
hotes officiels HTTPS sont acceptes : `hooks.slack.com`, `discord.com` et
`api.telegram.org` construit en interne. Les redirections sont refusees, le
delai reseau est borne et le corps de reponse n'est jamais renvoye a l'agent.

Initialiser une seule passerelle au demarrage du serveur :

```python
import os
from messaging import MessagingGateway, ServerChannelRegistry

registry = ServerChannelRegistry.from_environment(os.environ)
messaging = MessagingGateway(registry, timeout=10)
await messaging.send_agent_payload({"channel": "build_slack", "text": "Termine"})
```

Ne pas transmettre le registre, les variables d'environnement ou le transport
HTTP aux processus agents. L'API expose seulement les noms renvoyes par
`registry.channel_names`.

## Exploitation

- Configurer `ORCHESTRATOR_QUEUE_WORKERS` selon les ressources disponibles.
- Conserver la base et les dossiers de projets ensemble lors d'une sauvegarde.
- Surveiller le nombre de travaux par etat, les baux expires, les tentatives et
  les notifications echouees sans journaliser leur contenu ni leurs secrets.
- Une mise a l'echelle sur plusieurs machines demanderait une base partagee et
  un veritable service de messages ; SQLite vise une instance locale.

Pour une instance locale ou quelques workers, WAL et les transactions courtes
gardent la solution simple.
