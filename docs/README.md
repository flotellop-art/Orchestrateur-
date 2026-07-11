# Documentation d'Orchestrateur

Pour comprendre ou exploiter la bêta 0.2, commencez par :

| Document | Contenu |
| --- | --- |
| [`ARCHITECTURE_V2.md`](ARCHITECTURE_V2.md) | vue d'ensemble et choix principaux |
| [`SANDBOX.md`](SANDBOX.md) | isolation Docker, image et limites réseau |
| [`INSTALL_PERMISSIONS.md`](INSTALL_PERMISSIONS.md) | cinq niveaux de demande d'installation |
| [`AGENT_SKILLS.md`](AGENT_SKILLS.md) | compétences proposées puis validées par une personne |
| [`AUTOMATIONS.md`](AUTOMATIONS.md) | file durable, planifications et notifications |
| [`PACKAGING.md`](PACKAGING.md) | serveur autonome, installateur et publication |
| [`TEST_STRATEGY.md`](TEST_STRATEGY.md) | contrôles automatiques et essais manuels |

Les règles de sécurité pour l'accès HTTP et l'audit de dépôts se trouvent aussi
dans [`../SECURITY.md`](../SECURITY.md).

## Tutoriel du SDK Claude

Les fichiers `part1_...` à `part5_...` forment un ancien tutoriel en anglais sur
le SDK Claude Managed Agents. Ils donnent du contexte sur les boucles d'agents,
mais ne décrivent pas l'installation, la sécurité ou l'interface actuelles
d'Orchestrateur. Les versions, prix et noms de modèles cités dans ce tutoriel
peuvent évoluer ; consultez la documentation officielle du fournisseur avant de
les réutiliser.
