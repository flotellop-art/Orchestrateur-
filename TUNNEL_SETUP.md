# Configuration du tunnel Cloudflare permanent - agent.tryarty.com

## Prérequis
- Compte Cloudflare avec le domaine `tryarty.com`
- `cloudflared` installé (déjà fait)

---

## Étapes pour créer le tunnel permanent

### 1. Se connecter à Cloudflare depuis le terminal

```bat
cloudflared login
```

> Un navigateur s'ouvre. Connectez-vous à votre compte Cloudflare et autorisez le domaine `tryarty.com`.
> Un fichier `cert.pem` est créé dans `C:\Users\Tellop\.cloudflared\`

### 2. Créer le tunnel nommé

```bat
cloudflared tunnel create agent-tryarty
```

> Note l'UUID du tunnel affiché (ex: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

### 3. Créer le fichier de configuration

Créer le fichier `C:\Users\Tellop\.cloudflared\config.yml` :

```yaml
tunnel: agent-tryarty
credentials-file: C:\Users\Tellop\.cloudflared\<UUID-DU-TUNNEL>.json

ingress:
  - hostname: agent.tryarty.com
    service: http://localhost:8002
  - service: http_status:404
```

> Remplacez `<UUID-DU-TUNNEL>` par l'UUID obtenu à l'étape 2.

### 4. Créer l'enregistrement DNS

```bat
cloudflared tunnel route dns agent-tryarty agent.tryarty.com
```

> Cela crée automatiquement un enregistrement CNAME dans le dashboard Cloudflare :
> `agent.tryarty.com` → `<UUID>.cfargotunnel.com`

### 5. Tester le tunnel

```bat
cloudflared tunnel run agent-tryarty
```

Puis tester : `curl https://agent.tryarty.com/api/stats`

### 6. Installer comme service Windows (démarrage automatique)

```bat
cloudflared service install
```

> Le tunnel démarrera automatiquement avec Windows, même sans être connecté.

---

## Vérification du DNS dans Cloudflare Dashboard

1. Aller sur https://dash.cloudflare.com
2. Sélectionner `tryarty.com`
3. DNS > Records
4. Vérifier qu'il existe un CNAME : `agent` → `<UUID>.cfargotunnel.com` (Proxied)

---

## Commandes utiles

```bat
# Lister les tunnels existants
cloudflared tunnel list

# Voir les logs du tunnel
cloudflared tunnel info agent-tryarty

# Supprimer un tunnel
cloudflared tunnel delete agent-tryarty
```

---

## Lancement rapide

Double-cliquer sur `start_tunnel.bat` dans ce dossier.
- Si le tunnel permanent est configuré → lance `agent.tryarty.com`
- Sinon → lance un tunnel temporaire `trycloudflare.com`
