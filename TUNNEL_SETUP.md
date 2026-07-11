# Exposer Orchestrateur avec un tunnel Cloudflare

Un tunnel donne un accès distant à des fonctions capables de créer et lancer du
code. N'ouvrez jamais le serveur sans clé forte, liste d'hôtes explicite et
contrôle du compte Cloudflare.

## Sécurité obligatoire

Dans `.env`, configurez au minimum :

```dotenv
API_SECRET_KEY=<cle-aleatoire-d-au-moins-24-caracteres>
ORCHESTRATOR_ALLOWED_HOSTS=localhost,127.0.0.1,::1,orchestrateur.example.com
```

Vous pouvez générer la clé avec :

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Pour un tunnel temporaire, ajoutez `*.trycloudflare.com` aux hôtes autorisés.
`start_tunnel.bat` refuse de démarrer si la clé est trop courte ou si aucun hôte
distant n'est déclaré.

## Tunnel permanent

Les noms ci-dessous sont des exemples : remplacez le domaine, le nom du tunnel
et son identifiant par les vôtres.

```powershell
cloudflared login
cloudflared tunnel create orchestrateur-prod
```

Créez `%USERPROFILE%\.cloudflared\config.yml` :

```yaml
tunnel: orchestrateur-prod
credentials-file: C:\Users\VOTRE-COMPTE\.cloudflared\UUID-DU-TUNNEL.json

ingress:
  - hostname: orchestrateur.example.com
    service: http://localhost:8000
  - service: http_status:404
```

Créez ensuite la route DNS et démarrez le tunnel :

```powershell
cloudflared tunnel route dns orchestrateur-prod orchestrateur.example.com
cloudflared tunnel run orchestrateur-prod
```

Le script du dépôt peut aussi le lancer :

```powershell
.\start_tunnel.bat orchestrateur-prod
```

Sans argument, il utilise le tunnel déclaré dans `config.yml`. Sans fichier de
configuration, il ouvre un tunnel temporaire vers `http://localhost:8000`.

## Vérification

Une requête distante doit fournir la clé dans un en-tête, jamais dans l'URL :

```powershell
curl.exe -H "X-API-Key: VOTRE_CLE" https://orchestrateur.example.com/api/stats
```

Vérifiez aussi qu'une requête sans clé et une requête envoyée avec un autre
en-tête `Host` sont refusées. Consultez [`SECURITY.md`](SECURITY.md) avant toute
exposition durable.
