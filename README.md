# Orchestrateur — Créateur d'Applications IA

> Créez une application web fonctionnelle en décrivant simplement ce que vous voulez. Aucune connaissance technique requise.
>
> ![Python](https://img.shields.io/badge/Python-3.10+-blue) ![Electron](https://img.shields.io/badge/Electron-28-blue) ![FastAPI](https://img.shields.io/badge/FastAPI-async-green) ![Claude](https://img.shields.io/badge/Claude-Sonnet_4.6-orange)
>
> ---
>
> ## Vue d'ensemble
>
> **Multi-Agent App Creator** est une application desktop Windows qui permet à n'importe qui de créer une application web fonctionnelle en quelques secondes, simplement en décrivant ce qu'il veut en français.
>
> ### Ce que fait l'Orchestrateur
>
> 1. L'utilisateur décrit son besoin en langage naturel
> 2. 2. Claude génère automatiquement une app Python/Flask complète
>    3. 3. L'app est installée, démarrée et ouverte dans le navigateur — automatiquement
>       4. 4. Chaque étape est visible en temps réel via une barre de progression
>         
>          5. ---
>         
>          6. ## Architecture technique
>         
>          7. ### Couche 1 — Shell desktop : Electron 28
>
> L'application tourne dans Electron 28 (même moteur que VS Code ou Slack). Electron encapsule une fenêtre Chromium qui affiche l'interface web, et un processus Node.js principal qui gère le cycle de vie.
>
> **Fichiers clés :**
> - `electron-app/main.js` — gère la fenêtre, démarre le serveur Python, logique de vérification de port, système tray, cache-clearing
> - - `electron-app/preload.js` — bridge sécurisé entre le renderer et le process Electron
>   - - `electron-app/package.json` — dépendances Node (Electron 28, electron-builder 24)
>    
>     - **Bugs résolus :**
>     - - Electron cachait l'ancien HTML après chaque mise à jour → fix : `win.webContents.session.clearCache()` + headers `Cache-Control: no-store`
>       - - `localhost` résout en IPv6 (`::1`) sur Node.js 17+ mais Python n'écoute qu'en IPv4 → fix : `127.0.0.1` partout dans `main.js`
>        
>         - ### Couche 2 — Serveur backend : FastAPI (Python)
>        
>         - Le cœur de l'application. Fichier : `orchestrator.py` (~490 lignes).
>        
>         - **Technologies :** FastAPI, uvicorn, aiosqlite, anthropic SDK, asyncio
>
> **Base de données SQLite** (`apps.db`) — table `apps` :
> ```
> id | name | description | folder | status | port | created_at | error
> ```
>
> **Routes exposées :**
> | Route | Description |
> |-------|-------------|
> | `GET /` | Sert `index.html` avec headers no-cache |
> | `GET /api/stats` | Vérifie que le serveur est vivant |
> | `GET /api/apps` | Liste toutes les applications créées |
> | `POST /api/create` | Pipeline principal (SSE streaming) |
> | `POST /api/apps/{id}/start` | Relancer une app arrêtée |
> | `POST /api/apps/{id}/stop` | Arrêter un serveur |
> | `DELETE /api/apps/{id}` | Supprimer une app et ses fichiers |
>
> ### Couche 3 — Intelligence artificielle : Claude Sonnet 4.6
>
> Le modèle génère du code Python/Flask structuré en JSON strict.
>
> **Prompt système :**
> ```
> Tu es un générateur d'applications web Python/Flask.
> Réponds UNIQUEMENT avec un objet JSON valide.
> Structure : {"name": "...", "files": [{"filename": "app.py", "content": "..."}]}
> - app.py doit écouter sur le PORT indiqué
> - requirements.txt doit contenir uniquement "flask"
> - L'HTML doit être inline dans app.py
> ```
>
> **Problèmes résolus :**
> - Claude renvoyait du JSON entouré de texte → extraction `raw[first_{ : last_}+1]`
> - - Retours à la ligne réels dans les strings JSON → fonction `repair_json()` caractère par caractère
>  
>   - ### Couche 4 — Pipeline de création (6 étapes)
>  
>   - Le pipeline `creation_pipeline()` est un générateur asynchrone qui envoie des événements SSE au frontend en temps réel.
>  
>   - ```
>     1. Génération du code    → Claude + extraction JSON + repair (max 3 tentatives)
>     2. Vérification syntaxe  → python -m py_compile (max 2 corrections auto)
>     3. Écriture des fichiers → projects/app_YYYYMMDD_HHMMSS/
>     4. Installation deps     → pip install -r requirements.txt
>     5. Démarrage serveur     → asyncio.create_subprocess_exec
>     6. Vérification HTTP     → GET 127.0.0.1:{port}/ toutes les 500ms (max 25s)
>     ```
>
> ### Couche 5 — Interface utilisateur
>
> Fichier unique `static/index.html` (~414 lignes, CSS pur, aucun framework).
>
> - Zone de création : textarea + exemples cliquables + bouton principal
> - - Barre de progression : 6 étapes animées avec icônes
>   - - Galerie : cartes par application avec statut (running/stopped/error)
>     - - Boutons par app : Ouvrir / Relancer / Supprimer
>      
>       - ---
>
> ## Structure du projet
>
> ```
> claude-managed-agents/
> ├── orchestrator.py          # Backend principal FastAPI
> ├── static/index.html        # Frontend (fichier unique)
> ├── apps.db                  # SQLite : liste des applications
> ├── projects/                # Applications générées
> │   └── app_20260415_001234/
> │       ├── app.py
> │       └── requirements.txt
> ├── electron-app/            # Shell Electron
> │   ├── main.js
> │   ├── preload.js
> │   └── assets/icon.ico
> └── .venv/                   # Environnement Python
> ```
>
> ---
>
> ## 20 Applications utiles à créer avec l'Orchestrateur
>
> Ces 20 idées d'applications ont une vraie utilité quotidienne. Elles sont conçues pour être générées par l'Orchestrateur via une simple description en français.
>
> | # | Application | Description | Public cible |
> |---|-------------|-------------|--------------|
> | 1 | **Convertisseur d'unités universel** | Poids, distance, température, monnaie en temps réel | Tout public |
> | 2 | **Générateur de mots de passe sécurisé** | Règles personnalisables, score de force, copie en 1 clic | Tout public |
> | 3 | **Calculateur de budget mensuel** | Entrées/sorties, graphique camembert, solde restant | Particuliers |
> | 4 | **Timer Pomodoro** | 25/5 min, historique des sessions, statistiques de productivité | Étudiants, télétravailleurs |
> | 5 | **Lecteur RSS minimaliste** | Agrège des flux par catégorie, lecture offline | Professionnels, curieux |
> | 6 | **Générateur de factures simples** | PDF téléchargeable avec logo, TVA, numérotation auto | Freelances, auto-entrepreneurs |
> | 7 | **Calculateur de prêt immobilier** | Mensualités, coût total, tableau d'amortissement interactif | Acheteurs immobiliers |
> | 8 | **Gestionnaire de liste de courses** | Catégories, quantités, partage par QR code | Familles |
> | 9 | **Générateur de CV HTML** | Formulaire structuré → CV propre imprimable / exportable PDF | Chercheurs d'emploi |
> | 10 | **Suivi de poids / IMC** | Courbe de progression, calcul IMC, objectif personnalisé | Santé, bien-être |
> | 11 | **Calculateur de dosage cuisine** | Adapter une recette à N personnes automatiquement | Cuisiniers, restaurateurs |
> | 12 | **Minuteur multi-tâches** | Plusieurs timers simultanés avec labels et alertes sonores | Cuisine, atelier, bricolage |
> | 13 | **Bloc-notes chiffré** | Notes locales protégées par mot de passe, stockage SQLite | Confidentialité personnelle |
> | 14 | **Générateur de QR codes** | URL, texte, vCard, WiFi — téléchargement PNG | Usage pro et personnel |
> | 15 | **Calculateur de macros nutritionnels** | Protéines/lipides/glucides selon objectif sportif | Sportifs, régimes |
> | 16 | **Planificateur de trajet carbone** | Comparer voiture/train/avion en kg CO₂ | Sensibilisation écologique |
> | 17 | **Simulateur de retraite** | Calcul selon salaire, âge, cotisations, espérance de vie | Planification long terme |
> | 18 | **Générateur de planning hebdomadaire** | Interface visuelle, drag & drop, export PDF | Organisation perso/pro |
> | 19 | **Outil de comparaison de prix** | Saisir prix de différentes boutiques, calcul du meilleur rapport qualité/prix | Consommateurs malins |
> | 20 | **Journal de gratitude** | Écriture quotidienne guidée, statistiques d'humeur, calendrier | Bien-être mental |
>
> ---
>
> ## Installation et démarrage
>
> ### Prérequis
> - Python 3.10+
> - - Node.js 18+
>   - - Une clé API Anthropic
>    
>     - ### Démarrage rapide
>    
>     - ```bash
>       # 1. Cloner le repo
>       git clone https://github.com/flotellop-art/Orchestrateur-.git
>       cd Orchestrateur-
>
>       # 2. Créer l'environnement Python
>       python -m venv .venv
>       .venv\Scripts\activate   # Windows
>
>       # 3. Installer les dépendances Python
>       pip install fastapi uvicorn aiosqlite anthropic
>
>       # 4. Configurer la clé API
>       set ANTHROPIC_API_KEY=sk-ant-...
>
>       # 5. Lancer le serveur
>       python orchestrator.py
>
>       # 6. Ouvrir dans le navigateur
>       # http://127.0.0.1:8000
>       ```
>
> ### Application desktop (Electron)
>
> ```bash
> cd electron-app
> npm install
> npm start
> ```
>
> ---
>
> ## Historique de développement
>
> ### Phase 1 — Infrastructure
> Orchestrateur d'agents IA classique : agents Claude analysant du code, générant des tests, chassant des bugs.
>
> ### Phase 2 — Tentatives d'exécution de code agent
> Problèmes rencontrés : les agents généraient des tests (Jest, React), pas des serveurs ; extraction de code par regex trop fragile.
>
> ### Phase 3 — Réécriture complète (version actuelle)
> Abandon de l'approche "agents génériques" → un seul pipeline dédié à la création d'apps.
>
> **Innovations clés :**
> - Prompt JSON strict : Claude ne peut répondre qu'avec un JSON structuré
> - - `repair_json()` : corrige les retours à la ligne littéraux dans les strings JSON
>   - - `verify_python_syntax()` : vérifie avant d'écrire, corrige automatiquement
>     - - `wait_for_server()` : attend une réponse HTTP réelle avant de dire "prêt"
>       - - SSE streaming : chaque étape visible en temps réel
>        
>         - ---
>
> ## Mémoire partagée des agents gérés & missions planifiées
>
> ### Mémoire partagée entre missions des Agents gérés Anthropic
> Chaque délégation à un Agent géré (`assign_managed_agent`) participe désormais à une
> **mémoire commune** (namespace `managed` dans `memory.py`). Avant la mission, les leçons
> tirées des missions précédentes — ainsi que les règles `_shared` et les notes `_user` —
> sont injectées en tête de l'instruction. À la fin d'une mission réussie, 1 à 3 leçons
> réutilisables sont extraites (Haiku) et rangées dans `managed`. Les missions suivantes en
> bénéficient automatiquement, sans qu'on ait à tout répéter. Consultable via
> `GET /api/memory/list?namespace=managed`.
>
> ### Missions planifiées (à heure fixe)
> Onglet **Planifié** du centre de contrôle (`/control`). Programmez une mission qui se lance
> toute seule : **une seule fois** (date/heure), **chaque jour** (HH:MM), ou **à intervalle
> régulier** (toutes les N minutes). Les paramètres de la tâche (modèle du chef, recherche web,
> plafonds, mode entreprise + dépôt GitHub, jeton du projet) sont conservés avec la mission. Un
> planificateur asyncio (démarré au lancement du serveur) vérifie les échéances toutes les
> ~20 s et crée/lance la tâche correspondante, puis reprogramme la suivante.
>
> Les heures saisies dans l'UI sont locales (le navigateur transmet son décalage), converties et
> stockées en UTC. Le jeton GitHub d'une mission est conservé en base mais **jamais renvoyé** par
> l'API (seul un booléen `has_github_token` l'est).
>
> **Routes exposées :**
> | Route | Description |
> |-------|-------------|
> | `GET /api/schedules` | Liste les missions programmées (sans secret) |
> | `POST /api/schedules` | Crée une mission (`once` / `daily` / `interval`) |
> | `POST /api/schedules/{id}/toggle` | Active / suspend une mission |
> | `POST /api/schedules/{id}/run-now` | Lance immédiatement la mission |
> | `DELETE /api/schedules/{id}` | Supprime une mission programmée |
>
> ---
>
> ## Licence
>
> MIT — Libre d'utilisation, modification et distribution.
>
> ---
>
> *Construit avec Claude Sonnet 4.6 (Anthropic) · Electron 28 · FastAPI · Python*
