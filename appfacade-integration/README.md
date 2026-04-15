# Appfacade — Fichiers d'intégration Phase 1

Ces fichiers sont destinés au **dépôt Appfacade** (séparé), pas à l'Orchestrateur.
Ils ont été livrés ici parce que le dépôt Appfacade n'était pas accessible lors
de la génération de cette PR.

## Copie manuelle dans Appfacade

```
appfacade-integration/src/services/orchestrateurClient.ts
  → <appfacade>/src/services/orchestrateurClient.ts

appfacade-integration/src/components/settings/OrchestratorSync.tsx
  → <appfacade>/src/components/settings/OrchestratorSync.tsx
```

Puis ajouter `<OrchestratorSync />` comme dernière section dans le composant
de paramètres existant (ex. `SettingsPanel.tsx` ou `SettingsModal.tsx`).

## Validation côté Appfacade

```bash
cd <appfacade>
npx tsc --noEmit   # doit passer sans erreur
```

## Comportement attendu

- Orchestrateur non lancé (port 8000 muet) → `OrchestratorSync` retourne `null`,
  zéro impact UX.
- Orchestrateur détecté → section visible avec bouton de synchro de clé.
- Succès → badge vert « ✓ Clé synchronisée ».
- Échec → message rouge bref, pas de log de la clé.
