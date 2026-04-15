# Appfacade — Fichiers d'intégration Phase 1

Ces fichiers sont destinés au **dépôt Appfacade** (séparé), pas à l'Orchestrateur.
Ils ont été livrés ici parce que le dépôt Appfacade n'était pas accessible lors
de la génération de cette PR.

## Fichiers à copier dans Appfacade

| Source (ce dépôt) | Destination (Appfacade) |
|---|---|
| `src/services/orchestrateurClient.ts` | `src/services/orchestrateurClient.ts` |
| `src/components/settings/OrchestratorSync.tsx` | `src/components/settings/OrchestratorSync.tsx` |

> ⚠️ **Ne pas copier** `src/services/activeApiKey.ts` : c'est un stub local
> présent uniquement pour la validation isolée. Appfacade doit fournir sa
> propre implémentation exposant `getActiveApiKey(): string | null`
> (ou adapter l'import dans `OrchestratorSync.tsx`).

## Intégration dans le panneau de paramètres

Ajouter dans `SettingsModal.tsx` (ou équivalent), en dernière section :

```tsx
import { OrchestratorSync } from './OrchestratorSync';

// ... dans le JSX retourné, en bas :
<OrchestratorSync />
```

## Validation côté Appfacade

```bash
cd <appfacade>
npx tsc --noEmit   # doit passer sans erreur
```

## Validation isolée effectuée ici

Les deux fichiers ont été validés en isolation avec TypeScript strict :

```bash
cd appfacade-integration
npm install
npx tsc --noEmit   # ✅ exit 0
```

Config utilisée : `tsconfig.json` (strict, `noUncheckedIndexedAccess`,
`jsx: react-jsx`, `lib: ES2020 + DOM`).

## Comportement attendu

- Orchestrateur non lancé (port 8000 muet) → `OrchestratorSync` retourne `null`,
  zéro impact UX.
- Orchestrateur détecté → section visible avec bouton de synchro de clé.
- Succès → badge vert « ✓ Clé synchronisée ».
- Échec → message rouge bref, pas de log de la clé.
