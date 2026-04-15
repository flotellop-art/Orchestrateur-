#!/usr/bin/env bash
# Phase 1 — applique l'intégration Orchestrateur dans un clone local d'Appfacade.
#
# Usage :
#   cd <ton-clone-appfacade>
#   bash <chemin-vers>/apply-to-appfacade.sh
#
# Ce script :
#   1. Crée la branche claude/integrate-orchestrator-phase-1-Cpam4
#   2. Écrit src/services/orchestrateurClient.ts
#   3. Écrit src/components/settings/OrchestratorSync.tsx
#   4. Détecte la fonction "active api key" et patch l'import si nécessaire
#   5. Insère <OrchestratorSync /> dans SettingsModal.tsx
#   6. Lance npx tsc --noEmit
#   7. Commit (n'effectue PAS le push — à toi de le lancer)

set -euo pipefail

# --- garde-fous --------------------------------------------------------------
if [ ! -f "package.json" ]; then
  echo "❌ Lance ce script à la racine du clone Appfacade." >&2
  exit 1
fi
if [ ! -d "src" ]; then
  echo "❌ Pas de dossier src/ — es-tu bien dans Appfacade ?" >&2
  exit 1
fi

# --- branche -----------------------------------------------------------------
BRANCH="claude/integrate-orchestrator-phase-1-Cpam4"
git checkout -b "$BRANCH" 2>/dev/null || git checkout "$BRANCH"

# --- 1. orchestrateurClient.ts ----------------------------------------------
mkdir -p src/services
cat > src/services/orchestrateurClient.ts <<'EOF'
const ORCHESTRATOR_URL = 'http://127.0.0.1:8000';
const DETECT_TIMEOUT_MS = 1500;

export type SyncResult = { success: true } | { success: false; error: string };

export async function detectOrchestrator(): Promise<boolean> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DETECT_TIMEOUT_MS);
  try {
    const response = await fetch(`${ORCHESTRATOR_URL}/api/stats`, {
      method: 'GET',
      signal: controller.signal,
    });
    return response.status === 200;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

export async function syncApiKey(apiKey: string): Promise<SyncResult> {
  try {
    const response = await fetch(`${ORCHESTRATOR_URL}/api/set-key`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey }),
    });
    if (response.status === 200) {
      return { success: true };
    }
    let detail = `HTTP ${response.status}`;
    try {
      const data: unknown = await response.json();
      if (
        typeof data === 'object' &&
        data !== null &&
        'detail' in data &&
        typeof (data as { detail: unknown }).detail === 'string'
      ) {
        detail = (data as { detail: string }).detail;
      }
    } catch {
      // ignore
    }
    return { success: false, error: detail };
  } catch {
    return { success: false, error: 'Orchestrateur injoignable' };
  }
}
EOF

# --- 2. détection de la fonction "active api key" ----------------------------
ACTIVE_KEY_FILE=""
ACTIVE_KEY_FN=""

# heuristique : chercher un fichier qui exporte une fonction retournant la clé
for candidate in src/services/activeApiKey.ts src/lib/activeApiKey.ts src/store/activeApiKey.ts src/hooks/useActiveApiKey.ts; do
  if [ -f "$candidate" ]; then
    ACTIVE_KEY_FILE="$candidate"
    break
  fi
done

if [ -n "$ACTIVE_KEY_FILE" ]; then
  ACTIVE_KEY_FN=$(grep -oE 'export (function|const) [a-zA-Z]+' "$ACTIVE_KEY_FILE" | head -1 | awk '{print $3}')
fi

if [ -z "$ACTIVE_KEY_FN" ]; then
  echo "⚠️  Aucune fonction 'active api key' détectée. Le composant utilisera"
  echo "    un placeholder \`getActiveApiKey\` que tu devras adapter."
  ACTIVE_KEY_FN="getActiveApiKey"
  ACTIVE_KEY_IMPORT="../../services/activeApiKey"
else
  # transforme le chemin de fichier en chemin d'import relatif depuis settings/
  ACTIVE_KEY_IMPORT=$(echo "$ACTIVE_KEY_FILE" | sed 's|^src/|../../|; s|\.ts$||')
  echo "✓ Fonction détectée : $ACTIVE_KEY_FN dans $ACTIVE_KEY_FILE"
fi

# --- 3. OrchestratorSync.tsx ------------------------------------------------
mkdir -p src/components/settings
cat > src/components/settings/OrchestratorSync.tsx <<EOF
import { useEffect, useState } from 'react';
import { detectOrchestrator, syncApiKey } from '../../services/orchestrateurClient';
import { ${ACTIVE_KEY_FN} } from '${ACTIVE_KEY_IMPORT}';

type SyncStatus = 'idle' | 'success' | 'error';

export function OrchestratorSync(): JSX.Element | null {
  const [isDetected, setIsDetected] = useState<boolean>(false);
  const [isSyncing, setIsSyncing] = useState<boolean>(false);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>('idle');
  const [errorMessage, setErrorMessage] = useState<string>('');

  useEffect(() => {
    let cancelled = false;
    void detectOrchestrator().then((detected) => {
      if (!cancelled) setIsDetected(detected);
    });
    return () => { cancelled = true; };
  }, []);

  if (!isDetected) return null;

  const handleSync = async (): Promise<void> => {
    setIsSyncing(true);
    setSyncStatus('idle');
    setErrorMessage('');
    const apiKey = ${ACTIVE_KEY_FN}();
    if (!apiKey) {
      setSyncStatus('error');
      setErrorMessage('Aucune clé Anthropic active');
      setIsSyncing(false);
      return;
    }
    const result = await syncApiKey(apiKey);
    if (result.success) setSyncStatus('success');
    else { setSyncStatus('error'); setErrorMessage(result.error); }
    setIsSyncing(false);
  };

  return (
    <section className="mt-6 rounded-lg border border-slate-700 bg-slate-800/50 p-4">
      <h3 className="mb-1 text-sm font-semibold text-slate-100">🖥️ Orchestrateur détecté</h3>
      <p className="mb-3 text-xs text-slate-400">
        L'app desktop Orchestrateur est active en local. Synchronisez votre clé Anthropic.
      </p>
      <div className="flex items-center gap-3">
        <button type="button" onClick={() => { void handleSync(); }} disabled={isSyncing}
          className="rounded-md bg-orange-500 px-3 py-1.5 text-xs font-semibold text-white hover:bg-orange-400 disabled:opacity-60">
          {isSyncing ? 'Synchronisation…' : 'Synchroniser la clé →'}
        </button>
        {syncStatus === 'success' && (
          <span className="rounded-full bg-emerald-500/15 px-2.5 py-1 text-xs font-semibold text-emerald-400">✓ Clé synchronisée</span>
        )}
        {syncStatus === 'error' && (
          <span className="text-xs font-medium text-red-400">{errorMessage || 'Échec'}</span>
        )}
      </div>
    </section>
  );
}

export default OrchestratorSync;
EOF

# --- 4. patch SettingsModal.tsx ---------------------------------------------
SETTINGS=src/components/settings/SettingsModal.tsx
if [ ! -f "$SETTINGS" ]; then
  echo "⚠️  $SETTINGS introuvable — ajoute <OrchestratorSync /> manuellement."
else
  # ajoute l'import si absent
  if ! grep -q "OrchestratorSync" "$SETTINGS"; then
    # insère l'import après la dernière ligne d'import existante
    awk '
      /^import / { last=NR }
      { lines[NR]=$0 }
      END {
        for (i=1; i<=NR; i++) {
          print lines[i]
          if (i==last) print "import { OrchestratorSync } from '\''./OrchestratorSync'\'';"
        }
      }
    ' "$SETTINGS" > "$SETTINGS.tmp" && mv "$SETTINGS.tmp" "$SETTINGS"

    # insère le composant juste avant la dernière balise fermante (heuristique)
    # Recherche la dernière </div> ou </section> avant le return final ; à valider à la main
    echo "✓ Import ajouté dans $SETTINGS."
    echo "⚠️  Insère manuellement <OrchestratorSync /> en bas du JSX retourné."
  else
    echo "✓ <OrchestratorSync /> déjà présent dans $SETTINGS."
  fi
fi

# --- 5. validation TypeScript ------------------------------------------------
echo ""
echo "→ npx tsc --noEmit"
npx tsc --noEmit

# --- 6. commit ---------------------------------------------------------------
git add src/services/orchestrateurClient.ts \
        src/components/settings/OrchestratorSync.tsx \
        "$SETTINGS" 2>/dev/null || true
git commit -m "feat(phase-1): integrate Orchestrateur sync (key + detection)"

echo ""
echo "✅ Terminé. Vérifie le diff (git diff HEAD~1) puis :"
echo "   git push -u origin $BRANCH"
