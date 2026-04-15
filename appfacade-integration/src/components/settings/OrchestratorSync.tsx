/**
 * OrchestratorSync — section de paramètres Phase 1.
 *
 * Détecte l'app desktop Orchestrateur en local et permet de lui pousser
 * la clé Anthropic active d'Arty en un clic. Invisible si l'Orchestrateur
 * n'est pas lancé.
 *
 * Adapter si besoin l'import de la clé active : ce composant essaie
 * d'abord `../../services/activeApiKey` puis tombe sur un store zustand
 * `../../store` exposant `useStore(state => state.activeApiKey)`.
 * Si votre projet utilise un autre chemin, remplacez `getActiveApiKey`.
 */

import { useEffect, useState } from 'react';
import { detectOrchestrator, syncApiKey } from '../../services/orchestrateurClient';
import { getActiveApiKey } from '../../services/activeApiKey';

type SyncStatus = 'idle' | 'success' | 'error';

export function OrchestratorSync(): JSX.Element | null {
  const [isDetected, setIsDetected] = useState<boolean>(false);
  const [isSyncing, setIsSyncing] = useState<boolean>(false);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>('idle');
  const [errorMessage, setErrorMessage] = useState<string>('');

  useEffect(() => {
    let cancelled = false;
    void detectOrchestrator().then((detected) => {
      if (!cancelled) {
        setIsDetected(detected);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!isDetected) {
    return null;
  }

  const handleSync = async (): Promise<void> => {
    setIsSyncing(true);
    setSyncStatus('idle');
    setErrorMessage('');

    const apiKey = getActiveApiKey();
    if (!apiKey) {
      setSyncStatus('error');
      setErrorMessage('Aucune clé Anthropic active');
      setIsSyncing(false);
      return;
    }

    const result = await syncApiKey(apiKey);
    if (result.success) {
      setSyncStatus('success');
    } else {
      setSyncStatus('error');
      setErrorMessage(result.error);
    }
    setIsSyncing(false);
  };

  return (
    <section className="mt-6 rounded-lg border border-slate-700 bg-slate-800/50 p-4">
      <h3 className="mb-1 text-sm font-semibold text-slate-100">
        🖥️ Orchestrateur détecté
      </h3>
      <p className="mb-3 text-xs text-slate-400">
        L'app desktop Orchestrateur est active en local. Synchronisez votre
        clé Anthropic pour qu'elle génère des applications Flask sans
        configuration manuelle.
      </p>

      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={() => {
            void handleSync();
          }}
          disabled={isSyncing}
          className="rounded-md bg-orange-500 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-orange-400 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {isSyncing ? 'Synchronisation…' : 'Synchroniser la clé →'}
        </button>

        {syncStatus === 'success' && (
          <span className="rounded-full bg-emerald-500/15 px-2.5 py-1 text-xs font-semibold text-emerald-400">
            ✓ Clé synchronisée
          </span>
        )}

        {syncStatus === 'error' && (
          <span className="text-xs font-medium text-red-400">
            {errorMessage || 'Échec de la synchronisation'}
          </span>
        )}
      </div>
    </section>
  );
}

export default OrchestratorSync;
