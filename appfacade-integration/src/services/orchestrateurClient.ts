/**
 * Orchestrateur local client (Phase 1).
 *
 * Détecte la présence de l'app Electron Orchestrateur (FastAPI sur
 * 127.0.0.1:8000) et synchronise la clé Anthropic active d'Arty vers
 * ce serveur local. Toutes les erreurs sont silencieuses : l'absence de
 * l'Orchestrateur ne doit jamais perturber l'UX d'Appfacade.
 */

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
      // ignore parse errors, keep status-based message
    }
    return { success: false, error: detail };
  } catch {
    return { success: false, error: 'Orchestrateur injoignable' };
  }
}
