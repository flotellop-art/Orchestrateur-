/**
 * Orchestrateur — shell Electron (Phase 3).
 *
 * Rôle : démarrer le backend FastAPI local (orchestrator.py), attendre qu'il
 * réponde, puis charger l'interface Arty (tryarty.com) à l'intérieur de la
 * fenêtre Electron. L'interface Arty embarquée détecte automatiquement
 * l'Orchestrateur local (endpoint /api/stats) et synchronise sa clé
 * Anthropic via /api/set-key (cf. Phase 1).
 *
 * Sécurité : la fenêtre charge un domaine distant (Arty). On force donc
 * contextIsolation + sandbox, on désactive nodeIntegration, et on restreint
 * la navigation à une liste d'origines autorisées.
 */

'use strict';

const { app, BrowserWindow, shell, Menu, dialog, session } = require('electron');
const { spawn } = require('node:child_process');
const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const ORCHESTRATOR_HOST = '127.0.0.1';
const ORCHESTRATOR_PORT = 8000;
const ORCHESTRATOR_ORIGIN = `http://${ORCHESTRATOR_HOST}:${ORCHESTRATOR_PORT}`;

// URL Arty chargée dans la fenêtre. Surchargeable pour dev/staging.
const ARTY_URL = process.env.ARTY_URL || 'https://tryarty.com';

// Domaines autorisés à naviguer dans la fenêtre principale.
// Les autres liens s'ouvrent dans le navigateur système.
const ALLOWED_EMBED_HOSTS = new Set([
  'tryarty.com',
  'www.tryarty.com',
  'appfacade.pages.dev',
  '127.0.0.1',
  'localhost',
]);

// Délais : à 500 ms près, 25 s est le plafond historique (cf. wait_for_server).
const HEARTBEAT_INTERVAL_MS = 500;
const HEARTBEAT_TIMEOUT_MS = 25_000;

const IS_DEV = process.env.ORCHESTRATEUR_DEV === '1';

// ---------------------------------------------------------------------------
// État global
// ---------------------------------------------------------------------------

/** @type {BrowserWindow | null} */
let mainWindow = null;
/** @type {import('node:child_process').ChildProcess | null} */
let pythonProc = null;
/** true si on a démarré Python nous-mêmes (donc on doit le tuer à la sortie). */
let pythonOwned = false;

// ---------------------------------------------------------------------------
// Backend FastAPI : détection et démarrage
// ---------------------------------------------------------------------------

/**
 * Ping /api/stats. Résout `true` si le backend répond en HTTP 200.
 * @param {number} timeoutMs
 * @returns {Promise<boolean>}
 */
function pingOrchestrator(timeoutMs = 1500) {
  return new Promise((resolve) => {
    const req = http.get(
      {
        host: ORCHESTRATOR_HOST,
        port: ORCHESTRATOR_PORT,
        path: '/api/stats',
        timeout: timeoutMs,
      },
      (res) => {
        // On consomme la réponse pour libérer la socket.
        res.resume();
        resolve(res.statusCode === 200);
      },
    );
    req.on('error', () => resolve(false));
    req.on('timeout', () => {
      req.destroy();
      resolve(false);
    });
  });
}

/**
 * Trouve le script orchestrator.py (repo en dev, ou resourcesPath en prod).
 * @returns {string | null}
 */
function resolveOrchestratorScript() {
  const candidates = [
    path.join(__dirname, '..', 'orchestrator.py'),
    path.join(process.resourcesPath || '', 'orchestrator.py'),
  ];
  for (const candidate of candidates) {
    if (candidate && fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

/**
 * Résout l'interpréteur Python : .venv local si présent, sinon `python`.
 * @returns {string}
 */
function resolvePythonCommand() {
  const repoRoot = path.join(__dirname, '..');
  const venvCandidates =
    process.platform === 'win32'
      ? [path.join(repoRoot, '.venv', 'Scripts', 'python.exe')]
      : [path.join(repoRoot, '.venv', 'bin', 'python')];
  for (const candidate of venvCandidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return process.platform === 'win32' ? 'python' : 'python3';
}

/**
 * Lance `python orchestrator.py` en sous-processus.
 * @param {string} scriptPath
 */
function spawnOrchestrator(scriptPath) {
  const python = resolvePythonCommand();
  const proc = spawn(python, [scriptPath], {
    cwd: path.dirname(scriptPath),
    env: { ...process.env, PYTHONUNBUFFERED: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  proc.stdout.on('data', (chunk) => {
    if (IS_DEV) process.stdout.write(`[orchestrator] ${chunk}`);
  });
  proc.stderr.on('data', (chunk) => {
    // uvicorn écrit ses logs d'accès sur stderr — normal, pas une erreur.
    if (IS_DEV) process.stderr.write(`[orchestrator] ${chunk}`);
  });
  proc.on('exit', (code, signal) => {
    if (IS_DEV) {
      console.log(`[orchestrator] exit code=${code} signal=${signal}`);
    }
    pythonProc = null;
  });

  return proc;
}

/**
 * Attend que le backend réponde, jusqu'au timeout.
 * @returns {Promise<boolean>}
 */
async function waitForOrchestrator() {
  const deadline = Date.now() + HEARTBEAT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await pingOrchestrator(HEARTBEAT_INTERVAL_MS)) {
      return true;
    }
    await new Promise((r) => setTimeout(r, HEARTBEAT_INTERVAL_MS));
  }
  return false;
}

/**
 * Garantit qu'un Orchestrateur répond sur 127.0.0.1:8000.
 * Soit on le détecte déjà en vie, soit on le spawn.
 * @returns {Promise<{ok: boolean; reason?: string}>}
 */
async function ensureOrchestratorRunning() {
  if (await pingOrchestrator(800)) {
    pythonOwned = false;
    return { ok: true };
  }

  const scriptPath = resolveOrchestratorScript();
  if (!scriptPath) {
    return {
      ok: false,
      reason:
        "orchestrator.py introuvable — Arty se chargera sans backend local.",
    };
  }

  pythonProc = spawnOrchestrator(scriptPath);
  pythonOwned = true;

  const ready = await waitForOrchestrator();
  if (!ready) {
    return {
      ok: false,
      reason:
        "Le backend FastAPI n'a pas répondu dans les délais (25 s).",
    };
  }
  return { ok: true };
}

/**
 * Arrêt propre du backend si on l'a démarré.
 */
function stopOrchestrator() {
  if (pythonProc && pythonOwned && !pythonProc.killed) {
    try {
      if (process.platform === 'win32') {
        // taskkill /T pour l'arbre complet (uvicorn + worker).
        spawn('taskkill', ['/pid', String(pythonProc.pid), '/f', '/t']);
      } else {
        pythonProc.kill('SIGTERM');
      }
    } catch {
      // Silencieux : la fenêtre part de toute façon.
    }
  }
  pythonProc = null;
}

// ---------------------------------------------------------------------------
// Fenêtre principale
// ---------------------------------------------------------------------------

function buildMenu() {
  const template = [
    {
      label: 'Orchestrateur',
      submenu: [
        {
          label: 'Recharger Arty',
          accelerator: 'CmdOrCtrl+R',
          click: () => mainWindow?.reload(),
        },
        {
          label: 'Ouvrir le tableau de bord local',
          click: () => shell.openExternal(ORCHESTRATOR_ORIGIN),
        },
        { type: 'separator' },
        { role: 'quit', label: 'Quitter' },
      ],
    },
    {
      label: 'Affichage',
      submenu: [
        { role: 'togglefullscreen' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
        { role: 'resetZoom' },
      ],
    },
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 820,
    minWidth: 960,
    minHeight: 600,
    backgroundColor: '#0f172a',
    title: 'Orchestrateur',
    icon: path.join(__dirname, 'assets', 'icon.ico'),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      // Cache agressif : on veut toujours le dernier Arty déployé.
      // cache: false n'existe pas en option ; on utilise clearCache à l'init.
    },
    show: false,
  });

  // Nettoie l'ancien HTML Arty mis en cache par Chromium (cf. README).
  mainWindow.webContents.session.clearCache().catch(() => {});

  // Splash local pendant que le backend démarre.
  mainWindow.loadFile(path.join(__dirname, 'splash.html')).catch(() => {});
  mainWindow.show();

  // Navigation : autorise uniquement les hôtes connus dans la fenêtre,
  // ouvre tout le reste dans le navigateur système.
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url).catch(() => {});
    return { action: 'deny' };
  });

  mainWindow.webContents.on('will-navigate', (event, url) => {
    try {
      const { hostname, protocol } = new URL(url);
      if (protocol === 'file:' || ALLOWED_EMBED_HOSTS.has(hostname)) return;
      event.preventDefault();
      shell.openExternal(url).catch(() => {});
    } catch {
      event.preventDefault();
    }
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

/**
 * Remplace le splash local par Arty.
 * @param {string} url
 */
async function loadArty(url) {
  if (!mainWindow) return;
  try {
    await mainWindow.loadURL(url, {
      // no-store pour éviter qu'une version obsolète d'Arty soit rejouée.
      extraHeaders: 'Cache-Control: no-store\n',
    });
  } catch (err) {
    showFatal(
      `Impossible de charger Arty (${url}).`,
      err instanceof Error ? err.message : String(err),
    );
  }
}

function showFatal(title, detail) {
  if (!mainWindow) {
    dialog.showErrorBox(title, detail);
    return;
  }
  dialog.showMessageBox(mainWindow, {
    type: 'error',
    title,
    message: title,
    detail,
    buttons: ['Quitter'],
  }).finally(() => app.quit());
}

// ---------------------------------------------------------------------------
// Lifecycle Electron
// ---------------------------------------------------------------------------

// Single-instance lock : éviter deux orchestrateurs qui se battent pour le port 8000.
const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (!mainWindow) return;
    if (mainWindow.isMinimized()) mainWindow.restore();
    mainWindow.focus();
  });

  app.whenReady().then(async () => {
    // Durcissement : aucune permission par défaut pour le domaine distant.
    session.defaultSession.setPermissionRequestHandler((_wc, _perm, callback) => {
      callback(false);
    });

    buildMenu();
    createWindow();

    const status = await ensureOrchestratorRunning();
    if (!status.ok && IS_DEV) {
      console.warn(`[orchestrateur] backend indisponible : ${status.reason}`);
    }

    await loadArty(ARTY_URL);
  });
}

app.on('window-all-closed', () => {
  stopOrchestrator();
  if (process.platform !== 'darwin') app.quit();
});

app.on('before-quit', () => {
  stopOrchestrator();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
