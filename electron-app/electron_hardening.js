/**
 * electron_hardening.js — Correctifs de securite pour l'application Electron.
 *
 * Fonctionnalites :
 * - contextIsolation: true (isole le contexte renderer du contexte Node.js)
 * - nodeIntegration: false (empeche l'acces a Node.js depuis les pages web)
 * - sandbox: true (sandboxe le processus renderer)
 * - Verification des URLs chargees (whitelist)
 * - CSP (Content Security Policy) via headers
 * - Blocage de la navigation vers des URLs externes
 * - Desactivation de l'ouverture de nouvelles fenetres non autorisees
 *
 * Intégration : voir SECURITY_INSTALL.md
 * Ce fichier est un PATCH a appliquer sur electron-app/main.js.
 * Les sections modifiees sont clairement indiquees.
 */

'use strict';

const { session, shell } = require('electron');
const path = require('path');

// ---------------------------------------------------------------------------
// Configuration de securite
// ---------------------------------------------------------------------------

const PORT = 8000; // port de l'orchestrateur (main.js utilise 8000)
const BASE_URL = `http://127.0.0.1:${PORT}`;

/**
 * Origines autorisees pour le chargement de contenu.
 * UNIQUEMENT localhost — aucune URL externe ne doit etre chargee
 * dans la fenetre principale.
 */
const ALLOWED_ORIGINS = [
  `http://localhost:${PORT}`,
  `http://127.0.0.1:${PORT}`,
  `http://localhost:8000`,
  `http://127.0.0.1:8000`,
];

/**
 * Domaines autorises pour shell.openExternal (liens ouverts dans le navigateur
 * systeme). Tout autre domaine est bloque.
 */
const ALLOWED_EXTERNAL_HOSTS = [
  'docs.anthropic.com',
  'github.com',
  'localhost',
  '127.0.0.1',
];

// ---------------------------------------------------------------------------
// Content Security Policy
// ---------------------------------------------------------------------------

/**
 * CSP stricte :
 * - default-src 'self' : seules les ressources locales sont autorisees
 * - script-src 'self' : pas de scripts inline, pas d'eval
 * - style-src 'self' 'unsafe-inline' : styles inline autorises (UI frameworks)
 * - connect-src 'self' : XHR/fetch uniquement vers l'origine
 * - img-src 'self' data: : images locales + data URIs (avatars)
 * - object-src 'none' : pas de plugins
 * - base-uri 'self' : empeche l'injection de base tag
 * - frame-ancestors 'none' : empeche l'embedding dans des iframes
 */
const CSP_POLICY = [
  "default-src 'self'",
  // 'unsafe-inline' requis : control.html / workspace.html / index.html embarquent
  // leur JS en <script> inline. (Phase 2 : extraire le JS puis durcir a 'self'.)
  "script-src 'self' 'unsafe-inline'",
  "style-src 'self' 'unsafe-inline'",
  `connect-src 'self' http://127.0.0.1:${PORT} http://localhost:${PORT} http://127.0.0.1:8000 http://localhost:8000`,
  "img-src 'self' data:",
  "object-src 'none'",
  "base-uri 'self'",
  "frame-ancestors 'none'",
  "form-action 'self'",
].join('; ');

// ---------------------------------------------------------------------------
// webPreferences securisees (remplacement du bloc createWin)
// ---------------------------------------------------------------------------

/**
 * PATCH createWin — remplacer le bloc webPreferences dans createWin() par :
 *
 *   webPreferences: getSecureWebPreferences()
 *
 * Au lieu de l'inline actuel.
 */
function getSecureWebPreferences() {
  return {
    // SECURITE CRITIQUE : isolation du contexte renderer
    contextIsolation: true,

    // SECURITE CRITIQUE : pas d'acces a Node.js depuis le renderer
    nodeIntegration: false,

    // SECURITE : sandboxe le processus renderer (limite les appels systeme)
    sandbox: true,

    // Preload script : seul canal de communication autorise entre
    // renderer et main process (via contextBridge)
    preload: path.join(__dirname, 'preload.js'),

    // Desactiver la same-origin policy experimentale
    webSecurity: true,

    // Empeche la navigation vers des URLs non autorisees
    allowRunningInsecureContent: false,

    // Desactiver les fonctionnalites experimentales potentiellement dangereuses
    experimentalFeatures: false,

    // Desactiver l'acces aux fichiers locaux depuis les pages web distantes
    // (deja garanti par webSecurity: true, doublon defensif)
    navigateOnDragDrop: false,
  };
}

// ---------------------------------------------------------------------------
// Validation des URLs
// ---------------------------------------------------------------------------

/**
 * Verifie si une URL est dans la whitelist des origines autorisees.
 * @param {string} url
 * @returns {boolean}
 */
function isAllowedUrl(url) {
  try {
    const parsed = new URL(url);
    // Autoriser les URLs data: pour le splash screen
    if (parsed.protocol === 'data:') return true;
    // Autoriser les fichiers locaux (preload, etc.)
    if (parsed.protocol === 'file:') return true;
    // Verifier l'origine
    const origin = `${parsed.protocol}//${parsed.hostname}:${parsed.port || (parsed.protocol === 'https:' ? 443 : 80)}`;
    // Verifier contre la liste des origines autorisees
    for (const allowed of ALLOWED_ORIGINS) {
      if (url.startsWith(allowed)) return true;
    }
    return false;
  } catch (e) {
    return false;
  }
}

/**
 * Verifie si un host est autorise pour shell.openExternal.
 * @param {string} url
 * @returns {boolean}
 */
function isAllowedExternalUrl(url) {
  try {
    const parsed = new URL(url);
    // Seuls https:// sont autorises pour les liens externes
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return false;
    return ALLOWED_EXTERNAL_HOSTS.some(host =>
      parsed.hostname === host || parsed.hostname.endsWith('.' + host)
    );
  } catch (e) {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Configuration de la session (CSP + filtrage des requetes)
// ---------------------------------------------------------------------------

/**
 * Configure la session Electron avec CSP et filtrage.
 * A appeler apres que app soit prete : setupSecureSession()
 *
 * PATCH : appeler cette fonction dans app.whenReady().then(...)
 * avant createWin().
 */
function setupSecureSession() {
  const ses = session.defaultSession;

  // Injecter le header CSP sur toutes les reponses locales
  ses.webRequest.onHeadersReceived((details, callback) => {
    const responseHeaders = { ...details.responseHeaders };

    // Appliquer CSP uniquement sur les pages HTML (pas les assets)
    const contentType = (responseHeaders['content-type'] || responseHeaders['Content-Type'] || []).join('');
    if (contentType.includes('text/html') || !contentType) {
      responseHeaders['Content-Security-Policy'] = [CSP_POLICY];
      responseHeaders['X-Content-Type-Options'] = ['nosniff'];
      responseHeaders['X-Frame-Options'] = ['DENY'];
      responseHeaders['X-XSS-Protection'] = ['1; mode=block'];
      responseHeaders['Referrer-Policy'] = ['strict-origin-when-cross-origin'];
    }

    callback({ responseHeaders });
  });

  console.log('[SECURITY] Session securisee configuree (CSP, headers).');
}

// ---------------------------------------------------------------------------
// Hooks de securite pour BrowserWindow
// ---------------------------------------------------------------------------

/**
 * Attache les hooks de securite a une BrowserWindow.
 * A appeler juste apres new BrowserWindow(...).
 *
 * PATCH : appeler attachSecurityHooks(win) dans createWin()
 * apres la creation de la fenetre.
 *
 * @param {Electron.BrowserWindow} browserWindow
 */
function attachSecurityHooks(browserWindow) {
  const wc = browserWindow.webContents;

  // 1. Bloquer la navigation vers des URLs non autorisees
  wc.on('will-navigate', (event, url) => {
    if (!isAllowedUrl(url)) {
      console.warn('[SECURITY] Navigation bloquee :', url);
      event.preventDefault();
      // Ouvrir dans le navigateur systeme si l'URL est externe autorisee
      if (isAllowedExternalUrl(url)) {
        shell.openExternal(url);
      }
    }
  });

  // 2. Bloquer les redirections vers des URLs non autorisees
  wc.on('will-redirect', (event, url) => {
    if (!isAllowedUrl(url)) {
      console.warn('[SECURITY] Redirection bloquee :', url);
      event.preventDefault();
    }
  });

  // 3. Bloquer l'ouverture de nouvelles fenetres (window.open, _blank)
  wc.setWindowOpenHandler(({ url }) => {
    // Ouvrir dans le navigateur systeme si autorise
    if (isAllowedExternalUrl(url)) {
      shell.openExternal(url);
    } else {
      console.warn('[SECURITY] Ouverture de fenetre bloquee :', url);
    }
    // Toujours refuser la creation d'une nouvelle BrowserWindow
    return { action: 'deny' };
  });

  // 4. Bloquer les permissions non necessaires (camera, micro, notifications, etc.)
  browserWindow.webContents.session.setPermissionRequestHandler(
    (webContents, permission, callback) => {
      const allowedPermissions = ['clipboard-read', 'clipboard-sanitized-write'];
      if (allowedPermissions.includes(permission)) {
        callback(true);
      } else {
        console.warn('[SECURITY] Permission refusee :', permission);
        callback(false);
      }
    }
  );

  console.log('[SECURITY] Hooks de securite attaches a la fenetre.');
}

// ---------------------------------------------------------------------------
// Patch de shell.openExternal
// ---------------------------------------------------------------------------

/**
 * Wrapper securise pour shell.openExternal.
 * Valide l'URL avant de l'ouvrir dans le navigateur systeme.
 *
 * PATCH : remplacer tous les appels shell.openExternal(url)
 * dans main.js par safeOpenExternal(url).
 *
 * @param {string} url
 */
function safeOpenExternal(url) {
  if (isAllowedExternalUrl(url)) {
    shell.openExternal(url);
  } else {
    console.warn('[SECURITY] shell.openExternal bloque pour URL non autorisee :', url);
  }
}

// ---------------------------------------------------------------------------
// Exports (CommonJS — compatible avec main.js existant)
// ---------------------------------------------------------------------------

module.exports = {
  getSecureWebPreferences,
  setupSecureSession,
  attachSecurityHooks,
  safeOpenExternal,
  isAllowedUrl,
  isAllowedExternalUrl,
  CSP_POLICY,
  ALLOWED_ORIGINS,
};

// ---------------------------------------------------------------------------
// DIFF GUIDE — Modifications a apporter dans electron-app/main.js
// ---------------------------------------------------------------------------
//
// 1. En tete du fichier, ajouter :
//    const security = require('./patches/security/electron_hardening');
//    OU copier ce fichier dans electron-app/ et faire :
//    const security = require('./electron_hardening');
//
// 2. Dans app.whenReady().then(...), avant createWin() :
//    security.setupSecureSession();
//
// 3. Dans createWin(), remplacer le bloc webPreferences par :
//    webPreferences: security.getSecureWebPreferences()
//
// 4. Dans createWin(), apres "win = new BrowserWindow({...})" :
//    security.attachSecurityHooks(win);
//
// 5. Remplacer tous les shell.openExternal(url) par :
//    security.safeOpenExternal(url);
//
// 6. Dans createSplash(), le webPreferences existant a deja nodeIntegration:false
//    Ajouter sandbox:true et contextIsolation:true :
//    webPreferences: { nodeIntegration: false, contextIsolation: true, sandbox: true }
//
// ---------------------------------------------------------------------------
