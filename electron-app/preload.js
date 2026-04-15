/**
 * Preload du shell Electron Orchestrateur.
 *
 * Exposé à Arty sous `window.orchestrateur` :
 *   - `origin`          : URL du backend local (toujours http://127.0.0.1:8000).
 *   - `version`         : version du shell (package.json).
 *   - `isElectronShell` : true — Arty peut ainsi adapter son UI (masquer la
 *                         bannière « installer l'Orchestrateur » par exemple).
 *
 * On reste volontairement minimal : aucune API Node n'est exposée à la page
 * distante. Les échanges métier passent par des fetch HTTP vers l'origine
 * `http://127.0.0.1:8000` — la même API que celle utilisée par la version
 * web d'Arty (cf. appfacade-integration/src/services/orchestrateurClient.ts).
 */

'use strict';

const { contextBridge } = require('electron');

// Version figée à la main : en sandbox, require('./package.json') n'est pas
// garanti de fonctionner. Synchroniser avec electron-app/package.json.
const SHELL_VERSION = '0.3.0';

contextBridge.exposeInMainWorld('orchestrateur', Object.freeze({
  isElectronShell: true,
  origin: 'http://127.0.0.1:8000',
  version: SHELL_VERSION,
}));
