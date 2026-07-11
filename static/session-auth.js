(function (global) {
  'use strict';

  // Les anciennes versions conservaient la cle maitre sans limite de duree.
  // Elle n'est volontairement pas migree : l'utilisateur doit la saisir de
  // nouveau pour la session courante.
  try {
    global.localStorage.removeItem('ORCH_API_KEY');
  } catch (_) {}
})(typeof window !== 'undefined' ? window : globalThis);
