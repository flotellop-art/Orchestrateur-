(function (global) {
  'use strict';

  function parsePayload(raw) {
    var text = String(raw == null ? '' : raw);
    var event;
    try {
      event = JSON.parse(text);
    } catch (_) {
      return {parsed: false, raw: text, event: null};
    }
    if (!event || typeof event !== 'object' || Array.isArray(event)) {
      return {parsed: false, raw: text, event: null};
    }
    if (event.type === 'error') {
      throw new Error(event.message || 'Erreur du chat');
    }
    return {parsed: true, raw: text, event: event};
  }

  global.OrchestratorChatStream = {parsePayload: parsePayload};
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = {parsePayload: parsePayload};
  }
})(typeof window !== 'undefined' ? window : globalThis);
