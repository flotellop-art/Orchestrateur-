(function (global) {
  'use strict';

  function parseSSEFrame(frame) {
    var eventName = 'message';
    var data = [];
    var lastEventId = '';
    var retry = null;
    String(frame || '').split(/\r?\n/).forEach(function (line) {
      if (!line || line.charAt(0) === ':') return;
      var colon = line.indexOf(':');
      var field = colon < 0 ? line : line.slice(0, colon);
      var value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
      if (field === 'event') eventName = value || 'message';
      else if (field === 'data') data.push(value);
      else if (field === 'id') lastEventId = value;
      else if (field === 'retry' && /^\d+$/.test(value)) retry = Number(value);
    });
    if (!data.length) return null;
    return {type: eventName, data: data.join('\n'), lastEventId: lastEventId, retry: retry};
  }

  function storedApiKey() {
    try { return global.localStorage.getItem('ORCH_API_KEY') || ''; }
    catch (_) { return ''; }
  }

  function SecureEventSource(url, options) {
    this.url = String(url);
    this.readyState = SecureEventSource.CONNECTING;
    this.onopen = null;
    this.onmessage = null;
    this.onerror = null;
    this._listeners = {};
    this._closed = false;
    this._controller = null;
    this._retry = 1000;
    this._apiKey = options && options.apiKey;
    this._connect();
  }

  SecureEventSource.CONNECTING = 0;
  SecureEventSource.OPEN = 1;
  SecureEventSource.CLOSED = 2;

  SecureEventSource.prototype.addEventListener = function (type, callback) {
    if (typeof callback !== 'function') return;
    (this._listeners[type] = this._listeners[type] || []).push(callback);
  };

  SecureEventSource.prototype.removeEventListener = function (type, callback) {
    var list = this._listeners[type] || [];
    this._listeners[type] = list.filter(function (item) { return item !== callback; });
  };

  SecureEventSource.prototype._emit = function (type, event) {
    var handler = type === 'open' ? this.onopen : (type === 'error' ? this.onerror : null);
    if (type === 'message' && typeof this.onmessage === 'function') {
      try { this.onmessage(event); } catch (_) {}
    }
    if (typeof handler === 'function') {
      try { handler(event); } catch (_) {}
    }
    (this._listeners[type] || []).slice().forEach(function (callback) {
      try { callback(event); } catch (_) {}
    });
  };

  SecureEventSource.prototype._currentKey = function () {
    if (typeof this._apiKey === 'function') return String(this._apiKey() || '');
    if (typeof this._apiKey === 'string') return this._apiKey;
    return storedApiKey();
  };

  SecureEventSource.prototype._connect = async function () {
    var self = this;
    if (self._closed) return;
    self.readyState = SecureEventSource.CONNECTING;
    self._controller = new AbortController();
    var headers = {'Accept': 'text/event-stream'};
    var key = self._currentKey();
    if (key) headers['X-API-Key'] = key;

    try {
      var response = await global.fetch(self.url, {
        method: 'GET',
        headers: headers,
        cache: 'no-store',
        credentials: 'same-origin',
        signal: self._controller.signal
      });
      if (!response.ok || !response.body) throw new Error('SSE HTTP ' + response.status);
      self.readyState = SecureEventSource.OPEN;
      self._retry = 1000;
      self._emit('open', {type: 'open', target: self});

      var reader = response.body.getReader();
      var decoder = new TextDecoder();
      var buffer = '';
      while (!self._closed) {
        var chunk = await reader.read();
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, {stream: true});
        var boundary;
        while ((boundary = buffer.search(/\r?\n\r?\n/)) >= 0) {
          var match = buffer.slice(boundary).match(/^\r?\n\r?\n/)[0];
          var parsed = parseSSEFrame(buffer.slice(0, boundary));
          buffer = buffer.slice(boundary + match.length);
          if (!parsed) continue;
          if (parsed.retry !== null) self._retry = Math.max(250, Math.min(parsed.retry, 30000));
          var event = {
            type: parsed.type,
            data: parsed.data,
            lastEventId: parsed.lastEventId,
            target: self
          };
          self._emit(parsed.type, event);
        }
      }
      if (!self._closed) throw new Error('SSE stream closed');
    } catch (error) {
      if (self._closed || (error && error.name === 'AbortError')) return;
      self.readyState = SecureEventSource.CONNECTING;
      self._emit('error', {type: 'error', error: error, target: self});
      if (!self._closed) {
        var delay = self._retry;
        self._retry = Math.min(self._retry * 2, 30000);
        global.setTimeout(function () { self._connect(); }, delay);
      }
    }
  };

  SecureEventSource.prototype.close = function () {
    this._closed = true;
    this.readyState = SecureEventSource.CLOSED;
    if (this._controller) this._controller.abort();
  };

  global.SecureEventSource = SecureEventSource;
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = {SecureEventSource: SecureEventSource, parseSSEFrame: parseSSEFrame};
  }
})(typeof window !== 'undefined' ? window : globalThis);
