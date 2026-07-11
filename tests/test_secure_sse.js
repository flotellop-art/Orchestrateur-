'use strict';

const assert = require('assert');
const {SecureEventSource, parseSSEFrame} = require('../static/secure-sse.js');
const {parsePayload} = require('../static/chat-stream-utils.js');

async function main() {
  const parsed = parseSSEFrame('event: task_update\ndata: {"ok":true}');
  assert.strictEqual(parsed.type, 'task_update');
  assert.strictEqual(parsed.data, '{"ok":true}');

  let requestUrl = null;
  let requestHeaders = null;
  let requestRedirect = null;
  let legacyRemoval = null;
  global.sessionStorage = {
    getItem(name) {
      assert.strictEqual(name, 'ORCH_API_KEY');
      return 'session-secret';
    },
    setItem() { throw new Error('legacy key must not be copied to session storage'); }
  };
  global.localStorage = {
    getItem() { throw new Error('persistent storage must not be read'); },
    removeItem(name) { legacyRemoval = name; }
  };
  require('../static/session-auth.js');
  assert.strictEqual(legacyRemoval, 'ORCH_API_KEY');

  global.location = {href: 'http://localhost/control', origin: 'http://localhost'};
  global.fetch = async (url, options) => {
    requestUrl = url;
    requestHeaders = options.headers;
    requestRedirect = options.redirect;
    let sent = false;
    return {
      ok: true,
      status: 200,
      body: {
        getReader() {
          return {
            async read() {
              if (sent) return {done: true};
              sent = true;
              return {
                done: false,
                value: new TextEncoder().encode('event: connected\ndata: {"ready":true}\n\n')
              };
            }
          };
        }
      }
    };
  };

  assert.throws(
    () => new SecureEventSource('https://evil.example/collect'),
    /autre origine/
  );
  assert.strictEqual(requestUrl, null, 'cross-origin URL must be rejected before fetch');

  assert.strictEqual(parsePayload('not-json').parsed, false);
  assert.deepStrictEqual(parsePayload('{"type":"text_delta","text":"ok"}').event,
    {type: 'text_delta', text: 'ok'});
  assert.throws(() => parsePayload('{"type":"error","message":"modele indisponible"}'),
    /modele indisponible/);

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('SSE test timeout')), 1000);
    const source = new SecureEventSource('/api/events');
    source.addEventListener('connected', event => {
      try {
        assert.deepStrictEqual(JSON.parse(event.data), {ready: true});
        source.close();
        clearTimeout(timer);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  });

  assert.strictEqual(requestUrl, '/api/events');
  assert.strictEqual(requestHeaders['X-API-Key'], 'session-secret');
  assert.strictEqual(requestRedirect, 'error');
  assert.ok(!requestUrl.includes('api_key'));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
