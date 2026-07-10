'use strict';

const assert = require('assert');
const {SecureEventSource, parseSSEFrame} = require('../static/secure-sse.js');

async function main() {
  const parsed = parseSSEFrame('event: task_update\ndata: {"ok":true}');
  assert.strictEqual(parsed.type, 'task_update');
  assert.strictEqual(parsed.data, '{"ok":true}');

  let requestUrl = null;
  let requestHeaders = null;
  global.fetch = async (url, options) => {
    requestUrl = url;
    requestHeaders = options.headers;
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

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('SSE test timeout')), 1000);
    const source = new SecureEventSource('/api/events', {apiKey: 'header-secret'});
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
  assert.strictEqual(requestHeaders['X-API-Key'], 'header-secret');
  assert.ok(!requestUrl.includes('api_key'));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
