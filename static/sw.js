// Service worker minimal pour l'installation PWA + cache du "shell" de l'UI.
// IMPORTANT : on ne touche JAMAIS aux appels /api/ (SSE, POST, streaming) -> toujours reseau direct.
const CACHE = 'orchestrateur-v2';
const SHELL = ['/workspace', '/skills', '/automations', '/static/workspace.html', '/static/manifest.webmanifest', '/static/icon-app.svg'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL).catch(() => {})));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  // API / SSE / non-GET : laisser passer au reseau sans interception.
  if (url.pathname.startsWith('/api/') || e.request.method !== 'GET') return;
  // Shell de l'app : reseau d'abord, cache en repli (hors-ligne).
  e.respondWith(
    fetch(e.request)
      .then((r) => {
        if (r && r.ok) {
          const copy = r.clone();
          caches.open(CACHE).then((c) => c.put(e.request, copy));
        }
        return r;
      })
      .catch(() => caches.match(e.request))
  );
});
