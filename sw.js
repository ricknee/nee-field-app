// sw.js — Northeastern Electric Field App Service Worker
// Caches the app shell so it loads instantly even with no signal

// Bumped when the shell list changes, so the activate handler drops the old
// cache. Without a bump, a phone that already installed v1 keeps serving the old
// shell list and never picks up the manifest or the icon.
const CACHE_NAME = 'nee-app-v3';

// A weak cellular connection is often worse than being fully offline: fetch()
// stays pending for a long time, so its rejection handler never gets a chance to
// serve the cached app. This is especially visible in iOS Safari. Give a page
// navigation a brief chance to get the latest deploy, then use the installed
// shell while the slow request continues in the background and refreshes the
// cache for the next launch.
const NAVIGATION_NETWORK_TIMEOUT_MS = 3000;

// Files to cache on install (the app shell)
// The manifest and icon are here so "Add to Home Screen" and the Android
// long-press shortcuts still resolve with no signal — a home-screen icon that
// only works online is worse than none, because it looks broken.
const APP_SHELL = [
  '/',
  '/index.html',
  '/manifest.json',
  '/assets/NEE-Gray-Logo-400.png',
  // The inventory app is a second installed app with its own manifest and icon,
  // and the reasoning above applies to it identically.
  '/inventory.html',
  '/manifest-inventory.json',
  '/assets/NEE-Orange-Logo-400.png'
];

// Install — cache the app shell
self.addEventListener('install', event => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL))
  );
});

// Activate — clean up old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Fetch strategy:
// - For the HTML page: Network first, fall back to cache (so updates deploy cleanly)
// - For API calls (/.netlify/functions/): Network only, never cache
// - For fonts/assets: Cache first
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // Only GET is cacheable. Without this, the catch-all handler at the bottom
  // swallowed every upload: cache.put() throws a TypeError on a non-GET
  // request, the .catch() then resolved to `undefined`, and respondWith(
  // undefined) reaches the page as a bare "Failed to fetch" — which looks
  // exactly like a CORS rejection and sent us hunting the wrong problem.
  if (event.request.method !== 'GET') {
    return; // let the browser do it
  }

  // Cross-origin requests are not ours to manage. Photo URLs are presigned and
  // carry their signature in the query string, so every fresh one is a new
  // cache key — caching them would balloon the cache with entries that expire
  // anyway. The browser's own HTTP cache handles these correctly.
  if (url.origin !== self.location.origin) {
    return;
  }

  // Never cache API calls — always go to network
  if (url.pathname.startsWith('/.netlify/')) {
    return; // Let browser handle normally
  }

  // For the main HTML document: network first, cache fallback
  if (event.request.mode === 'navigate') {
    const network = fetch(event.request);

    // Keep the worker alive for the cache refresh even when the timeout below
    // wins the race and the cached page has already been returned to Safari.
    event.waitUntil(
      network.then(response => {
        // Do not replace a working cached app with an error page.
        if (response && response.ok) {
          const clone = response.clone();
          return caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
      }).catch(() => {})
    );

    const cached = caches.match(event.request).then(response =>
      response || caches.match('/index.html')
    );

    const timeout = new Promise(resolve => {
      setTimeout(() => resolve(cached), NAVIGATION_NETWORK_TIMEOUT_MS);
    });

    event.respondWith(
      Promise.race([network, timeout]).catch(() => cached)
    );
    return;
  }

  // For everything else (fonts, images): cache first
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;
      return fetch(event.request).then(response => {
        if (response && response.status === 200) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      }).catch(() => cached);
    })
  );
});
