// sw.js — Northeastern Electric Field App Service Worker
// Caches the app shell so it loads instantly even with no signal

const CACHE_NAME = 'nee-app-v1';

// Files to cache on install (the app shell)
const APP_SHELL = [
  '/',
  '/index.html'
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

  // Never cache API calls — always go to network
  if (url.pathname.startsWith('/.netlify/')) {
    return; // Let browser handle normally
  }

  // For the main HTML document: network first, cache fallback
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then(response => {
          // Update cache with fresh version
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          return response;
        })
        .catch(() => caches.match('/index.html'))
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
