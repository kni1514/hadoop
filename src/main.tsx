import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

// The in-app "Install" popup was removed. The manifest and service worker are
// kept, so the app stays installable the browser's own way: the install icon
// in the address bar, the browser menu ("Install app" / "Add to Home screen"),
// or Safari's Share > Add to Home Screen. The install event is not
// intercepted, so the browser can show its native install UI.

// Ask the browser to keep this app's data (HDFS files, tables, scripts) even when the
// disk gets full. Installed apps are usually granted this automatically.
if (navigator.storage && typeof navigator.storage.persist === 'function') {
  navigator.storage.persisted().then((already) => {
    if (!already) navigator.storage.persist().catch(() => {});
  }).catch(() => {});
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

if ('serviceWorker' in navigator && import.meta.env.DEV) {
  // A production service worker can otherwise keep Vite's development page
  // on a cached bundle from an earlier build on the same origin and port.
  navigator.serviceWorker.getRegistrations().then((registrations) => {
    registrations.forEach((registration) => registration.unregister());
  });
  if ('caches' in window) {
    caches.keys().then((keys) => {
      keys.filter((key) => key.startsWith('hadoop-offline-')).forEach((key) => caches.delete(key));
    });
  }
} else if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker
      .register('./sw.js', { scope: './' })
      .then((registration) => {
        setInterval(() => {
          registration.update().catch(() => {});
        }, 120_000);
      })
      .catch((err) => {
        console.warn('[PWA] ServiceWorker registration error:', err);
      });
  });
}
