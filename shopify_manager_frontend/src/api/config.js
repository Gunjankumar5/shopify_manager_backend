import { getAuthHeaders } from "../lib/authFetch";

// ── Resolve API origin dynamically ───────────────────────────────────────────
// Priority:
//   1. VITE_API_ORIGIN env var (set in Vercel for production)
//   2. localhost:8000 (local development only)
//
// NEVER hardcode a URL here — set VITE_API_ORIGIN in Vercel dashboard instead.

const isLocalDev =
  window.location.hostname === "localhost" ||
  window.location.hostname === "127.0.0.1";

const rawOrigin = (import.meta.env.VITE_API_ORIGIN || "").trim().replace(/\/$/, "");

const resolvedOrigin = (() => {
  if (rawOrigin) {
    // Ensure protocol is present
    return /^https?:\/\//i.test(rawOrigin) ? rawOrigin : `https://${rawOrigin}`;
  }
  if (isLocalDev) {
    return "http://127.0.0.1:8000";
  }
  // In production with no env var — log a clear warning
  console.error(
    "[config] VITE_API_ORIGIN is not set! " +
    "Go to Vercel → Settings → Environment Variables and add it."
  );
  return "";
})();

export const API_ORIGIN     = resolvedOrigin;
export const API_BASE_URL   = `${resolvedOrigin}/api`;
export const API_HEALTH_URL = `${resolvedOrigin}/health`;

export const API_ENDPOINTS = {
  products:        `${API_BASE_URL}/products`,
  collections:     `${API_BASE_URL}/collections/`,
  locations:       `${API_BASE_URL}/inventory/locations`,
  inventoryLevels: `${API_BASE_URL}/inventory/levels`,
};

export async function fetchJson(url, options = {}) {
  if (!API_ORIGIN) {
    throw new Error(
      "Backend URL is not configured. " +
      "Set VITE_API_ORIGIN in your Vercel environment variables."
    );
  }

  const headers = await getAuthHeaders(options.headers || {});
  const res     = await fetch(url, { headers, ...options });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${url} ${text}`);
  }

  return res.json();
}
