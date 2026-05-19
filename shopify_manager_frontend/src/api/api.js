import { API_BASE_URL } from "./config";
import { getAuthHeaders } from "../lib/authFetch";

const GET_CACHE_TTL_MS = 30000;
const getCache = new Map();
const inflightGets = new Map();

// ── Cache statistics for monitoring ────────────────────────────────────────────
const cacheStats = {
  hits: 0,
  misses: 0,
  get hitRate() {
    const total = this.hits + this.misses;
    return total === 0 ? 0 : ((this.hits / total) * 100).toFixed(1);
  },
};

// Log cache stats every minute (development only)
if (import.meta.env.DEV) {
  setInterval(() => {
    if (localStorage.getItem('DEBUG_CACHE')) {
      console.log(`[Cache Stats] Hits: ${cacheStats.hits}, Misses: ${cacheStats.misses}, Rate: ${cacheStats.hitRate}%`);
    }
  }, 60000);
}

function clearGetCache() {
  getCache.clear();
  inflightGets.clear();
  try {
    // remove persistent entries too
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith("api_cache:")) localStorage.removeItem(key);
    }
  } catch {}
}

function clearProductCache() {
  const keysToDelete = Array.from(getCache.keys()).filter(
    (key) =>
      key.includes("/products") ||
      key.includes("/inventory") ||
      key.includes("/collections"),
  );
  keysToDelete.forEach((key) => getCache.delete(key));

  // Also clear inflight requests for these endpoints
  const inflightKeysToDelete = Array.from(inflightGets.keys()).filter(
    (key) =>
      key.includes("/products") ||
      key.includes("/inventory") ||
      key.includes("/collections"),
  );
  inflightKeysToDelete.forEach((key) => inflightGets.delete(key));
}

function getCachedValue(url, ttlMs) {
  const cached = getCache.get(url);
  if (!cached) {
    cacheStats.misses++;
    return null;
  }
  // Sticky entries never expire until explicitly cleared
  if (cached.sticky) {
    cacheStats.hits++;
    return cached.data;
  }
  if (ttlMs !== Infinity && Date.now() - cached.ts > ttlMs) {
    getCache.delete(url);
    cacheStats.misses++;
    return null;
  }
  cacheStats.hits++;
  return cached.data;
}

function storeCachedValue(url, data) {
  getCache.set(url, { data, ts: Date.now() });
}

async function parseResponseBody(response) {
  const contentType = (
    response.headers.get("content-type") || ""
  ).toLowerCase();
  if (response.status === 204) {
    return null;
  }

  if (contentType.includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }

  try {
    const text = await response.text();
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch {
      return { message: text };
    }
  } catch {
    return null;
  }
}

function extractErrorMessage(data, status) {
  if (data && typeof data === "object") {
    if (typeof data.detail === "string" && data.detail.trim())
      return data.detail;
    if (typeof data.message === "string" && data.message.trim())
      return data.message;
    if (Array.isArray(data.detail) && data.detail.length) {
      return data.detail.map((item) => JSON.stringify(item)).join("; ");
    }
  }
  return `HTTP ${status}`;
}

// Helper: fetch with timeout
async function fetchWithTimeout(url, options = {}, timeoutMs = 120000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const headers = await getAuthHeaders(options.headers || {});
    const response = await fetch(url, {
      ...options,
      headers,
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new Error(`Request timeout after ${timeoutMs / 1000}s`);
    }
    throw error;
  }
}

export const api = {
  get: async (p, options = {}) => {
    const url = `${API_BASE_URL}${p}`;
    const ttlMs = options.ttlMs ?? GET_CACHE_TTL_MS;
    const force = Boolean(options.force);
    const persist = Boolean(options.persist);
    console.log(`GET ${url}`);

    // Check persistent localStorage first (if requested)
    const storageKey = `api_cache:${url}`;
    if (!force && persist) {
      try {
        const raw = localStorage.getItem(storageKey);
        if (raw) {
          const parsed = JSON.parse(raw);
          // If entry was stored as sticky, return it regardless of ts
          if (parsed.sticky || ttlMs === Infinity) return parsed.data;
          if (Date.now() - parsed.ts < ttlMs) return parsed.data;
          localStorage.removeItem(storageKey);
        }
      } catch {}
    }

    if (!force) {
      const cached = getCachedValue(url, ttlMs);
      if (cached !== null) {
        return cached;
      }

      const inflight = inflightGets.get(url);
      if (inflight) {
        return inflight;
      }
    }

    const request = (async () => {
      const r = await fetchWithTimeout(url, { headers: {} }, 120000);
      const data = await parseResponseBody(r);
      if (!r.ok) {
        const error = new Error(extractErrorMessage(data, r.status));
        error.status = r.status;
        throw error;
      }
      const sticky = Boolean(options.sticky);
      // store in-memory
      getCache.set(url, { data, ts: Date.now(), sticky });
      if (persist) {
        try {
          localStorage.setItem(
            storageKey,
            JSON.stringify({ data, ts: Date.now(), sticky }),
          );
        } catch {}
      }
      return data;
    })();

    inflightGets.set(url, request);

    try {
      return await request;
    } finally {
      inflightGets.delete(url);
    }
  },

  post: async (p, b) => {
    const url = `${API_BASE_URL}${p}`;
    console.log(`POST ${url}`, b);
    const r = await fetchWithTimeout(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(b),
    }, 120000);
    const data = await parseResponseBody(r);
    if (!r.ok) {
      const error = new Error(extractErrorMessage(data, r.status));
      error.status = r.status;
      throw error;
    }
    clearProductCache();
    return data;
  },

  put: async (p, b) => {
    const url = `${API_BASE_URL}${p}`;
    console.log(`PUT ${url}`, b);
    const r = await fetchWithTimeout(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(b),
    }, 120000);
    const data = await parseResponseBody(r);
    if (!r.ok) {
      const error = new Error(extractErrorMessage(data, r.status));
      error.status = r.status;
      throw error;
    }
    clearProductCache();
    return data;
  },

  delete: async (p) => {
    const url = `${API_BASE_URL}${p}`;
    console.log(`DELETE ${url}`);
    const r = await fetchWithTimeout(url, { method: "DELETE" }, 120000);
    const data = await parseResponseBody(r);
    if (!r.ok) {
      const error = new Error(extractErrorMessage(data, r.status));
      error.status = r.status;
      throw error;
    }
    clearProductCache();
    return data;
  },

  upload: async (p, fd) => {
    const url = `${API_BASE_URL}${p}`;
    console.log(`UPLOAD ${url}`);
    const r = await fetchWithTimeout(url, { 
      method: "POST", 
      body: fd 
    }, 300000);
    const data = await parseResponseBody(r);
    if (!r.ok) {
      const error = new Error(extractErrorMessage(data, r.status));
      error.status = r.status;
      throw error;
    }
    clearProductCache();
    return data;
  },
  clearCache: clearGetCache,
};
