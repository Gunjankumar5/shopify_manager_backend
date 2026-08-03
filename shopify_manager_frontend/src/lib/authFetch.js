import { supabase } from "./supabaseClient";

let cachedAccessToken = null;
let tokenLoaded = false;

if (supabase) {
  supabase.auth.onAuthStateChange((_event, session) => {
    cachedAccessToken = session?.access_token || null;
    tokenLoaded = true;
  });
}

export async function getAuthHeaders(existingHeaders = {}) {
  if (!supabase) {
    return existingHeaders;
  }

  if (tokenLoaded && !cachedAccessToken) {
    return existingHeaders;
  }

  if (cachedAccessToken) {
    return {
      ...existingHeaders,
      Authorization: `Bearer ${cachedAccessToken}`,
    };
  }

  try {
    const {
      data: { session },
    } = await supabase.auth.getSession();
    const accessToken = session?.access_token;
    cachedAccessToken = accessToken || null;
    tokenLoaded = true;
    if (!accessToken) {
      return existingHeaders;
    }
    return {
      ...existingHeaders,
      Authorization: `Bearer ${accessToken}`,
    };
  } catch {
    return existingHeaders;
  }
}

export async function authFetch(url, options = {}) {
  const headers = await getAuthHeaders(options.headers || {});
  return fetch(url, {
    ...options,
    headers,
  });
}
