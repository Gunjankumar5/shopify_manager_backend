"""
Shared store persistence utilities used by all routes.

Stores are persisted in Supabase `connected_stores` table — NOT local JSON files.
This means store connections survive Railway redeploys and work from any device.
"""
from contextvars import ContextVar
import time, requests, logging, os
from fastapi import HTTPException

logger = logging.getLogger(__name__)

REQUEST_USER_ID: ContextVar[str | None] = ContextVar("request_user_id", default=None)
TOKEN_REFRESH_WINDOW_SECONDS = 300


def set_request_user_id(user_id: str | None):
    REQUEST_USER_ID.set(user_id)


def get_request_user_id() -> str | None:
    return REQUEST_USER_ID.get()


# ── Supabase client ───────────────────────────────────────────────────────────

def _get_supabase():
    """Get Supabase admin client using service role key (bypasses RLS)."""
    try:
        from supabase import create_client
        url = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment variables")
        return create_client(url, key)
    except ImportError:
        raise HTTPException(status_code=503, detail="supabase package not installed. Run: pip install supabase")
    except Exception as e:
        logger.error(f"Supabase client error: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")


# ── Row → dict helper ─────────────────────────────────────────────────────────

def _row_to_store(row: dict) -> dict:
    return {
        "shop":             row.get("shop"),
        "shop_name":        row.get("shop_name"),
        "api_key":          row.get("api_key"),
        "api_secret":       row.get("api_secret"),
        "access_token":     row.get("access_token"),
        "token_expires_at": row.get("token_expires_at"),
        "api_version":      row.get("api_version", "2026-01"),
        "connected_at":     str(row.get("connected_at", "")),
        "email":            row.get("email", ""),
        "currency":         row.get("currency", ""),
        "is_active":        row.get("is_active", False),
    }


# ── Store CRUD ────────────────────────────────────────────────────────────────

def load_stores(user_id: str | None = None) -> dict:
    """Load all stores for a user. Returns { shop_key: store_dict }"""
    resolved = user_id or get_request_user_id()
    if not resolved:
        return {}
    try:
        result = _get_supabase() \
            .table("connected_stores") \
            .select("*") \
            .eq("user_id", resolved) \
            .execute()
        return {
            row["shop_key"]: _row_to_store(row)
            for row in (result.data or [])
            if row.get("shop_key")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"load_stores error: {e}")
        return {}


def save_stores(stores: dict, user_id: str | None = None):
    """Save multiple stores to Supabase."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        raise HTTPException(status_code=401, detail="Authentication required")
    for shop_key, store in stores.items():
        save_single_store(shop_key, store, user_id=resolved)


def save_single_store(shop_key: str, store: dict, user_id: str | None = None):
    """Upsert a single store in Supabase."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        _get_supabase().table("connected_stores").upsert({
            "user_id":          resolved,
            "shop_key":         shop_key,
            "shop":             store.get("shop", ""),
            "shop_name":        store.get("shop_name", ""),
            "api_key":          store.get("api_key", ""),
            "api_secret":       store.get("api_secret", ""),
            "access_token":     store.get("access_token", ""),
            "token_expires_at": store.get("token_expires_at"),
            "api_version":      store.get("api_version", "2026-01"),
            "email":            store.get("email", ""),
            "currency":         store.get("currency", ""),
            "is_active":        store.get("is_active", False),
        }, on_conflict="user_id,shop_key").execute()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"save_single_store error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save store: {str(e)}")


def delete_store(shop_key: str, user_id: str | None = None):
    """Delete a store from Supabase."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        _get_supabase().table("connected_stores") \
            .delete() \
            .eq("user_id", resolved) \
            .eq("shop_key", shop_key) \
            .execute()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"delete_store error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete store: {str(e)}")


# ── Active store ──────────────────────────────────────────────────────────────

def get_active_store_key(user_id: str | None = None) -> str | None:
    """Get active store key for a user from Supabase."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        return None
    try:
        sb = _get_supabase()
        # Try explicitly active store first
        r = sb.table("connected_stores") \
              .select("shop_key") \
              .eq("user_id", resolved) \
              .eq("is_active", True) \
              .limit(1) \
              .execute()
        if r.data:
            return r.data[0]["shop_key"]
        # Fallback: most recently connected
        r = sb.table("connected_stores") \
              .select("shop_key") \
              .eq("user_id", resolved) \
              .order("connected_at", desc=True) \
              .limit(1) \
              .execute()
        if r.data:
            return r.data[0]["shop_key"]
        return None
    except Exception as e:
        logger.error(f"get_active_store_key error: {e}")
        return None


def set_active_store_key(shop_key: str | None, user_id: str | None = None):
    """Set active store for a user in Supabase."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        sb = _get_supabase()
        # Deactivate all
        sb.table("connected_stores") \
          .update({"is_active": False}) \
          .eq("user_id", resolved) \
          .execute()
        # Activate selected
        if shop_key:
            sb.table("connected_stores") \
              .update({"is_active": True}) \
              .eq("user_id", resolved) \
              .eq("shop_key", shop_key) \
              .execute()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"set_active_store_key error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to set active store: {str(e)}")


# ── Token refresh ─────────────────────────────────────────────────────────────

def _refresh_store_token(shop_key: str, store: dict, user_id: str | None = None) -> dict:
    """Refresh token and save updated store to Supabase."""
    logger.info(f"Refreshing token for {shop_key}...")
    try:
        r = requests.post(
            f"https://{store['shop']}/admin/oauth/access_token",
            data={
                "client_id":     store.get("api_key"),
                "client_secret": store.get("api_secret"),
                "grant_type":    "client_credentials",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            store["access_token"]     = data.get("access_token")
            store["token_expires_at"] = time.time() + data.get("expires_in", 3600)
            save_single_store(shop_key, store, user_id=user_id)
            logger.info(f"✅ Token refreshed for {shop_key}")
    except Exception as e:
        logger.warning(f"Token refresh failed for {shop_key}: {e}")
    return store


# ── Main helpers ──────────────────────────────────────────────────────────────

def get_connected_store(shop_key: str | None = None, user_id: str | None = None) -> dict | None:
    """Return active store dict, auto-refreshing token if expiring soon."""
    resolved = user_id or get_request_user_id()
    if not resolved:
        return None
    try:
        stores    = load_stores(resolved)
        if not stores:
            return None
        store_key = shop_key or get_active_store_key(resolved)
        if not store_key or store_key not in stores:
            return None
        store = stores[store_key]
        # Auto-refresh if expiring within 5 minutes
        expires = store.get("token_expires_at", 0)
        if expires and time.time() > expires - TOKEN_REFRESH_WINDOW_SECONDS:
            store = _refresh_store_token(store_key, store, user_id=resolved)
        return store
    except Exception as e:
        logger.error(f"get_connected_store error: {e}")
        return None


def get_shopify_client(shop_key: str | None = None, user_id: str | None = None):
    """Return ShopifyClient initialised with active store credentials."""
    from shopify_client import ShopifyClient

    resolved = user_id or get_request_user_id()
    if not resolved:
        raise HTTPException(status_code=401, detail="Authentication required")

    store = get_connected_store(shop_key=shop_key, user_id=resolved)
    if not store:
        raise HTTPException(
            status_code=400,
            detail="No Shopify store connected. Please connect a store first.",
        )

    shop_name    = store.get("shop") or store.get("shop_name")
    access_token = store.get("access_token")
    api_version  = store.get("api_version", "2026-01")

    if not shop_name or not access_token:
        raise HTTPException(
            status_code=400,
            detail="Invalid store credentials. Please reconnect the store.",
        )

    try:
        return ShopifyClient(
            shop_name=shop_name,
            access_token=access_token,
            api_version=api_version,
        )
    except Exception as e:
        logger.error(f"ShopifyClient init error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initialise Shopify client: {str(e)}")


def get_store_access_token(
    shop_key: str | None = None,
    force_refresh: bool = False,
    user_id: str | None = None,
) -> str | None:
    """Get access token for a store, optionally forcing refresh."""
    resolved = user_id or get_request_user_id()
    store    = get_connected_store(shop_key=shop_key, user_id=resolved)
    if not store:
        return None
    if force_refresh:
        active_key = shop_key or get_active_store_key(resolved)
        if active_key:
            store = _refresh_store_token(active_key, store, user_id=resolved)
    return store.get("access_token")


def load_all_user_stores() -> dict[str, dict]:
    """Load all stores across all users (admin/background tasks only)."""
    try:
        result = _get_supabase().table("connected_stores").select("*").execute()
        all_stores: dict[str, dict] = {}
        for row in (result.data or []):
            uid      = row.get("user_id")
            shop_key = row.get("shop_key")
            if uid and shop_key:
                all_stores.setdefault(uid, {})[shop_key] = _row_to_store(row)
        return all_stores
    except Exception as e:
        logger.error(f"load_all_user_stores error: {e}")
        return {}