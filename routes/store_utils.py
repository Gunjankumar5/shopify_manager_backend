"""
Shared store persistence utilities used by all routes.

Data files live in  backend/data/
    stores.json       — credentials + tokens scoped per authenticated user
    active_store.json — active store key scoped per authenticated user
"""
from pathlib import Path
from contextvars import ContextVar
import json, time, requests, logging
from fastapi import HTTPException

logger = logging.getLogger(__name__)

DATA_DIR         = Path(__file__).parent.parent / "data"
STORES_FILE      = DATA_DIR / "stores.json"
ACTIVE_STORE_FILE = DATA_DIR / "active_store.json"
TOKEN_REFRESH_WINDOW_SECONDS = 300
REQUEST_USER_ID: ContextVar[str | None] = ContextVar("request_user_id", default=None)


def set_request_user_id(user_id: str | None):
    REQUEST_USER_ID.set(user_id)


def get_request_user_id() -> str | None:
    return REQUEST_USER_ID.get()


def _is_store_record(value: object) -> bool:
    return isinstance(value, dict) and "shop" in value


def _read_stores_raw() -> dict:
    if not STORES_FILE.exists():
        return {}
    try:
        data = json.loads(STORES_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_stores_raw(data: dict):
    DATA_DIR.mkdir(exist_ok=True)
    STORES_FILE.write_text(json.dumps(data, indent=2))


def _read_active_map() -> dict:
    if not ACTIVE_STORE_FILE.exists():
        return {}
    try:
        data = json.loads(ACTIVE_STORE_FILE.read_text())
        if not isinstance(data, dict):
            return {}
        if "active_store" in data:
            # Backward compatibility with old single-active-store format.
            value = data.get("active_store")
            return {"__legacy__": value} if value else {}
        return data
    except Exception:
        return {}


def _write_active_map(data: dict):
    DATA_DIR.mkdir(exist_ok=True)
    if not data:
        if ACTIVE_STORE_FILE.exists():
            ACTIVE_STORE_FILE.unlink()
        return
    ACTIVE_STORE_FILE.write_text(json.dumps(data, indent=2))


def _migrate_legacy_if_needed(user_id: str):
    raw = _read_stores_raw()
    if not raw or "users" in raw:
        return

    legacy_stores = {k: v for k, v in raw.items() if _is_store_record(v)}
    if not legacy_stores:
        return

    logger.info(f"Migrating legacy stores.json to user-scoped schema for {user_id}")
    _write_stores_raw({
        "users": {
            user_id: {
                "stores": legacy_stores,
            }
        }
    })

    active_map = _read_active_map()
    legacy_active = active_map.pop("__legacy__", None)
    if legacy_active and user_id not in active_map:
        active_map[user_id] = legacy_active
        _write_active_map(active_map)


# ── File I/O ──────────────────────────────────────────────────────────────────

def load_stores(user_id: str | None = None) -> dict:
    resolved_user_id = user_id or get_request_user_id()
    if not resolved_user_id:
        return {}

    _migrate_legacy_if_needed(resolved_user_id)
    raw = _read_stores_raw()
    users = raw.get("users", {}) if isinstance(raw.get("users"), dict) else {}
    user_entry = users.get(resolved_user_id, {})
    stores = user_entry.get("stores", {}) if isinstance(user_entry, dict) else {}
    return stores if isinstance(stores, dict) else {}


def load_all_user_stores() -> dict[str, dict]:
    raw = _read_stores_raw()
    if not raw:
        return {}

    if "users" in raw:
        users = raw.get("users", {}) if isinstance(raw.get("users"), dict) else {}
        result: dict[str, dict] = {}
        for user_id, user_entry in users.items():
            if not isinstance(user_entry, dict):
                continue
            stores = user_entry.get("stores", {})
            if isinstance(stores, dict):
                result[user_id] = stores
        return result

    # Legacy fallback
    legacy_stores = {k: v for k, v in raw.items() if _is_store_record(v)}
    return {"__legacy__": legacy_stores} if legacy_stores else {}


def save_stores(stores: dict, user_id: str | None = None):
    resolved_user_id = user_id or get_request_user_id()
    if not resolved_user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    _migrate_legacy_if_needed(resolved_user_id)
    raw = _read_stores_raw()
    users = raw.get("users", {}) if isinstance(raw.get("users"), dict) else {}
    user_entry = users.get(resolved_user_id, {})
    if not isinstance(user_entry, dict):
        user_entry = {}
    user_entry["stores"] = stores
    users[resolved_user_id] = user_entry
    _write_stores_raw({"users": users})


# ── Active-store tracking ─────────────────────────────────────────────────────

def get_active_store_key(user_id: str | None = None) -> str | None:
    resolved_user_id = user_id or get_request_user_id()
    if not resolved_user_id:
        return None

    active_map = _read_active_map()
    if resolved_user_id in active_map:
        return active_map.get(resolved_user_id)

    # Fallback: most recently connected store for this user.
    stores = load_stores(resolved_user_id)
    if not stores:
        return None
    return sorted(
        stores.keys(),
        key=lambda k: stores[k].get("connected_at", ""),
        reverse=True,
    )[0]


def set_active_store_key(shop_key: str | None, user_id: str | None = None):
    resolved_user_id = user_id or get_request_user_id()
    if not resolved_user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    active_map = _read_active_map()
    if shop_key is None:
        active_map.pop(resolved_user_id, None)
    else:
        active_map[resolved_user_id] = shop_key
    _write_active_map(active_map)


# ── Store + client helpers ────────────────────────────────────────────────────

def _refresh_store_token(stores: dict, store_key: str, store: dict, user_id: str | None = None) -> dict:
    logger.info(f"Token refresh requested for {store_key}")
    try:
        r = requests.post(
            f"https://{store['shop']}/admin/oauth/access_token",
            data={
                "client_id": store.get("api_key"),
                "client_secret": store.get("api_secret"),
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        r.raise_for_status()
        token_data = r.json()
        store["access_token"] = token_data.get("access_token")
        store["token_expires_at"] = time.time() + token_data.get("expires_in", 3600)
        stores[store_key] = store
        save_stores(stores, user_id=user_id)
        logger.info(f"Token refreshed for {store_key}")
    except Exception as e:
        logger.warning(f"Token refresh failed for {store_key}: {e}")
    return store


def _get_store_record(
    shop_key: str | None = None,
    force_refresh: bool = False,
    user_id: str | None = None,
) -> tuple[str | None, dict | None]:
    try:
        resolved_user_id = user_id or get_request_user_id()
        if not resolved_user_id:
            return None, None

        stores = load_stores(resolved_user_id)
        if not stores:
            return None, None

        resolved_key = shop_key or get_active_store_key(resolved_user_id)
        if not resolved_key or resolved_key not in stores:
            return None, None

        store = stores[resolved_key]
        token_expires_at = store.get("token_expires_at", 0)
        should_refresh = force_refresh or (
            token_expires_at and time.time() > token_expires_at - TOKEN_REFRESH_WINDOW_SECONDS
        )

        if should_refresh:
            store = _refresh_store_token(stores, resolved_key, store, user_id=resolved_user_id)

        return resolved_key, store
    except Exception as e:
        logger.error(f"Error loading store: {e}")
        return None, None

def get_connected_store(user_id: str | None = None) -> dict | None:
    """Return the active store dict, auto-refreshing the token if it's about to expire."""
    _, store = _get_store_record(user_id=user_id)
    return store


def get_store_access_token(
    shop_key: str | None = None,
    force_refresh: bool = False,
    user_id: str | None = None,
) -> str | None:
    _, store = _get_store_record(shop_key=shop_key, force_refresh=force_refresh, user_id=user_id)
    if not store:
        return None
    return store.get("access_token")


def get_shopify_client(shop_key: str | None = None, user_id: str | None = None):
    """Return a ShopifyClient initialised with the selected store's credentials."""
    from shopify_client import ShopifyClient

    resolved_user_id = user_id or get_request_user_id()
    if not resolved_user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    resolved_key, store = _get_store_record(shop_key=shop_key, user_id=resolved_user_id)
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
            token_refresh_callback=lambda: get_store_access_token(
                resolved_key,
                force_refresh=True,
                user_id=resolved_user_id,
            ),
        )
    except Exception as e:
        logger.error(f"Error initialising ShopifyClient: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initialise Shopify client: {str(e)}")
