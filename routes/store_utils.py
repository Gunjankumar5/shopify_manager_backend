"""
Shared store persistence utilities used by all routes.

Data files live in  backend/data/
  stores.json       — credentials + tokens for every connected store
  active_store.json — single key identifying the currently selected store
"""
from pathlib import Path
import json, time, requests, logging
from fastapi import HTTPException

logger = logging.getLogger(__name__)

DATA_DIR         = Path(__file__).parent.parent / "data"
STORES_FILE      = DATA_DIR / "stores.json"
ACTIVE_STORE_FILE = DATA_DIR / "active_store.json"


# ── File I/O ──────────────────────────────────────────────────────────────────

def load_stores() -> dict:
    if STORES_FILE.exists():
        try:
            return json.loads(STORES_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_stores(stores: dict):
    DATA_DIR.mkdir(exist_ok=True)
    STORES_FILE.write_text(json.dumps(stores, indent=2))


# ── Active-store tracking ─────────────────────────────────────────────────────

def get_active_store_key() -> str | None:
    if ACTIVE_STORE_FILE.exists():
        try:
            data = json.loads(ACTIVE_STORE_FILE.read_text())
            return data.get("active_store")
        except Exception:
            pass
    # Fallback: most recently connected store
    stores = load_stores()
    if not stores:
        return None
    return sorted(
        stores.keys(),
        key=lambda k: stores[k].get("connected_at", ""),
        reverse=True,
    )[0]


def set_active_store_key(shop_key: str | None):
    DATA_DIR.mkdir(exist_ok=True)
    if shop_key is None:
        if ACTIVE_STORE_FILE.exists():
            ACTIVE_STORE_FILE.unlink()
    else:
        ACTIVE_STORE_FILE.write_text(json.dumps({"active_store": shop_key}, indent=2))


# ── Store + client helpers ────────────────────────────────────────────────────

def get_connected_store() -> dict | None:
    """Return the active store dict, auto-refreshing the token if it's about to expire."""
    try:
        stores = load_stores()
        if not stores:
            return None

        store_key = get_active_store_key()
        if not store_key or store_key not in stores:
            return None

        store = stores[store_key]

        # Refresh token if expiring within 5 minutes
        token_expires_at = store.get("token_expires_at", 0)
        if token_expires_at and time.time() > token_expires_at - 300:
            logger.info(f"Token expiring soon for {store_key}, refreshing...")
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
                    token_data = r.json()
                    store["access_token"]    = token_data.get("access_token")
                    store["token_expires_at"] = time.time() + token_data.get("expires_in", 3600)
                    stores[store_key] = store
                    save_stores(stores)
                    logger.info(f"Token refreshed for {store_key}")
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}, using existing token")

        return stores[store_key]
    except Exception as e:
        logger.error(f"Error loading store: {e}")
        return None


def get_shopify_client():
    """Return a ShopifyClient initialised with the active store's credentials."""
    from shopify_client import ShopifyClient

    store = get_connected_store()
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
        return ShopifyClient(shop_name=shop_name, access_token=access_token, api_version=api_version)
    except Exception as e:
        logger.error(f"Error initialising ShopifyClient: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initialise Shopify client: {str(e)}")
