from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import requests
import time
import logging
from .store_utils import load_stores, save_stores, get_active_store_key, set_active_store_key
from .auth_utils import require_authenticated_user

logger = logging.getLogger(__name__)
router = APIRouter()


def _mask_secret(value: str) -> str:
    token = (value or "").strip()
    if not token:
        return ""
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}...{token[-4:]}"


def generate_access_token(shop: str, api_key: str, api_secret: str, api_version: str = "2026-01"):
    """Try multiple auth methods to connect to Shopify.

    Tries: 1) OAuth client_credentials 2) api_secret as direct access token
    3) basic auth fallback. Returns dict {access_token, expires_in} on success.
    """
    # Method 1: OAuth client_credentials
    try:
        r = requests.post(
            f"https://{shop}/admin/oauth/access_token",
            data={
                "client_id": api_key,
                "client_secret": api_secret,
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            token = data.get("access_token")
            if token:
                logger.info(f"✅ Token via OAuth client_credentials for {shop}")
                return {"access_token": token, "expires_in": data.get("expires_in", 3600)}
        logger.warning(f"OAuth failed: {r.status_code} - {r.text[:100]}")
    except Exception as e:
        logger.warning(f"OAuth method failed: {e}")

    # Method 2: api_secret IS the access token (private/custom apps — never expires)
    try:
        test = requests.get(
            f"https://{shop}/admin/api/{api_version}/shop.json",
            headers={"X-Shopify-Access-Token": api_secret},
            timeout=10,
        )
        if test.status_code == 200:
            logger.info(f"✅ Token via direct access token for {shop}")
            return {"access_token": api_secret, "expires_in": 86400 * 365}
        logger.warning(f"Direct token failed: {test.status_code}")
    except Exception as e:
        logger.warning(f"Direct token method failed: {e}")

    # Method 3: basic auth fallback
    try:
        test2 = requests.get(
            f"https://{shop}/admin/api/{api_version}/shop.json",
            auth=(api_key, api_secret),
            timeout=10,
        )
        if test2.status_code == 200:
            logger.info(f"✅ Token via basic auth for {shop}")
            return {"access_token": api_secret, "expires_in": 86400 * 365}
        logger.warning(f"Basic auth failed: {test2.status_code}")
    except Exception as e:
        logger.warning(f"Basic auth method failed: {e}")

    raise HTTPException(
        status_code=401,
        detail="Could not authenticate with Shopify. Please check your API Key and Secret/Access Token."
    )


# ── Models ────────────────────────────────────────────────────────────────────
class ConnectRequest(BaseModel):
    shop_name: str
    api_key: str
    api_secret: str
    api_version: str = "2026-01"


# ── Routes — NO /api/auth prefix here; main.py adds prefix="/api/auth" ────────

@router.post("/connect")
def connect_store(req: ConnectRequest, user_id: str = Depends(require_authenticated_user)):
    logger.info(f"🔗 Connect request for: {req.shop_name}")

    shop = req.shop_name.strip().lower()
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"
    shop_key = shop.replace(".myshopify.com", "")

    token_data = generate_access_token(shop, req.api_key, req.api_secret, req.api_version)
    access_token = token_data["access_token"]
    expires_in   = token_data["expires_in"]

    # Fetch shop info
    shop_info = {"name": shop_key}
    try:
        verify = requests.get(
            f"https://{shop}/admin/api/{req.api_version}/shop.json",
            headers={"X-Shopify-Access-Token": access_token},
            timeout=10,
        )
        if verify.status_code == 200:
            shop_info = verify.json().get("shop", {})
            logger.info(f"✅ Shop info: {shop_info.get('name')}")
    except Exception as e:
        logger.warning(f"Could not fetch shop info: {e}")

    # Save to stores.json
    stores = load_stores(user_id=user_id)
    stores[shop_key] = {
        "shop": shop,
        "shop_name": shop_info.get("name", shop_key),
        "api_key": req.api_key,
        "api_secret": req.api_secret,
        "access_token": access_token,
        "token_expires_at": time.time() + expires_in,
        "api_version": req.api_version,
        "connected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "email": shop_info.get("email", ""),
        "currency": shop_info.get("currency", ""),
    }
    save_stores(stores, user_id=user_id)
    set_active_store_key(shop_key, user_id=user_id)
    logger.info(f"✅ Store {shop_key} saved and set as active")

    return {
        "success": True,
        "shop": shop,
        "shop_name": shop_info.get("name", shop_key),
        "shop_key": shop_key,
        "token_preview": _mask_secret(access_token),
        "api_version": req.api_version,
        "message": f"Successfully connected to {shop_info.get('name', shop)}!",
    }


@router.get("/stores")
def list_stores(user_id: str = Depends(require_authenticated_user)):
    logger.info(f"[GET /auth/stores] Request from user: {user_id}")
    try:
        stores = load_stores(user_id=user_id)
        active_key = get_active_store_key(user_id=user_id)
        logger.info(f"[GET /auth/stores] Returning {len(stores)} stores for user {user_id}")
        return {
            "stores": [
                {
                    "shop_key": k,
                    "shop": s.get("shop"),
                    "shop_name": s.get("shop_name"),
                    "api_version": s.get("api_version"),
                    "connected_at": s.get("connected_at"),
                    "email": s.get("email", ""),
                    "currency": s.get("currency", ""),
                    "is_active": k == active_key,
                }
                for k, s in stores.items()
            ],
            "count": len(stores),
            "active_store": active_key,
        }
    except Exception as e:
        logger.error(f"[GET /auth/stores] Error: {e}")
        raise


@router.get("/active-store")
def get_active_store(user_id: str = Depends(require_authenticated_user)):
    active_key = get_active_store_key(user_id=user_id)
    if not active_key:
        raise HTTPException(status_code=404, detail="No store connected")
    stores = load_stores(user_id=user_id)
    if active_key not in stores:
        raise HTTPException(status_code=404, detail="Active store not found")
    s = stores[active_key]
    return {
        "shop_key": active_key,
        "shop": s.get("shop"),
        "shop_name": s.get("shop_name"),
        "api_version": s.get("api_version"),
        "connected_at": s.get("connected_at"),
    }


@router.post("/active-store/{shop_key}")
def set_active_store(shop_key: str, user_id: str = Depends(require_authenticated_user)):
    stores = load_stores(user_id=user_id)
    if shop_key not in stores:
        raise HTTPException(status_code=404, detail="Store not found")
    set_active_store_key(shop_key, user_id=user_id)
    s = stores[shop_key]
    return {
        "success": True,
        "active_store": shop_key,
        "shop_name": s.get("shop_name"),
        "message": f"Switched to {s.get('shop_name', shop_key)}",
    }


@router.delete("/stores/{shop_key}")
def disconnect_store(shop_key: str, user_id: str = Depends(require_authenticated_user)):
    stores = load_stores(user_id=user_id)
    if shop_key not in stores:
        raise HTTPException(status_code=404, detail="Store not found")
    del stores[shop_key]
    save_stores(stores, user_id=user_id)
    active_key = get_active_store_key(user_id=user_id)
    if active_key == shop_key:
        remaining = list(stores.keys())
        set_active_store_key(remaining[0] if remaining else None, user_id=user_id)
    return {"success": True, "message": f"Store {shop_key} disconnected"}
