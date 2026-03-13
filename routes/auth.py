from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import requests, time
import logging
from .store_utils import load_stores, save_stores, get_active_store_key, set_active_store_key

logger = logging.getLogger(__name__)
router = APIRouter()


def generate_access_token(shop: str, api_key: str, api_secret: str, api_version: str = "2026-01"):
    """Generate access token using client_credentials OAuth flow"""
    try:
        url = f"https://{shop}/admin/oauth/access_token"
        payload = {
            "client_id": api_key,
            "client_secret": api_secret,
            "grant_type": "client_credentials",
            "scope": "write_products,read_products,write_orders,read_orders,write_inventory,read_inventory,write_collections,read_collections"
        }
        logger.info(f"Requesting token from {url} with client_id: {api_key[:10]}...")
        
        r = requests.post(url, data=payload, timeout=10)
        logger.info(f"Shopify response: {r.status_code}")
        
        if r.status_code == 200:
            data = r.json()
            logger.info(f"✅ Token generated successfully for {shop}")
            return {
                "access_token": data.get("access_token"),
                "expires_in": data.get("expires_in", 3600),
                "token_type": data.get("token_type", "Bearer"),
            }
        else:
            error_text = r.text
            logger.error(f"❌ Shopify OAuth error: {r.status_code} - {error_text}")
            raise Exception(f"Shopify returned {r.status_code}: {error_text}")
    except requests.exceptions.Timeout:
        logger.error(f"❌ Timeout connecting to Shopify")
        raise Exception("Shopify connection timeout - check your internet and credentials")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error: {e}")
        raise Exception(f"Network error connecting to Shopify: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Error generating token: {e}")
        raise


# ── Models ────────────────────────────────────────────────────────────────────
class ConnectRequest(BaseModel):
    shop_name: str        # e.g. "mystore" or "mystore.myshopify.com"
    api_key: str
    api_secret: str
    api_version: str = "2026-01"


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/api/auth/connect")
async def connect_store(req: ConnectRequest):
    """
    Accept store credentials, generate access token, and save them locally.
    Uses Shopify's client_credentials OAuth flow to generate a valid access token.
    """
    logger.info(f"🔗 Connect request received for shop: {req.shop_name}")
    
    # Normalize shop name
    shop = req.shop_name.strip().lower()
    if not shop.endswith(".myshopify.com"):
        shop = f"{shop}.myshopify.com"

    shop_key = shop.replace(".myshopify.com", "")

    # Generate access token using OAuth client_credentials flow
    try:
        logger.info(f"Attempting to generate token for {shop}...")
        token_data = generate_access_token(shop, req.api_key, req.api_secret, req.api_version)
        access_token = token_data["access_token"]
        expires_in = token_data["expires_in"]
        logger.info(f"✅ Token generated, expires in {expires_in}s")
    except Exception as e:
        logger.error(f"❌ Token generation failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid credentials: {str(e)}")
    
    # Fetch actual shop info using the generated token
    shop_info = {"name": shop_key}
    try:
        logger.info(f"Fetching shop info from Shopify...")
        verify = requests.get(
            f"https://{shop}/admin/api/{req.api_version}/shop.json",
            headers={"X-Shopify-Access-Token": access_token},
            timeout=10
        )
        if verify.status_code == 200:
            shop_info = verify.json().get("shop", {})
            logger.info(f"✅ Shop info retrieved: {shop_info.get('name')}")
        else:
            logger.warning(f"Could not get shop info: {verify.status_code}")
    except Exception as e:
        logger.warning(f"Could not fetch shop info: {e}")
    
    # Save to stores.json with all credentials for token refresh
    stores = load_stores()
    stores[shop_key] = {
        "shop": shop,
        "shop_name": shop_info.get("name", shop_key),
        "api_key": req.api_key,
        "api_secret": req.api_secret,
        "access_token": access_token,
        "token_expires_at": time.time() + expires_in,
        "api_version": req.api_version,
        "connected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    save_stores(stores)
    
    # Set this as the active store automatically
    set_active_store_key(shop_key)
    logger.info(f"✅ Store {shop_key} saved and set as active store")

    return {
        "success": True,
        "shop": shop,
        "shop_name": shop_info.get("name", shop_key),
        "shop_key": shop_key,
        "token_preview": access_token[:20] + "..." if len(access_token) > 20 else access_token,
        "api_version": req.api_version,
        "message": f"Successfully connected to {shop}! Token will auto-refresh."
    }


@router.get("/api/auth/stores")
async def list_stores():
    """List all connected stores (without exposing tokens)."""
    stores = load_stores()
    active_key = get_active_store_key()
    safe = []
    for key, s in stores.items():
        safe.append({
            "shop_key": key,
            "shop": s.get("shop"),
            "shop_name": s.get("shop_name"),
            "api_version": s.get("api_version"),
            "connected_at": s.get("connected_at"),
            "is_active": key == active_key,
        })
    return {"stores": safe, "count": len(safe), "active_store": active_key}


@router.get("/api/auth/active-store")
async def get_active_store():
    """Get the currently active store."""
    active_key = get_active_store_key()
    if not active_key:
        raise HTTPException(status_code=404, detail="No store connected")
    stores = load_stores()
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


@router.post("/api/auth/active-store/{shop_key}")
async def set_active_store(shop_key: str):
    """Switch the active store."""
    stores = load_stores()
    if shop_key not in stores:
        raise HTTPException(status_code=404, detail="Store not found")
    set_active_store_key(shop_key)
    s = stores[shop_key]
    logger.info(f"✅ Active store switched to: {shop_key}")
    return {
        "success": True,
        "active_store": shop_key,
        "shop_name": s.get("shop_name"),
        "message": f"Switched to {s.get('shop_name', shop_key)}"
    }


@router.delete("/api/auth/stores/{shop_key}")
async def disconnect_store(shop_key: str):
    """Disconnect/remove a store."""
    stores = load_stores()
    if shop_key not in stores:
        raise HTTPException(status_code=404, detail="Store not found")
    del stores[shop_key]
    save_stores(stores)
    # If this was active, switch to another store if available
    active_key = get_active_store_key()
    if active_key == shop_key:
        remaining = list(stores.keys())
        set_active_store_key(remaining[0] if remaining else None)
    return {"success": True, "message": f"Store {shop_key} disconnected"}


@router.get("/api/auth/stores/{shop_key}/token")
async def get_store_token(shop_key: str):
    """Get token for a store (internal use only)."""
    stores = load_stores()
    if shop_key not in stores:
        raise HTTPException(status_code=404, detail="Store not found")
    s = stores[shop_key]
    return {
        "access_token": s.get("access_token"),
        "shop": s.get("shop"),
        "api_version": s.get("api_version"),
    }
