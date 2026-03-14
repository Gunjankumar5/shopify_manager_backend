from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import asyncio, os, time, logging

load_dotenv()

from routes import products, collections, inventory, upload, export, auth
from routes.store_utils import get_connected_store, load_stores, save_stores
import requests

logger = logging.getLogger(__name__)

# ── Background token refresher (multi-store aware) ────────────────────────────
async def token_refresh_loop():
    """Check all connected stores every 30 min and refresh expiring tokens."""
    while True:
        try:
            await asyncio.sleep(1800)
            stores = load_stores()
            for shop_key, store in stores.items():
                expires_at = store.get("token_expires_at", 0)
                if expires_at and time.time() > expires_at - 600:
                    logger.info(f"🔄 Refreshing token for {shop_key}...")
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
                            stores[shop_key] = store
                            save_stores(stores)
                            logger.info(f"✅ Token refreshed for {shop_key}")
                        else:
                            logger.warning(f"⚠️ Refresh failed for {shop_key}: {r.status_code}")
                    except Exception as e:
                        logger.warning(f"⚠️ Refresh error for {shop_key}: {e}")
        except Exception as e:
            logger.error(f"Token refresh loop error: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(token_refresh_loop())
    logger.info("🚀 Multi-store token auto-refresh started")
    yield
    task.cancel()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Shopify Product Manager API",
    description="Backend API for managing Shopify products, collections, and inventory",
    version="1.0.0",
    redirect_slashes=False,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes — prefix added HERE, not inside the router files ──────────────────
app.include_router(auth.router,        prefix="/api/auth",        tags=["Auth"])
app.include_router(products.router,    prefix="/api/products",    tags=["Products"])
app.include_router(collections.router, prefix="/api/collections", tags=["Collections"])
app.include_router(inventory.router,   prefix="/api/inventory",   tags=["Inventory"])
app.include_router(upload.router,      prefix="/api/upload",      tags=["Upload"])
app.include_router(export.router,      prefix="/api/export",      tags=["Export"])


@app.get("/")
async def root():
    return {"message": "Shopify Product Manager API", "docs": "/docs", "version": "1.0.0"}


@app.get("/health")
async def health_check():
    try:
        store = get_connected_store()
        return {
            "status": "healthy",
            "store_connected": store is not None,
            "active_store": store.get("shop_name") if store else None,
        }
    except Exception:
        return {"status": "healthy", "store_connected": False}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", os.getenv("BACKEND_PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)
