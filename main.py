from fastapi import FastAPI
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pathlib import Path
import asyncio, os, time, logging

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

from routes import products, collections, inventory, upload, export, auth, metafields, users
from routes.store_utils import get_connected_store, load_all_user_stores, save_stores, set_request_user_id
from routes.auth_utils import resolve_user_id_from_request
from routes.user_utils import initialize_admin_user
import requests

logger = logging.getLogger(__name__)


class RequestUserContextMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        set_request_user_id(None)
        request = Request(scope)
        user_id = await resolve_user_id_from_request(request, required=False)
        if user_id:
            set_request_user_id(user_id)

        try:
            await self.app(scope, receive, send)
        finally:
            set_request_user_id(None)

# ── Background token refresher (multi-store aware) ────────────────────────────
async def token_refresh_loop():
    """Check all connected stores every 30 min and refresh expiring tokens."""
    while True:
        try:
            await asyncio.sleep(1800)
            all_user_stores = load_all_user_stores()
            for user_id, stores in all_user_stores.items():
                if not stores:
                    continue
                updated = False
                for shop_key, store in stores.items():
                    expires_at = store.get("token_expires_at", 0)
                    if expires_at and time.time() > expires_at - 600:
                        logger.info(f"🔄 Refreshing token for {shop_key} (user {user_id})...")
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
                                store["access_token"] = data.get("access_token")
                                store["token_expires_at"] = time.time() + data.get("expires_in", 3600)
                                stores[shop_key] = store
                                updated = True
                                logger.info(f"✅ Token refreshed for {shop_key}")
                            else:
                                logger.warning(f"⚠️ Refresh failed for {shop_key}: {r.status_code}")
                        except Exception as e:
                            logger.warning(f"⚠️ Refresh error for {shop_key}: {e}")

                if updated and user_id != "__legacy__":
                    save_stores(stores, user_id=user_id)
        except Exception as e:
            logger.error(f"Token refresh loop error: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize admin user if none exists (for testing/demo)
    try:
        from routes.user_utils import load_users
        if not load_users():
            logger.info("🔧 Initializing default admin user for demo/testing...")
            initialize_admin_user(
                admin_user_id="demo_admin_user",
                email="admin@shopmanager.local",
                full_name="Admin User"
            )
            logger.info("✅ Demo admin user created (user_id: demo_admin_user)")
    except Exception as e:
        logger.warning(f"Could not initialize admin user: {e}")

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

# Compress responses to reduce bandwidth for large JSON payloads
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://shopify-management-frontend-dev.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RequestUserContextMiddleware)

# ── Routes — prefix added HERE, not inside the router files ──────────────────
app.include_router(auth.router,        prefix="/api/auth",        tags=["Auth"])
app.include_router(users.router,       prefix="/api/users",       tags=["Users"])
app.include_router(products.router,    prefix="/api/products",    tags=["Products"])
app.include_router(collections.router, prefix="/api/collections", tags=["Collections"])
app.include_router(inventory.router,   prefix="/api/inventory",   tags=["Inventory"])
app.include_router(upload.router,      prefix="/api/upload",      tags=["Upload"])
app.include_router(export.router,      prefix="/api/export",      tags=["Export"])
app.include_router(metafields.router, prefix="/api/metafields", tags=["Metafields"])


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
