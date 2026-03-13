from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import asyncio, os, time

load_dotenv()

from routes import products, collections, inventory, upload, export, automation, auth
from shopify_client import _refresh_token, _cache

# ── Background token refresher ────────────────────────────────────────────────
async def token_refresh_loop():
    """Automatically refreshes Shopify token 5 minutes before expiry."""
    while True:
        try:
            wait = max(60, _cache["expires_at"] - time.time() - 300)
            print(f"🔄 Next token refresh in {int(wait/60)} minutes")
            await asyncio.sleep(wait)
            _refresh_token()
            print("✅ Token auto-refreshed successfully")
        except Exception as e:
            print(f"⚠️ Token refresh error: {e}")
            await asyncio.sleep(60)  # retry in 1 min if error

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background token refresh on startup
    task = asyncio.create_task(token_refresh_loop())
    print("🚀 Token auto-refresh background task started")
    yield
    task.cancel()  # cleanup on shutdown

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Shopify Product Manager API",
    description="Backend API for managing Shopify products, collections, and inventory",
    version="1.0.0",
    redirect_slashes=False,
    lifespan=lifespan   # ← attach background task
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, tags=["Auth"])
app.include_router(products.router,    prefix="/api/products",    tags=["Products"])
app.include_router(collections.router, prefix="/api/collections", tags=["Collections"])
app.include_router(inventory.router,   prefix="/api/inventory",   tags=["Inventory"])
app.include_router(upload.router,      prefix="/api/upload",      tags=["Upload"])
app.include_router(export.router,      prefix="/api/export",      tags=["Export"])
app.include_router(automation.router,  prefix="/api/automation",  tags=["Automation"])

@app.get("/")
async def root():
    return {"message": "Shopify Product Manager API", "docs": "/docs", "version": "1.0.0"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", os.getenv("BACKEND_PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)
