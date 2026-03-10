from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
load_dotenv()
from routes import products, collections, inventory, upload
load_dotenv()

app = FastAPI(
    title="Shopify Product Manager API",
    description="Backend API for Shopify product, collection, and inventory management",
    version="1.0.0",
    redirect_slashes=False
)

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(collections.router, prefix="/api/collections", tags=["Collections"])
app.include_router(inventory.router, prefix="/api/inventory", tags=["Inventory"])
app.include_router(upload.router, prefix="/api/upload", tags=["Upload"])

@app.get("/")
async def root():
    return {
        "message": "Shopify Product Manager API",
        "docs": "/docs",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
