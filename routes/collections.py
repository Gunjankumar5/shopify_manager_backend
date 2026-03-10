from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from shopify_client import ShopifyClient
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

shopify = ShopifyClient()


def _normalize_title(value: str) -> str:
    return (value or "").strip().lower()

@router.get("/")
async def list_collections(limit: int = Query(50, le=250)):
    """List all collections"""
    try:
        result = shopify.get_collections(limit=limit)
        return result
    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{collection_id}")
async def get_collection(collection_id: int):
    """Get a specific collection"""
    try:
        result = shopify.get_collection(collection_id)
        return result
    except Exception as e:
        logger.error(f"Error fetching collection {collection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/")
async def create_collection(collection_data: dict):
    """Create a new collection
    
    Expected fields:
    - title (required)
    - body_html (optional)
    - image (optional)
    - published_at (optional)
    - published_scope (optional)
    """
    try:
        if "title" not in collection_data:
            raise ValueError("Collection title is required")

        title_key = _normalize_title(collection_data.get("title"))
        existing = shopify.get_collections(limit=250).get("custom_collections", [])
        if any(_normalize_title(item.get("title")) == title_key for item in existing):
            raise HTTPException(status_code=409, detail="Collection with this title already exists")
        
        result = shopify.create_collection(collection_data)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{collection_id}")
async def update_collection(collection_id: int, collection_data: dict):
    """Update an existing collection"""
    try:
        if "title" in collection_data:
            title_key = _normalize_title(collection_data.get("title"))
            existing = shopify.get_collections(limit=250).get("custom_collections", [])
            duplicate = any(
                _normalize_title(item.get("title")) == title_key and int(item.get("id", 0)) != collection_id
                for item in existing
            )
            if duplicate:
                raise HTTPException(status_code=409, detail="Collection with this title already exists")

        result = shopify.update_collection(collection_id, collection_data)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating collection {collection_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{collection_id}")
async def delete_collection(collection_id: int):
    """Delete a custom collection"""
    try:
        result = shopify.delete_collection(collection_id)
        return result
    except Exception as e:
        logger.error(f"Error deleting collection {collection_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{collection_id}/products")
async def add_products_to_collection(collection_id: int, product_ids: List[int]):
    """Add products to a collection"""
    try:
        result = shopify.add_products_to_collection(collection_id, product_ids)
        return result
    except Exception as e:
        logger.error(f"Error adding products to collection: {e}")
        raise HTTPException(status_code=400, detail=str(e))
