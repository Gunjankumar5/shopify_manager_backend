from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from shopify_client import ShopifyClient
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
shopify = ShopifyClient()


# ── IMPORTANT: all fixed-path routes MUST come before /{product_id} ──────────

@router.get("")
async def list_products(
    limit: Optional[int] = Query(None),
    status: str = Query("any"),
    search: Optional[str] = None
):
    try:
        if search:
            products = shopify.search_products(search)
            return {"products": products, "count": len(products)}
        else:
            result = shopify.get_products(limit=limit, status=status, fetch_all=True)
            return result
    except Exception as e:
        logger.error(f"Error listing products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync")
async def sync_products():
    try:
        # Get actual count from Shopify
        count_result = shopify.count_products()
        actual_count = count_result.get("count", 0)
        
        # Fetch all products
        result = shopify.get_products(fetch_all=True)
        products = result.get("products", [])
        
        return {
            "message": f"Successfully synced {len(products)} products from Shopify",
            "synced_count": len(products),
            "actual_store_count": actual_count,
            "difference": actual_count - len(products),
            "products": products,
            "count": len(products)
        }
    except Exception as e:
        logger.error(f"Error syncing products: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to sync: {str(e)}")


@router.get("/find-duplicates")
async def find_duplicate_products():
    """Find duplicate products without deleting them."""
    try:
        result = shopify.get_products(fetch_all=True)
        products = result.get("products", [])
        seen_titles = {}
        duplicates = []
        for product in products:
            title = product.get("title", "").lower().strip()
            pid = product.get("id")
            if not title:
                continue
            if title in seen_titles:
                duplicates.append({
                    "id": pid,
                    "title": product.get("title"),
                    "status": product.get("status"),
                    "created_at": product.get("created_at"),
                    "duplicate_of_id": seen_titles[title]
                })
            else:
                seen_titles[title] = pid
        return {
            "total_scanned": len(products),
            "duplicates_found": len(duplicates),
            "duplicates": duplicates
        }
    except Exception as e:
        logger.error(f"Error finding duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remove-duplicates")
async def remove_duplicate_products():
    """Find and delete duplicate products. Keeps first occurrence, deletes the rest."""
    try:
        result = shopify.get_products(fetch_all=True)
        products = result.get("products", [])
        seen_titles = {}
        duplicates = []
        deleted = []
        errors = []

        for product in products:
            title = product.get("title", "").lower().strip()
            pid = product.get("id")
            if not title:
                continue
            if title in seen_titles:
                duplicates.append({"id": pid, "title": product.get("title"), "duplicate_of": seen_titles[title]})
            else:
                seen_titles[title] = pid

        for dup in duplicates:
            try:
                shopify.delete_product(dup["id"])
                deleted.append(dup)
                logger.info(f"Deleted duplicate: {dup['title']} (id={dup['id']})")
            except Exception as e:
                errors.append({"title": dup["title"], "error": str(e)})

        return {
            "total_scanned": len(products),
            "duplicates_found": len(duplicates),
            "deleted": len(deleted),
            "failed": len(errors),
            "deleted_products": deleted,
            "errors": errors
        }
    except Exception as e:
        logger.error(f"Error removing duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-create")
async def bulk_create_products(products: List[dict]):
    try:
        results, errors = [], []
        for idx, product_data in enumerate(products):
            try:
                results.append(shopify.create_product(product_data))
            except Exception as e:
                errors.append({"index": idx, "error": str(e), "title": product_data.get("title")})
        return {"created": len(results), "failed": len(errors), "results": results, "errors": errors}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bulk-update")
async def bulk_update_products(updates: List[dict]):
    try:
        results, errors = [], []
        for update in updates:
            product_id = update.pop("id", None)
            if not product_id:
                errors.append({"error": "Missing product id"})
                continue
            try:
                results.append(shopify.update_product(product_id, update))
            except Exception as e:
                errors.append({"id": product_id, "error": str(e)})
        return {"updated": len(results), "failed": len(errors), "results": results, "errors": errors}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Routes with path params MUST come AFTER all fixed-path routes ─────────────

@router.get("/{product_id}")
async def get_product(product_id: int):
    try:
        return shopify.get_product(product_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_product(product_data: dict):
    try:
        if "title" not in product_data:
            raise ValueError("Product title is required")
        return shopify.create_product(product_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}")
async def update_product(product_id: int, product_data: dict):
    try:
        return shopify.update_product(product_id, product_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{product_id}")
async def delete_product(product_id: int):
    try:
        shopify.delete_product(product_id)
        return {"message": f"Product {product_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}/variants/{variant_id}")
async def update_variant(product_id: int, variant_id: int, variant_data: dict):
    try:
        return shopify.update_product_variant(product_id, variant_id, variant_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))