from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional, List
import logging, json
from .store_utils import get_shopify_client

logger = logging.getLogger(__name__)
router = APIRouter()
<<<<<<< HEAD
shopify = ShopifyClient()
=======
>>>>>>> fbca71b (eat(connect-store): add Shopify store connection feature)


# ── IMPORTANT: all fixed-path routes MUST come before /{product_id} ──────────

@router.get("")
async def list_products(
    limit: Optional[int] = Query(None),
    status: str = Query("any"),
    search: Optional[str] = None,
    fetch_all: bool = Query(False)
):
    try:
        logger.info(f"[Products] GET request - limit: {limit}, status: {status}, search: {search}, fetch_all: {fetch_all}")
        shopify = get_shopify_client()
        result = shopify.get_products(limit=limit, status=status, title=search, fetch_all=fetch_all)
        products = result.get("products", [])
        logger.info(f"[Products] ✅ Got {len(products)} products")
        return {"products": products, "count": len(products)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Products] ❌ Error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch products: {str(e)}")


@router.get("/count")
async def count_products(status: str = Query("any")):
    """Return the total product count from Shopify (fast, no product data)."""
    try:
        shopify = get_shopify_client()
        result = shopify.count_products()
        return {"count": result.get("count", 0)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
async def fetch_all_products(status: str = Query("any")):
    """Stream all products page-by-page as NDJSON so the frontend can show progress.

    Each line is a JSON object:
      {"type":"page","page":1,"products":[...]}   ← one line per Shopify page
      {"type":"done","total":NNN}                  ← final line
    """
    shopify = get_shopify_client()

    import requests as _req
    import time as _time

    def generate():
        page_size = 250
        params = {
            "limit": page_size,
            "fields": "id,title,vendor,status,handle,image,images,variants",
        }
        if status and status != "any":
            params["status"] = status

        next_url = f"{shopify._get_base_url()}/products.json"
        page_num = 0
        total = 0

        while next_url:
            page_num += 1
            # Small delay between pages to respect Shopify's rate limit (2 req/s)
            if page_num > 1:
                _time.sleep(0.6)
            try:
                retries = 0
                while True:
                    r = _req.get(
                        next_url,
                        headers=shopify._get_headers(),
                        params=params if page_num == 1 else None,
                        timeout=25,
                    )
                    if r.status_code == 429 and retries < 5:
                        wait = float(r.headers.get("Retry-After", 2))
                        _time.sleep(wait)
                        retries += 1
                        continue
                    r.raise_for_status()
                    break
            except Exception as e:
                yield json.dumps({"type": "error", "message": str(e)}) + "\n"
                return

            batch = r.json().get("products", [])
            total += len(batch)
            yield json.dumps({"type": "page", "page": page_num, "products": batch}) + "\n"

            next_url = r.links.get("next", {}).get("url")

        yield json.dumps({"type": "done", "total": total}) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@router.get("/sync")
async def sync_products():
    try:
        shopify = get_shopify_client()
        
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error syncing products: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to sync: {str(e)}")


@router.get("/find-duplicates")
async def find_duplicate_products():
    """Find duplicate products without deleting them."""
    try:
        shopify = get_shopify_client()
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
        shopify = get_shopify_client()
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-create")
async def bulk_create_products(products: List[dict]):
    try:
        shopify = get_shopify_client()
        results, errors = [], []
        for idx, product_data in enumerate(products):
            try:
                results.append(shopify.create_product(product_data))
            except Exception as e:
                errors.append({"index": idx, "error": str(e), "title": product_data.get("title")})
        return {"created": len(results), "failed": len(errors), "results": results, "errors": errors}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bulk-update")
async def bulk_update_products(updates: List[dict]):
    try:
        shopify = get_shopify_client()
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Routes with path params MUST come AFTER all fixed-path routes ─────────────

@router.get("/{product_id}")
async def get_product(product_id: int):
    try:
        shopify = get_shopify_client()
        return shopify.get_product(product_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_product(product_data: dict):
    try:
        if "title" not in product_data:
            raise ValueError("Product title is required")
        shopify = get_shopify_client()
        return shopify.create_product(product_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}")
async def update_product(product_id: int, product_data: dict):
    try:
        shopify = get_shopify_client()
        return shopify.update_product(product_id, product_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{product_id}")
async def delete_product(product_id: int):
    try:
        shopify = get_shopify_client()
        shopify.delete_product(product_id)
        return {"message": f"Product {product_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}/variants/{variant_id}")
async def update_variant(product_id: int, variant_id: int, variant_data: dict):
    try:
        shopify = get_shopify_client()
        return shopify.update_product_variant(product_id, variant_id, variant_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))