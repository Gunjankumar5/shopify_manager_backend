from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional, List, Any, Dict
import logging, json
from .store_utils import get_shopify_client

logger = logging.getLogger(__name__)
router = APIRouter()


def _as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        t = v.strip().lower()
        if t in {"true", "1", "yes", "on"}:
            return True
        if t in {"false", "0", "no", "off"}:
            return False
    if isinstance(v, (int, float)):
        return bool(v)
    return None


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _clean_number_str(v: Any) -> Optional[str]:
    if v in (None, ""):
        return None
    try:
        return str(float(v)).rstrip("0").rstrip(".") if "." in str(float(v)) else str(int(float(v)))
    except Exception:
        s = _clean_str(v)
        return s


def _normalize_collection_ids(value: Any) -> List[int]:
    raw_values = value if isinstance(value, list) else [value]
    collection_ids: List[int] = []
    for item in raw_values:
        if item in (None, ""):
            continue
        if isinstance(item, dict):
            item = item.get("id") or item.get("value")
        try:
            collection_ids.append(int(item))
        except Exception:
            continue
    return sorted(set(collection_ids))


def _normalize_images(raw_images: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_images, list):
        return []
    images: List[Dict[str, Any]] = []
    for item in raw_images:
        if isinstance(item, str):
            src = _clean_str(item)
            if src:
                images.append({"src": src})
            continue
        if not isinstance(item, dict):
            continue

        img: Dict[str, Any] = {}
        src = _clean_str(item.get("src"))
        attachment = _clean_str(item.get("attachment"))
        filename = _clean_str(item.get("filename"))

        if src:
            img["src"] = src
        if attachment:
            img["attachment"] = attachment
        if filename:
            img["filename"] = filename

        if img:
            images.append(img)

    return images


def _normalize_variants(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_variants = payload.get("variants")
    variants_in = raw_variants if isinstance(raw_variants, list) else []

    top_price = _clean_str(payload.get("price"))
    top_compare = _clean_str(payload.get("comparePrice") or payload.get("compare_at_price"))
    top_sku = _clean_str(payload.get("sku"))
    top_barcode = _clean_str(payload.get("barcode"))
    top_qty = payload.get("qty", payload.get("inventory_quantity"))
    top_weight = payload.get("weight")
    top_track = _as_bool(payload.get("trackQty", payload.get("track_quantity")))
    top_physical = _as_bool(payload.get("isPhysical", payload.get("requires_shipping")))

    if not variants_in and any([top_price, top_sku, top_barcode, top_compare, top_qty is not None]):
        variants_in = [{}]

    allowed_variant_keys = {
        "id",
        "option1",
        "option2",
        "option3",
        "price",
        "compare_at_price",
        "sku",
        "barcode",
        "taxable",
        "requires_shipping",
        "inventory_management",
        "inventory_policy",
        "inventory_quantity",
        "weight",
        "weight_unit",
        "grams",
    }

    normalized: List[Dict[str, Any]] = []
    for idx, raw in enumerate(variants_in):
        if not isinstance(raw, dict):
            continue

        v: Dict[str, Any] = {}
        for k in allowed_variant_keys:
            if k in raw and raw[k] is not None and raw[k] != "":
                v[k] = raw[k]

        if "option1" not in v:
            name = _clean_str(raw.get("option1") or raw.get("name") or raw.get("title"))
            v["option1"] = name or "Default Title"

        price = _clean_str(raw.get("price")) or (top_price if idx == 0 else None)
        if price:
            v["price"] = price

        compare_price = _clean_str(raw.get("compare_at_price") or raw.get("comparePrice"))
        if compare_price is None and idx == 0:
            compare_price = top_compare
        if compare_price:
            v["compare_at_price"] = compare_price

        sku = _clean_str(raw.get("sku"))
        if sku is None and idx == 0:
            sku = top_sku
        if sku:
            v["sku"] = sku

        barcode = _clean_str(raw.get("barcode"))
        if barcode is None and idx == 0:
            barcode = top_barcode
        if barcode:
            v["barcode"] = barcode

        track = _as_bool(raw.get("trackQty"))
        if track is None:
            track = _as_bool(raw.get("inventory_management"))
        if track is None and idx == 0:
            track = top_track
        if track is True and "inventory_management" not in v:
            v["inventory_management"] = "shopify"

        qty = raw.get("inventory_quantity")
        if qty is None and idx == 0:
            qty = top_qty
        if qty not in (None, ""):
            try:
                v["inventory_quantity"] = int(float(qty))
            except Exception:
                pass

        req_shipping = _as_bool(raw.get("requires_shipping"))
        if req_shipping is None and idx == 0:
            req_shipping = top_physical
        if req_shipping is not None:
            v["requires_shipping"] = req_shipping

        weight = raw.get("weight")
        if weight in (None, "") and idx == 0:
            weight = top_weight
        if weight not in (None, ""):
            try:
                v["weight"] = float(weight)
                v.setdefault("weight_unit", "kg")
            except Exception:
                pass

        normalized.append(v)

    return normalized


def normalize_product_payload(product_data: Dict[str, Any], is_update: bool = False) -> Dict[str, Any]:
    payload = product_data if isinstance(product_data, dict) else {}
    clean: Dict[str, Any] = {}

    # Accept common direct Shopify fields
    allowed_product_keys = {
        "title",
        "body_html",
        "vendor",
        "product_type",
        "handle",
        "status",
        "published",
        "published_scope",
        "tags",
        "template_suffix",
    }
    for k in allowed_product_keys:
        if k in payload and payload[k] is not None:
            clean[k] = payload[k]

    # Accept AddProductPage aliases
    if "description" in payload and "body_html" not in clean:
        clean["body_html"] = payload.get("description")
    if "productType" in payload and "product_type" not in clean:
        clean["product_type"] = payload.get("productType")
    if "seo" in payload and isinstance(payload.get("seo"), dict):
        seo = payload["seo"]
        if "handle" in seo and "handle" not in clean:
            clean["handle"] = seo.get("handle")

    # Clean string-ish fields
    for k in ["title", "vendor", "product_type", "handle", "body_html", "template_suffix"]:
        if k in clean:
            val = _clean_str(clean[k])
            if val is None:
                clean.pop(k, None)
            else:
                clean[k] = val

    tags = clean.get("tags")
    if isinstance(tags, list):
        tags = ", ".join([str(t).strip() for t in tags if str(t).strip()])
    if tags is not None:
        tags = _clean_str(tags)
        if tags:
            clean["tags"] = tags
        else:
            clean.pop("tags", None)

    images = _normalize_images(payload.get("images"))
    if images:
        clean["images"] = images

    variants = _normalize_variants(payload)
    if variants:
        clean["variants"] = variants

    # Tolerate extra AddProductPage-only fields by accepting and dropping them silently here.
    _ = payload.get("costPerItem")
    _ = payload.get("collections")
    _ = payload.get("seo")
    _ = payload.get("chargeTax")

    if is_update:
        clean.pop("title", None) if clean.get("title") == "" else None

    return clean


def _extract_variant_cost(payload: Dict[str, Any]) -> Optional[str]:
    top_level_cost = _clean_number_str(payload.get("costPerItem") or payload.get("cost_per_item"))
    if top_level_cost is not None:
        return top_level_cost

    variants = payload.get("variants") if isinstance(payload.get("variants"), list) else []
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        cost = _clean_number_str(variant.get("costPerItem") or variant.get("cost") or variant.get("unit_cost"))
        if cost is not None:
            return cost
    return None


def _extract_seo_payload(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    seo = payload.get("seo") if isinstance(payload.get("seo"), dict) else {}
    seo_title = None
    seo_description = None

    if isinstance(seo, dict) and "title" in seo:
        seo_title = "" if seo.get("title") is None else str(seo.get("title")).strip()
    elif "seoTitle" in payload:
        seo_title = "" if payload.get("seoTitle") is None else str(payload.get("seoTitle")).strip()

    if isinstance(seo, dict) and "description" in seo:
        seo_description = "" if seo.get("description") is None else str(seo.get("description")).strip()
    elif "seoDescription" in payload:
        seo_description = "" if payload.get("seoDescription") is None else str(payload.get("seoDescription")).strip()

    return {
        "title": seo_title,
        "description": seo_description,
        "handle": _clean_str(seo.get("handle") if seo else payload.get("handle")),
    }


def _apply_post_product_updates(shopify, product: Dict[str, Any], product_data: Dict[str, Any]):
    product_id = product.get("id")
    if not product_id:
        return

    seo = _extract_seo_payload(product_data)
    if seo["title"] is not None or seo["description"] is not None:
        shopify.set_product_seo(product_id, title=seo["title"], description=seo["description"])

    collection_ids = _normalize_collection_ids(product_data.get("collections"))
    if "collections" in product_data:
        shopify.sync_product_collections(product_id, collection_ids)

    cost = _extract_variant_cost(product_data)
    if cost is not None:
        variants = product.get("variants") or []
        if variants:
            inventory_item_id = variants[0].get("inventory_item_id")
            if inventory_item_id:
                shopify.update_inventory_item(inventory_item_id, {"cost": cost})


def _enrich_product_for_editor(shopify, product: Dict[str, Any]) -> Dict[str, Any]:
    product_id = product.get("id")
    if not product_id:
        return product

    enriched = dict(product)
    try:
        enriched["collection_ids"] = shopify.get_product_collection_ids(product_id)
    except Exception:
        enriched["collection_ids"] = []

    try:
        enriched["seo"] = shopify.get_product_seo(product_id)
    except Exception:
        enriched["seo"] = {"title": "", "description": ""}

    variants = enriched.get("variants") or []
    if variants:
        inventory_item_id = variants[0].get("inventory_item_id")
        if inventory_item_id:
            try:
                inventory_item = shopify.get_inventory_item(inventory_item_id).get("inventory_item", {})
                if inventory_item.get("cost") not in (None, ""):
                    variants[0]["cost"] = inventory_item.get("cost")
            except Exception:
                pass

    return enriched


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
                    r = shopify._request(
                        "GET",
                        next_url,
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
        result = shopify.get_product(product_id)
        product = result.get("product") if isinstance(result, dict) else None
        if isinstance(product, dict):
            result["product"] = _enrich_product_for_editor(shopify, product)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_product(product_data: dict):
    try:
        normalized = normalize_product_payload(product_data, is_update=False)
        if "title" not in normalized:
            raise ValueError("Product title is required")
        shopify = get_shopify_client()
        result = shopify.create_product(normalized)
        product = result.get("product") if isinstance(result, dict) else None
        if isinstance(product, dict):
            _apply_post_product_updates(shopify, product, product_data)
            result["product"] = _enrich_product_for_editor(shopify, product)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{product_id}")
async def update_product(product_id: int, product_data: dict):
    try:
        normalized = normalize_product_payload(product_data, is_update=True)
        shopify = get_shopify_client()
        result = shopify.update_product(product_id, normalized)
        product = result.get("product") if isinstance(result, dict) else None
        if isinstance(product, dict):
            _apply_post_product_updates(shopify, product, product_data)
            refreshed = shopify.get_product(product_id)
            refreshed_product = refreshed.get("product") if isinstance(refreshed, dict) else None
            if isinstance(refreshed_product, dict):
                result["product"] = _enrich_product_for_editor(shopify, refreshed_product)
        return result
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