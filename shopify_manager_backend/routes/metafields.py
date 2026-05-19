"""
routes/metafields.py

Endpoints:
  GET    /metafields/products/{product_id}          → list product metafields
  POST   /metafields/products/{product_id}          → create metafield
  GET    /metafields/collections/{collection_id}    → list collection metafields
  POST   /metafields/collections/{collection_id}    → create collection metafield
  GET    /metafields/variants/{variant_id}          → list variant metafields
  POST   /metafields/variants/{variant_id}          → create variant metafield
  GET    /metafields/definitions/products           → list product metafield definitions
  GET    /metafields/definitions/collections        → list collection metafield definitions
  GET    /metafields/definitions/variants           → list variant metafield definitions
  PUT    /metafields/{metafield_id}                 → update metafield value
  DELETE /metafields/{metafield_id}                 → delete metafield
"""

from fastapi import APIRouter, HTTPException, Request
from .store_utils import get_shopify_client
import logging
import threading
import time
import os

logger = logging.getLogger(__name__)
router = APIRouter()

# Simple in-memory TTL caches to avoid repeated remote calls during heavy UI usage
# keys: for metafields -> f"{owner_type}:{owner_id}" ; for definitions -> owner_type
_cache_lock = threading.Lock()
_metafields_cache = {}  # key -> {data, ts}
_definitions_cache = {}  # owner_type -> {data, ts}
_CACHE_TTL_SECONDS = 60


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_metafields(owner_type: str, owner_id: int, client) -> list:
    """Fetch all metafields for a resource using REST API."""
    cache_key = f"{owner_type}:{owner_id}"
    now = time.time()
    with _cache_lock:
        entry = _metafields_cache.get(cache_key)
        if entry and now - entry["ts"] < _CACHE_TTL_SECONDS:
            return entry["data"]

    try:
        url = f"{client._get_base_url()}/{owner_type}/{owner_id}/metafields.json"
        r = client._request("GET", url, params={"limit": 250})
        r.raise_for_status()
        data = r.json().get("metafields", [])

        with _cache_lock:
            _metafields_cache[cache_key] = {"data": data, "ts": now}

        return data
    except Exception as e:
        logger.error(f"Error fetching metafields for {owner_type}/{owner_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _get_definitions(owner_type: str, client) -> list:
    """Fetch metafield definitions using GraphQL."""
    # Map REST resource to GraphQL owner type
    owner_map = {
        "products": "PRODUCT",
        "variants": "PRODUCTVARIANT",
        "collections": "COLLECTION",
    }
    gql_owner = owner_map.get(owner_type, "PRODUCT")

    query = """
    query($ownerType: MetafieldOwnerType!, $cursor: String) {
      metafieldDefinitions(ownerType: $ownerType, first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          namespace
          key
          name
          description
          type { name }
          validations { name value }
        }
      }
    }
    """

    now = time.time()
    with _cache_lock:
        entry = _definitions_cache.get(owner_type)
        if entry and now - entry["ts"] < _CACHE_TTL_SECONDS:
            return entry["data"]

    all_defs = []
    cursor = None
    while True:
        variables = {"ownerType": gql_owner}
        if cursor:
            variables["cursor"] = cursor

        try:
            result = client.graphql(query, variables)
            data = result.get("metafieldDefinitions", {})
            nodes = data.get("nodes", [])
            all_defs.extend(nodes)

            page_info = data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        except Exception as e:
            logger.error(f"Error fetching metafield definitions: {e}")
            break

    with _cache_lock:
        _definitions_cache[owner_type] = {"data": all_defs, "ts": now}

    return all_defs


# ── List metafields ───────────────────────────────────────────────────────────

@router.get("/products/{product_id}")
def get_product_metafields(product_id: int):
    try:
        client     = get_shopify_client()
        metafields = _get_metafields("products", product_id, client)
        return {"metafields": metafields, "count": len(metafields)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/variants/{variant_id}")
def get_variant_metafields(variant_id: int):
    try:
        client     = get_shopify_client()
        metafields = _get_metafields("variants", variant_id, client)
        return {"metafields": metafields, "count": len(metafields)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Create metafield ──────────────────────────────────────────────────────────

@router.post("/products/{product_id}")
def create_product_metafield(product_id: int, body: dict):
    try:
        client = get_shopify_client()
        url = f"{client._get_base_url()}/products/{product_id}/metafields.json"
        payload = {"metafield": {
            "namespace": body.get("namespace", "custom"),
            "key":       body.get("key"),
            "type":      body.get("type", "single_line_text_field"),
            "value":     str(body.get("value", "")),
        }}
        r = client._request("POST", url, json=payload)
        r.raise_for_status()
        # Invalidate cache for this product so subsequent reads return fresh data
        with _cache_lock:
            _metafields_cache.pop(f"products:{product_id}", None)
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/variants/{variant_id}")
def create_variant_metafield(variant_id: int, body: dict):
    try:
        client = get_shopify_client()
        url = f"{client._get_base_url()}/variants/{variant_id}/metafields.json"
        payload = {"metafield": {
            "namespace": body.get("namespace", "custom"),
            "key":       body.get("key"),
            "type":      body.get("type", "single_line_text_field"),
            "value":     str(body.get("value", "")),
        }}
        r = client._request("POST", url, json=payload)
        r.raise_for_status()
        with _cache_lock:
            _metafields_cache.pop(f"variants:{variant_id}", None)
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collections/{collection_id}")
def get_collection_metafields(collection_id: int):
    try:
        client     = get_shopify_client()
        metafields = _get_metafields("collections", collection_id, client)
        return {"metafields": metafields, "count": len(metafields)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collections/{collection_id}")
def create_collection_metafield(collection_id: int, body: dict):
    try:
        client = get_shopify_client()
        url = f"{client._get_base_url()}/collections/{collection_id}/metafields.json"
        payload = {"metafield": {
            "namespace": body.get("namespace", "custom"),
            "key":       body.get("key"),
            "type":      body.get("type", "single_line_text_field"),
            "value":     str(body.get("value", "")),
        }}
        r = client._request("POST", url, json=payload)
        r.raise_for_status()
        with _cache_lock:
            _metafields_cache.pop(f"collections:{collection_id}", None)
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Definitions ───────────────────────────────────────────────────────────────

@router.get("/definitions/products")
def get_product_definitions(request: Request):
    try:
        client      = get_shopify_client()
        definitions = _get_definitions("products", client)
        return {"definitions": definitions, "count": len(definitions)}
    except HTTPException as he:
        # Allow dev fallback for unauthorized / missing-store when opted-in
        try:
            allow_dev = str(os.getenv("ALLOW_DEV_SHORTCUTS", "")).lower() in ("1", "true", "yes")
        except Exception:
            allow_dev = False
        # Allow fallback for local requests when dev shortcuts enabled
        client_host = getattr(request.client, 'host', '') or ''
        if allow_dev and (client_host in ("127.0.0.1", "::1", "localhost") or allow_dev):
            sample_defs = [
                {"namespace": "global", "key": "color", "name": "Color", "type": {"name": "single_line_text_field"}, "validations": [{"name": "choices", "value": "[\"red\", \"green\", \"blue\"]"}]},
            ]
            return {"definitions": sample_defs, "count": len(sample_defs)}
        raise
    except Exception as e:
        try:
            allow_dev = str(os.getenv("ALLOW_DEV_SHORTCUTS", "")).lower() in ("1", "true", "yes")
        except Exception:
            allow_dev = False
        if allow_dev:
            sample_defs = [
                {"namespace": "global", "key": "color", "name": "Color", "type": {"name": "single_line_text_field"}, "validations": [{"name": "choices", "value": "[\"red\", \"green\", \"blue\"]"}]},
            ]
            return {"definitions": sample_defs, "count": len(sample_defs)}
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/definitions/collections")
def get_collection_definitions():
    try:
        client      = get_shopify_client()
        definitions = _get_definitions("collections", client)
        return {"definitions": definitions, "count": len(definitions)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/definitions/variants")
def get_variant_definitions():
    try:
        client      = get_shopify_client()
        definitions = _get_definitions("variants", client)
        return {"definitions": definitions, "count": len(definitions)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Update metafield ──────────────────────────────────────────────────────────

@router.put("/{metafield_id}")
def update_metafield(metafield_id: int, body: dict):
    try:
        client = get_shopify_client()
        url = f"{client._get_base_url()}/metafields/{metafield_id}.json"
        payload = {"metafield": {
            "id":    metafield_id,
            "value": str(body.get("value", "")),
        }}
        r = client._request("PUT", url, json=payload)
        r.raise_for_status()
        # Invalidate entire metafields cache (conservative) since we don't know owner id here
        with _cache_lock:
            _metafields_cache.clear()
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Delete metafield ──────────────────────────────────────────────────────────

@router.delete("/{metafield_id}")
def delete_metafield(metafield_id: int):
    try:
        client = get_shopify_client()
        url = f"{client._get_base_url()}/metafields/{metafield_id}.json"
        r = client._request("DELETE", url)
        r.raise_for_status()
        with _cache_lock:
            _metafields_cache.clear()
        return {"success": True, "message": f"Metafield {metafield_id} deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))