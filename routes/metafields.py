"""
routes/metafields.py

Endpoints:
  GET    /api/metafields/definitions/{owner_type}   → metafield definitions (GraphQL)
  GET    /api/metafields/{resource}/{resource_id}   → list metafields
  POST   /api/metafields/{resource}/{resource_id}   → create metafield
  PUT    /api/metafields/{metafield_id}             → update metafield
  DELETE /api/metafields/{metafield_id}             → delete metafield
"""

from fastapi import APIRouter, HTTPException, Depends
from routes.store_utils import get_shopify_client
from routes.auth_utils import require_authenticated_user
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

VALID_RESOURCES = {"products", "collections", "variants"}

OWNER_TYPE_MAP = {
    "products": "PRODUCT",
    "collections": "COLLECTION",
    "variants": "PRODUCTVARIANT",
}


# ── Definitions ───────────────────────────────────────────────────────────────

@router.get("/definitions/{owner_type}")
def get_metafield_definitions(
    owner_type: str,
    user_id: str = Depends(require_authenticated_user),
):
    gql_type = OWNER_TYPE_MAP.get(owner_type.lower())
    if not gql_type:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid owner_type. Use: {', '.join(OWNER_TYPE_MAP.keys())}",
        )
    try:
        return get_shopify_client().get_metafield_definitions(gql_type)
    except Exception as e:
        logger.error(f"Error fetching metafield definitions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/{resource}/{resource_id}")
def list_metafields(
    resource: str,
    resource_id: int,
    user_id: str = Depends(require_authenticated_user),
):
    if resource not in VALID_RESOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid resource. Use: {', '.join(VALID_RESOURCES)}")
    try:
        return get_shopify_client().get_metafields(resource, resource_id)
    except Exception as e:
        logger.error(f"Error listing metafields for {resource}/{resource_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("/{resource}/{resource_id}")
def create_metafield(
    resource: str,
    resource_id: int,
    data: dict,
    user_id: str = Depends(require_authenticated_user),
):
    if resource not in VALID_RESOURCES:
        raise HTTPException(status_code=400, detail="Invalid resource.")
    required = {"namespace", "key", "type", "value"}
    missing = required - data.keys()
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing)}")
    try:
        return get_shopify_client().create_metafield(resource, resource_id, data)
    except Exception as e:
        logger.error(f"Error creating metafield: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ── Update ────────────────────────────────────────────────────────────────────

@router.put("/{metafield_id}")
def update_metafield(
    metafield_id: int,
    data: dict,
    user_id: str = Depends(require_authenticated_user),
):
    if "value" not in data:
        raise HTTPException(status_code=400, detail="Body must include 'value'")
    try:
        return get_shopify_client().update_metafield(metafield_id, data)
    except Exception as e:
        logger.error(f"Error updating metafield {metafield_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{metafield_id}")
def delete_metafield(
    metafield_id: int,
    user_id: str = Depends(require_authenticated_user),
):
    try:
        return get_shopify_client().delete_metafield(metafield_id)
    except Exception as e:
        logger.error(f"Error deleting metafield {metafield_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))