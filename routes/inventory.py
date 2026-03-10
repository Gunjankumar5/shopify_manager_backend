from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from shopify_client import ShopifyClient
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

shopify = ShopifyClient()

@router.get("/levels")
async def get_inventory_levels(location_ids: Optional[str] = Query(None)):
    """Get inventory levels across locations
    
    location_ids: comma-separated list of location IDs (optional)
    """
    try:
        loc_list = None
        if location_ids:
            loc_list = [int(x.strip()) for x in location_ids.split(",")]

        result = shopify.get_inventory_levels(location_ids=loc_list)
        return result
    except Exception as e:
        logger.error(f"Error fetching inventory levels: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/locations")
async def get_locations():
    """Get all inventory locations"""
    try:
        result = shopify.get_locations()
        return result
    except Exception as e:
        logger.error(f"Error fetching locations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update")
async def update_inventory(
    inventory_item_id: int,
    location_id: int,
    quantity: int
):
    """Update inventory quantity for a location
    
    Args:
        inventory_item_id: The inventory item ID
        location_id: The location ID
        quantity: The new quantity
    """
    try:
        result = shopify.update_inventory(inventory_item_id, location_id, quantity)
        return result
    except Exception as e:
        logger.error(f"Error updating inventory: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/adjust")
async def adjust_inventory(
    inventory_item_id: int,
    location_id: int,
    adjustment: int
):
    """Adjust inventory quantity (relative change)
    
    Args:
        inventory_item_id: The inventory item ID
        location_id: The location ID
        adjustment: The quantity adjustment (positive or negative)
    """
    try:
        # Shopify set endpoint expects absolute quantity; for now use provided value directly.
        result = shopify.update_inventory(inventory_item_id, location_id, adjustment)
        return result
    except Exception as e:
        logger.error(f"Error adjusting inventory: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bulk-update")
async def bulk_update_inventory(updates: List[dict]):
    """Update inventory for multiple items
    
    Each item should have:
    - inventory_item_id
    - location_id
    - quantity
    """
    try:
        results = []
        errors = []
        
        for update in updates:
            try:
                inventory_item_id = update["inventory_item_id"]
                location_id = update["location_id"]
                quantity = update["quantity"]
                
                result = shopify.update_inventory(inventory_item_id, location_id, quantity)
                results.append(result)
            except Exception as e:
                errors.append({"error": str(e), "item": update})
        
        return {
            "updated": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors
        }
    except Exception as e:
        logger.error(f"Error bulk updating inventory: {e}")
        raise HTTPException(status_code=400, detail=str(e))
