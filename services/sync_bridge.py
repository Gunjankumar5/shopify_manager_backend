"""
sync_bridge.py

Self-contained sync engine that compares edited grid rows against the
snapshot baseline and pushes only the changed fields to Shopify via REST.

Called by the sync WebSocket endpoint. Runs in a daemon thread.
"""

import json
import time
import threading
import traceback
from queue import Queue

from shopify_client import ShopifyClient
from routes.store_utils import load_stores, get_active_store_key, get_connected_store

# ─── Sentinel ────────────────────────────────────────────────────────────────
DONE_SENTINEL = "__SYNC_DONE__"

# ─── Per-request progress queues ─────────────────────────────────────────────
_queues: dict[str, Queue] = {}
_lock = threading.Lock()


def get_queue(session_id: str) -> Queue:
    with _lock:
        q = Queue()
        _queues[session_id] = q
    return q


def pop_queue(session_id: str) -> Queue | None:
    with _lock:
        return _queues.get(session_id)


def remove_queue(session_id: str):
    with _lock:
        _queues.pop(session_id, None)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _numeric_id(gid: str) -> str:
    """Extract numeric ID from a Shopify GID like 'gid://shopify/Product/123'."""
    return str(gid).split("/")[-1]


def _diff_row(row: dict, snapshot: dict) -> tuple[dict, dict]:
    """
    Compare a row against snapshot data and return dicts of changed
    product-level and variant-level fields.
    """
    product_gid = row.get("Product ID", "")
    snap_entry = snapshot.get(product_gid)
    if not snap_entry:
        return {}, {}

    snap_product = snap_entry.get("product", {})
    snap_variants = snap_entry.get("variants", [])

    variant_gid = row.get("Variant ID", "")
    snap_variant = {}
    for sv in snap_variants:
        if sv.get("id") == variant_gid:
            snap_variant = sv
            break

    # Product-level field mapping: row key → (snapshot key, payload key)
    PRODUCT_FIELDS = {
        "Title":            ("title",           "title"),
        "Body (HTML)":      ("descriptionHtml", "body_html"),
        "Vendor":           ("vendor",          "vendor"),
        "Type":             ("productType",     "product_type"),
        "Tags":             ("tags",            "tags"),
        "Status":           ("status",          "status"),
    }

    # Variant-level field mapping: row key → (snapshot key, payload key)
    VARIANT_FIELDS = {
        "Variant Price":            ("price",          "price"),
        "Variant Compare At Price": ("compareAtPrice", "compare_at_price"),
        "Variant SKU":              ("sku",            "sku"),
        "Variant Barcode":          ("barcode",        "barcode"),
    }

    product_changes = {}
    for row_key, (snap_key, api_key) in PRODUCT_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_product.get(snap_key, "")
        # Normalize tags (snapshot stores as list, row as comma string)
        if snap_key == "tags" and isinstance(old_val, list):
            old_val = ", ".join(old_val)
        if str(new_val or "") != str(old_val or ""):
            val = new_val
            if api_key == "status":
                val = str(new_val).lower()
            product_changes[api_key] = val

    variant_changes = {}
    for row_key, (snap_key, api_key) in VARIANT_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_variant.get(snap_key, "")
        if str(new_val or "") != str(old_val or ""):
            variant_changes[api_key] = str(new_val) if new_val else None

    return product_changes, variant_changes


# ─── Main sync worker (runs in thread) ───────────────────────────────────────

def _resolve_store_client(shop_key: str | None = None) -> ShopifyClient:
    """Resolve Shopify client from selected/active connected store credentials."""
    store = None

    if shop_key:
        stores = load_stores()
        store = stores.get(shop_key)

    if not store:
        # Fallback to currently active connected store
        store = get_connected_store()

    if store:
        shop_name = store.get("shop") or store.get("shop_name")
        access_token = store.get("access_token")
        api_version = store.get("api_version", "2026-01")
        if shop_name and access_token:
            return ShopifyClient(
                shop_name=shop_name,
                access_token=access_token,
                api_version=api_version,
            )

    # Last fallback for backward compatibility (env-based client)
    return ShopifyClient()


def run_sync(session_id: str, rows: list[dict], snapshot: dict, shop_key: str | None = None):
    """
    Sync pipeline:
      1. Compare each row against snapshot to find changes
      2. Push changed product/variant fields to Shopify REST API
      3. Stream row-by-row progress via queue
      4. Push summary + sentinel when done
    """
    queue = pop_queue(session_id)

    def push(msg: dict):
        if queue:
            queue.put(json.dumps(msg))

    counts = {"total": 0, "updated": 0, "skipped": 0, "errors": 0}
    start_time = time.time()

    try:
        client = _resolve_store_client(shop_key)

        for idx, row in enumerate(rows):
            counts["total"] += 1

            product_gid = row.get("Product ID", "")
            variant_gid = row.get("Variant ID", "")
            product_id = _numeric_id(product_gid)
            variant_id = _numeric_id(variant_gid)

            if not product_id.isdigit():
                push({
                    "row_index": idx,
                    "variant_id": variant_gid,
                    "status": "SKIPPED",
                    "changes": [],
                    "error": "Missing Product ID",
                })
                counts["skipped"] += 1
                continue

            try:
                product_changes, variant_changes = _diff_row(row, snapshot)

                if not product_changes and not variant_changes:
                    push({
                        "row_index": idx,
                        "variant_id": variant_gid,
                        "status": "SKIPPED",
                        "changes": [],
                        "error": None,
                    })
                    counts["skipped"] += 1
                    continue

                change_list = []

                if product_changes:
                    client.update_product(product_id, product_changes)
                    change_list += list(product_changes.keys())

                if variant_changes and variant_id.isdigit():
                    client.update_product_variant(
                        product_id, variant_id, {"id": int(variant_id), **variant_changes}
                    )
                    change_list += list(variant_changes.keys())

                push({
                    "row_index": idx,
                    "variant_id": variant_gid,
                    "status": "UPDATED",
                    "changes": change_list,
                    "error": None,
                })
                counts["updated"] += 1

            except Exception as row_err:
                push({
                    "row_index": idx,
                    "variant_id": variant_gid,
                    "status": "ERROR",
                    "changes": [],
                    "error": str(row_err),
                })
                counts["errors"] += 1

        # Summary
        push({
            "done": True,
            "total":            counts["total"],
            "updated":          counts["updated"],
            "created":          0,
            "skipped":          counts["skipped"],
            "deleted":          0,
            "errors":           counts["errors"],
            "conflicts":        0,
            "duration_seconds": round(time.time() - start_time, 2),
        })

    except Exception as e:
        err = str(e)
        push({
            "done": True,
            "error": err,
            "auth_error": "401" in err or "Unauthorized" in err,
            "traceback": traceback.format_exc(),
        })
        print(f"[SYNC_BRIDGE] Fatal error: {e}")

    finally:
        if queue:
            queue.put(DONE_SENTINEL)