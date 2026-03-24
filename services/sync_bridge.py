"""
sync_bridge.py

CRITICAL FIX: Python's ContextVar does NOT propagate across thread boundaries.
So user_id MUST be passed explicitly from the FastAPI route into this thread.
"""

import json
import time
import threading
import traceback
from queue import Queue

DONE_SENTINEL = "__SYNC_DONE__"

_queues: dict[str, Queue] = {}
_lock = threading.Lock()


def _log(msg: str):
    print(f"[EXCEL_SYNC] {msg}")


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


def _numeric_id(gid: str) -> str:
    return str(gid).split("/")[-1]


def _diff_row(row: dict, snapshot: dict) -> tuple[dict, dict, str]:
    """
    Compare a row to snapshot and return (product_changes, variant_changes, error_msg).
    error_msg is None if product found in snapshot, or a descriptive error if not.
    """
    product_gid = row.get("Product ID", "")
    snap_entry  = snapshot.get(product_gid)
    if not snap_entry:
        # Return empty changes and an error code so caller can handle appropriately
        return {}, {}, f"not_in_snapshot"

    snap_product  = snap_entry.get("product", {})
    snap_variants = snap_entry.get("variants", [])

    variant_gid  = row.get("Variant ID", "")
    snap_variant = {}
    for sv in snap_variants:
        if sv.get("id") == variant_gid:
            snap_variant = sv
            break

    PRODUCT_FIELDS = {
        "Title":       ("title",           "title"),
        "Body (HTML)": ("descriptionHtml", "body_html"),
        "Vendor":      ("vendor",          "vendor"),
        "Type":        ("productType",     "product_type"),
        "Tags":        ("tags",            "tags"),
        "Status":      ("status",          "status"),
    }

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
        if snap_key == "tags" and isinstance(old_val, list):
            old_val = ", ".join(old_val)
        if str(new_val or "") != str(old_val or ""):
            product_changes[api_key] = str(new_val).lower() if api_key == "status" else new_val

    variant_changes = {}
    for row_key, (snap_key, api_key) in VARIANT_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_variant.get(snap_key, "")
        if str(new_val or "") != str(old_val or ""):
            variant_changes[api_key] = str(new_val) if new_val else None

    return product_changes, variant_changes, None  # None = no error


def run_sync(
    session_id: str,
    rows: list[dict],
    snapshot: dict,
    shop_key: str | None = None,
    user_id: str | None = None,
):
    """
    Run sync in a background thread.
    user_id is passed explicitly because ContextVar does NOT cross thread boundaries.
    """
    queue = pop_queue(session_id)

    def push(msg: dict):
        if queue:
            queue.put(json.dumps(msg))

    counts     = {"total": 0, "updated": 0, "skipped": 0, "errors": 0}
    start_time = time.time()
    _log(f"session={session_id} started rows={len(rows)} user={user_id}")

    try:
        # ── Set user context inside this thread ───────────────────────────
        from routes.store_utils import set_request_user_id, get_shopify_client
        set_request_user_id(user_id)

        if not user_id:
            _log(f"session={session_id} ERROR: No user_id provided")
            push({"done": True, "error": "Not authenticated. Please log in.", "auth_error": True})
            return

        # ── Get Shopify client ────────────────────────────────────────────
        try:
            client = get_shopify_client(shop_key=shop_key, user_id=user_id)
        except Exception as e:
            _log(f"session={session_id} ERROR: {e}")
            push({"done": True, "error": str(e), "auth_error": True})
            return

        # ── Process rows ──────────────────────────────────────────────────
        for idx, row in enumerate(rows):
            counts["total"] += 1
            product_gid = row.get("Product ID", "")
            variant_gid = row.get("Variant ID", "")
            product_id  = _numeric_id(product_gid)
            variant_id  = _numeric_id(variant_gid)

            # ── Validate Product ID format ────────────────────────────────
            if not product_id.isdigit():
                error_msg = f"Invalid Product ID: {product_gid or '(empty)'}"
                push({"row_index": idx, "variant_id": variant_gid, "status": "SKIPPED", "changes": [], "error": error_msg})
                counts["skipped"] += 1
                _log(f"session={session_id} row={idx+1} SKIPPED {error_msg}")
                continue

            try:
                product_changes, variant_changes, diff_error = _diff_row(row, snapshot)

                # ── Check for products not in snapshot ────────────────────
                if diff_error == "not_in_snapshot":
                    error_msg = "Product not in snapshot (may be newly created)"
                    push({"row_index": idx, "variant_id": variant_gid, "status": "SKIPPED", "changes": [], "error": error_msg})
                    counts["skipped"] += 1
                    _log(f"session={session_id} row={idx+1} SKIPPED product not in snapshot")
                    continue

                # ── Check if there are any actual changes ──────────────────
                if not product_changes and not variant_changes:
                    push({"row_index": idx, "variant_id": variant_gid, "status": "SKIPPED", "changes": [], "error": "No changes detected"})
                    counts["skipped"] += 1
                    continue

                change_list = []
                if product_changes:
                    client.update_product(product_id, product_changes)
                    change_list += list(product_changes.keys())

                if variant_changes and variant_id.isdigit():
                    client.update_product_variant(product_id, variant_id, {"id": int(variant_id), **variant_changes})
                    change_list += list(variant_changes.keys())

                push({"row_index": idx, "variant_id": variant_gid, "status": "UPDATED", "changes": change_list, "error": None})
                counts["updated"] += 1
                _log(f"session={session_id} row={idx+1}/{len(rows)} UPDATED changes={','.join(change_list)}")

            except Exception as row_err:
                push({"row_index": idx, "variant_id": variant_gid, "status": "ERROR", "changes": [], "error": str(row_err), "traceback": traceback.format_exc()})
                counts["errors"] += 1
                _log(f"session={session_id} row={idx+1}/{len(rows)} ERROR {row_err}")

        push({
            "done": True,
            "total": counts["total"],
            "updated": counts["updated"],
            "created": 0,
            "skipped": counts["skipped"],
            "deleted": 0,
            "errors": counts["errors"],
            "conflicts": 0,
            "duration_seconds": round(time.time() - start_time, 2),
        })
        _log(f"session={session_id} finished total={counts['total']} updated={counts['updated']} skipped={counts['skipped']} errors={counts['errors']}")

    except Exception as e:
        push({"done": True, "error": str(e), "auth_error": "401" in str(e), "traceback": traceback.format_exc()})
        _log(f"session={session_id} fatal_error={e}")

    finally:
        if queue:
            queue.put(DONE_SENTINEL)