"""
sync_bridge.py

Syncs ALL grid columns back to Shopify:
  - Standard product fields (Title, Body, Vendor, Type, Tags, Status)
  - SEO fields (SEO Title, SEO Description)
  - All metafield columns (display name → ns.key via metafield_defs.json)
  - Variant fields (Price, Compare At Price, SKU, Barcode)

CRITICAL: Python's ContextVar does NOT propagate across thread boundaries.
user_id MUST be passed explicitly from the FastAPI route into this thread.
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


def _build_metafield_lookup() -> tuple[dict, dict]:
    """
    Build two lookup dicts from metafield_defs.json:
      - display_to_nskey: { "Snowboard length": "custom.snowboard_length" }
      - nskey_to_type:    { "custom.snowboard_length": "single_line_text_field" }
    """
    try:
        from services.metafield_defs import load_metafield_defs
        defs = load_metafield_defs().get("product", {})
        display_to_nskey = {}
        nskey_to_type    = {}
        for ns_key, meta in defs.items():
            name = meta.get("name", "").strip()
            if name:
                display_to_nskey[name] = ns_key
            # Also map ns.key → ns.key directly (fallback)
            display_to_nskey[ns_key] = ns_key
            nskey_to_type[ns_key] = meta.get("type", "single_line_text_field")
        return display_to_nskey, nskey_to_type
    except Exception as e:
        _log(f"Could not load metafield defs: {e}")
        return {}, {}


def _diff_row(row: dict, snapshot: dict, display_to_nskey: dict) -> tuple[dict, dict, dict, dict]:
    """
    Compare a grid row against the snapshot and return changed fields.

    Returns:
        product_changes   — standard REST product fields
        variant_changes   — variant fields
        metafield_changes — { "ns.key": new_value }
        seo_changes       — { "title": ..., "description": ... }
    """
    product_gid = row.get("Product ID", "")
    snap_entry  = snapshot.get(product_gid)
    if not snap_entry:
        return {}, {}, {}, {}

    snap_product  = snap_entry.get("product", {})
    snap_variants = snap_entry.get("variants", [])

    variant_gid  = row.get("Variant ID", "")
    snap_variant = {}
    for sv in snap_variants:
        if sv.get("id") == variant_gid:
            snap_variant = sv
            break

    # ── Standard product fields ───────────────────────────────────────────────
    PRODUCT_FIELDS = {
        "Title":       ("title",           "title"),
        "Body (HTML)": ("descriptionHtml", "body_html"),
        "Vendor":      ("vendor",          "vendor"),
        "Type":        ("productType",     "product_type"),
        "Tags":        ("tags",            "tags"),
        "Status":      ("status",          "status"),
    }

    product_changes = {}
    for row_key, (snap_key, api_key) in PRODUCT_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_product.get(snap_key, "")
        if snap_key == "tags" and isinstance(old_val, list):
            old_val = ", ".join(old_val)
        if str(new_val or "") != str(old_val or ""):
            product_changes[api_key] = str(new_val).lower() if api_key == "status" else new_val

    # ── Variant fields ────────────────────────────────────────────────────────
    VARIANT_FIELDS = {
        "Variant Price":            ("price",          "price"),
        "Variant Compare At Price": ("compareAtPrice", "compare_at_price"),
        "Variant SKU":              ("sku",            "sku"),
        "Variant Barcode":          ("barcode",        "barcode"),
    }

    variant_changes = {}
    for row_key, (snap_key, api_key) in VARIANT_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_variant.get(snap_key, "")
        if str(new_val or "") != str(old_val or ""):
            variant_changes[api_key] = str(new_val) if new_val else None

    # ── SEO fields ────────────────────────────────────────────────────────────
    SEO_FIELDS = {
        "SEO Title":       "title",
        "SEO Description": "description",
    }

    seo_changes = {}
    snap_seo = snap_product.get("seo") or {}
    for row_key, api_key in SEO_FIELDS.items():
        new_val = row.get(row_key, "")
        old_val = snap_seo.get(api_key, "")
        if str(new_val or "") != str(old_val or ""):
            seo_changes[api_key] = new_val or ""

    # ── Metafield columns ─────────────────────────────────────────────────────
    # Skip all known non-metafield columns
    SKIP_COLUMNS = {
        "Product ID", "Handle", "Title", "Body (HTML)", "Vendor", "Type",
        "Tags", "Status", "Created At", "Updated At", "Last Synced",
        "SEO Title", "SEO Description", "Product Metafields",
        "Image URLs", "Image Alt Text",
        "Variant ID", "Variant SKU", "Variant Price", "Variant Compare At Price",
        "Variant Barcode", "Variant Inventory Policy",
        "Inventory Item ID", "Inventory Tracked", "Requires Shipping",
        "Cost per item", "Variant Grams", "Variant Weight Unit",
        "Collection Names", "Collection Handles", "Collection Metafields",
        "Sync Status", "Last Synced",
    }

    metafield_changes = {}
    snap_metafields = snap_entry.get("metafields", {})  # { "ns.key": current_value }

    for col_name, new_val in row.items():
        # Skip known non-metafield columns
        if col_name in SKIP_COLUMNS:
            continue
        # Skip inventory columns
        if col_name.startswith("Inventory Qty -"):
            continue

        # Try to resolve column name to ns.key
        ns_key = display_to_nskey.get(col_name)
        if not ns_key:
            continue

        # Compare against snapshot metafield value
        old_val = snap_metafields.get(ns_key, "")
        if str(new_val or "") != str(old_val or ""):
            metafield_changes[ns_key] = str(new_val) if new_val is not None else ""

    return product_changes, variant_changes, metafield_changes, seo_changes


def _sync_metafields(client, product_gid: str, metafield_changes: dict, nskey_to_type: dict):
    """
    Push metafield changes to Shopify using GraphQL metafieldsSet mutation.
    """
    if not metafield_changes:
        return

    metafields_input = []
    for ns_key, value in metafield_changes.items():
        try:
            ns, key = ns_key.split(".", 1)
        except ValueError:
            _log(f"Invalid ns.key format: {ns_key}, skipping")
            continue

        mf_type = nskey_to_type.get(ns_key, "single_line_text_field")
        metafields_input.append({
            "ownerId":   product_gid,
            "namespace": ns,
            "key":       key,
            "value":     value,
            "type":      mf_type,
        })

    if not metafields_input:
        return

    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { namespace key value }
        userErrors  { field message }
      }
    }
    """
    result = client.graphql(mutation, {"metafields": metafields_input})
    errors = (result.get("metafieldsSet") or {}).get("userErrors", [])
    if errors:
        raise Exception(f"Metafield errors: {errors}")


def _sync_seo(client, product_id: str, seo_changes: dict):
    """
    Push SEO title/description to Shopify via REST metafields.
    """
    if not seo_changes:
        return

    # SEO fields are stored as global namespace metafields in Shopify
    SEO_METAFIELD_MAP = {
        "title":       ("global", "title_tag"),
        "description": ("global", "description_tag"),
    }

    for api_key, value in seo_changes.items():
        if api_key not in SEO_METAFIELD_MAP:
            continue
        namespace, key = SEO_METAFIELD_MAP[api_key]
        client._post(
            f"/products/{product_id}/metafields.json",
            {
                "metafield": {
                    "namespace": namespace,
                    "key":       key,
                    "value":     value or "",
                    "type":      "single_line_text_field",
                }
            }
        )


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

        # ── Build metafield lookup once for all rows ──────────────────────
        display_to_nskey, nskey_to_type = _build_metafield_lookup()
        _log(f"session={session_id} metafield_defs loaded: {len(display_to_nskey)} columns mapped")

        # Track which products have already been updated to avoid duplicate API calls
        updated_products = set()
        updated_seo_products = set()

        for idx, row in enumerate(rows):
            # ← ADD THIS for first row only
            if idx == 0:
                _log(f"[ROW_KEYS] columns in row: {list(row.keys())}")
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
                product_changes, variant_changes, metafield_changes, seo_changes = _diff_row(
                    row, snapshot, display_to_nskey
                )

                if not product_changes and not variant_changes and not metafield_changes and not seo_changes:
                    push({"row_index": idx, "variant_id": variant_gid, "status": "SKIPPED", "changes": [], "error": None})
                    counts["skipped"] += 1
                    continue

                change_list = []

                # Standard product fields (only update once per product, not once per variant)
                if product_changes and product_id not in updated_products:
                    client.update_product(product_id, product_changes)
                    updated_products.add(product_id)
                    change_list += list(product_changes.keys())

                # Variant fields
                if variant_changes and variant_id.isdigit():
                    client.update_product_variant(
                        product_id, variant_id,
                        {"id": int(variant_id), **variant_changes}
                    )
                    change_list += list(variant_changes.keys())

                # SEO fields (only update once per product, not once per variant)
                if seo_changes and product_id not in updated_seo_products:
                    _sync_seo(client, product_id, seo_changes)
                    updated_seo_products.add(product_id)
                    change_list += [f"seo.{k}" for k in seo_changes.keys()]

                # Metafield columns
                metafield_response = []
                metafield_errors = []
                valid_metafield_changes = {}
                # Validate metafield values against allowed choices
                if metafield_changes:
                    from services.metafield_defs import load_metafield_defs
                    metafield_defs = load_metafield_defs().get("product", {})
                    for ns_key, value in metafield_changes.items():
                        meta = metafield_defs.get(ns_key, {})
                        choices = meta.get("choices")
                        if choices is not None and len(choices) > 0:
                            if str(value) not in [str(c) for c in choices]:
                                # Invalid value, skip this metafield and report error
                                ns, key = ns_key.split(".", 1)
                                col_type = "custom" if ns == "custom" else ("global" if ns == "global" else ns)
                                display_name = None
                                for k, v in display_to_nskey.items():
                                    if v == ns_key:
                                        display_name = k
                                        break
                                metafield_errors.append({
                                    "column": display_name or ns_key,
                                    "namespace": ns,
                                    "key": key,
                                    "type": col_type,
                                    "error": f"Value '{value}' not in allowed choices: {choices}"
                                })
                                continue
                        valid_metafield_changes[ns_key] = value

                if valid_metafield_changes:
                    # If only one metafield is being updated, send only that one
                    if len(valid_metafield_changes) == 1:
                        ns_key, value = next(iter(valid_metafield_changes.items()))
                        _sync_metafields(client, product_gid, {ns_key: value}, nskey_to_type)
                        display_name = None
                        for k, v in display_to_nskey.items():
                            if v == ns_key:
                                display_name = k
                                break
                        ns, key = ns_key.split(".", 1)
                        col_type = "custom" if ns == "custom" else ("global" if ns == "global" else ns)
                        metafield_response.append({
                            "column": display_name or ns_key,
                            "namespace": ns,
                            "key": key,
                            "type": col_type
                        })
                        change_list += [f"metafield:{ns_key}"]
                    else:
                        _sync_metafields(client, product_gid, valid_metafield_changes, nskey_to_type)
                        for ns_key in valid_metafield_changes.keys():
                            ns, key = ns_key.split(".", 1)
                            col_type = "custom" if ns == "custom" else ("global" if ns == "global" else ns)
                            display_name = None
                            for k, v in display_to_nskey.items():
                                if v == ns_key:
                                    display_name = k
                                    break
                            metafield_response.append({
                                "column": display_name or ns_key,
                                "namespace": ns,
                                "key": key,
                                "type": col_type
                            })
                        change_list += [f"metafield:{k}" for k in valid_metafield_changes.keys()]

                if metafield_errors:
                    push({
                        "row_index":  idx,
                        "variant_id": variant_gid,
                        "status":     "ERROR",
                        "changes":    change_list,
                        "metafields": metafield_response,
                        "error":      metafield_errors,
                    })
                    counts["errors"] += 1
                    _log(f"session={session_id} row={idx+1}/{len(rows)} ERROR {metafield_errors}")
                else:
                    push({
                        "row_index":  idx,
                        "variant_id": variant_gid,
                        "status":     "UPDATED",
                        "changes":    change_list,
                        "metafields": metafield_response,
                        "error":      None,
                    })
                    counts["updated"] += 1
                    _log(f"session={session_id} row={idx+1}/{len(rows)} UPDATED changes={','.join(change_list)}")

            except Exception as row_err:
                push({
                    "row_index":  idx,
                    "variant_id": variant_gid,
                    "status":     "ERROR",
                    "changes":    [],
                    "error":      str(row_err),
                    "traceback":  traceback.format_exc(),
                })
                counts["errors"] += 1
                _log(f"session={session_id} row={idx+1}/{len(rows)} ERROR {row_err}")

        push({
            "done":             True,
            "total":            counts["total"],
            "updated":          counts["updated"],
            "created":          0,
            "skipped":          counts["skipped"],
            "deleted":          0,
            "errors":           counts["errors"],
            "conflicts":        0,
            "duration_seconds": round(time.time() - start_time, 2),
        })
        _log(
            f"session={session_id} finished "
            f"total={counts['total']} updated={counts['updated']} "
            f"skipped={counts['skipped']} errors={counts['errors']}"
        )

    except Exception as e:
        push({
            "done":       True,
            "error":      str(e),
            "auth_error": "401" in str(e),
            "traceback":  traceback.format_exc(),
        })
        _log(f"session={session_id} fatal_error={e}")

    finally:
        if queue:
            queue.put(DONE_SENTINEL)