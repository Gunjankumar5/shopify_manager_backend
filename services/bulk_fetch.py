"""
services/bulk_fetch.py

Performance optimizations:
  1. Bulk GraphQL operation fetches ALL product data in ONE request
  2. All secondary data (inventory, collections, metafields, defs) run in PARALLEL
  3. Metafield definitions cached in memory (no re-fetch on repeat loads)
  4. Choice/list metafields store allowed values for frontend dropdowns
  5. Batch size increased to 250 for inventory fetches
"""

import time
import os
import io
import tempfile
import requests
import pandas as pd
import importlib
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

_orjson = None
try:
    _orjson = importlib.import_module("orjson")
except Exception:
    _orjson = None

from routes.store_utils import get_shopify_client
from services.metafield_defs import fetch_and_store_metafield_defs, get_display_name_map, load_metafield_defs

# ── In-memory cache for metafield definitions (avoid re-fetching) ─────────────
_DEFS_CACHE: dict = {}
_DEFS_CACHE_TTL = 300  # 5 minutes
_DEFS_CACHE_TIME: float = 0


class BulkFetchService:

    def __init__(self):
        self.client = get_shopify_client()

    # =========================================================
    # FULL SYNC — optimized parallel fetch
    # =========================================================

    def full_sync(self, progress_callback=None):

        def log(msg):
            if progress_callback:
                progress_callback(msg)
            print(msg)

        t0 = time.time()

        log("Starting Shopify bulk operation...")
        self._start_bulk_operation()

        log("Waiting for bulk operation to complete...")
        op = self._wait_for_bulk_operation()

        log(f"Bulk op done in {time.time()-t0:.1f}s — downloading JSONL...")
        file_path = self._download_jsonl(op["url"], "products_bulk.jsonl")

        try:
            rows, snapshot = self._parse_jsonl(file_path)
        finally:
            os.unlink(file_path)

        log(f"Parsed {len(snapshot)} products, {len(rows)} variants in {time.time()-t0:.1f}s")

        # ── Collect IDs for parallel fetches ─────────────────────────────────
        inventory_ids = list(set(filter(None, (r["Inventory Item ID"] for r in rows))))
        product_ids   = list(set(filter(None, (r["Product ID"] for r in rows))))

        log(f"Launching parallel fetches: {len(inventory_ids)} inventory items, {len(product_ids)} products...")

        # ── ALL secondary fetches run in PARALLEL ─────────────────────────────
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_locations):                           "locations",
                executor.submit(self._fetch_collections):                         "collections",
                executor.submit(self._fetch_product_metafields, product_ids):     "metafields",
                executor.submit(self._fetch_metafield_defs_cached):              "defs",
            }
            # Inventory needs locations first — submit separately after
            results = {}
            locations = None

            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                    log(f"  ✓ {key} done ({time.time()-t0:.1f}s)")
                except Exception as e:
                    log(f"  ✗ {key} failed: {e}")
                    results[key] = {} if key != "metafields" else ({}, [])

            locations = results.get("locations", {})

        # ── Fetch inventory levels now that we have locations ─────────────────
        log(f"Fetching inventory levels ({len(inventory_ids)} items)...")
        inventory_levels = self._fetch_inventory_levels_fast(inventory_ids, locations)
        log(f"  ✓ inventory done ({time.time()-t0:.1f}s)")

        # ── Unpack results ────────────────────────────────────────────────────
        product_collections      = results.get("collections", {})
        product_metafield_result = results.get("metafields", ({}, []))
        metafield_defs           = results.get("defs", {})

        if isinstance(product_metafield_result, tuple):
            product_metafield_values, product_metafield_columns = product_metafield_result
        else:
            product_metafield_values, product_metafield_columns = {}, []

        # ── Build display name map ────────────────────────────────────────────
        display_name_map = {
            ns_key: meta["name"]
            for ns_key, meta in metafield_defs.get("product", {}).items()
            if meta.get("name")
        }

        # ── Store metafield values in snapshot for sync diff ──────────────────
        for pid in snapshot:
            mf_vals = product_metafield_values.get(pid, {})
            snapshot[pid]["metafields"] = mf_vals

        # ── Apply all data to rows ────────────────────────────────────────────
        log("Applying data to rows...")

        # Build choice map: { "display_name": ["choice1", "choice2"] }
        choice_map = {}
        for ns_key, meta in metafield_defs.get("product", {}).items():
            name    = meta.get("name", ns_key)
            choices = meta.get("choices")
            if choices:
                choice_map[name] = choices

        for row in rows:
            pid = row["Product ID"]

            # Inventory
            inv_id = row.get("Inventory Item ID")
            if inv_id and inv_id in inventory_levels:
                for loc_name, qty in inventory_levels[inv_id].items():
                    row[f"Inventory Qty - {loc_name}"] = qty

            # Metafields — one column per metafield with display name
            metafield_values = product_metafield_values.get(pid, {})
            all_mf_cols = set(product_metafield_columns) | set(display_name_map.keys())

            for col in sorted(all_mf_cols):
                display_name = display_name_map.get(col, col)
                row[display_name] = metafield_values.get(col, "")

            # Combined metafields column
            row["Product Metafields"] = " | ".join(
                f"{display_name_map.get(col, col)}={metafield_values[col]}"
                for col in product_metafield_columns
                if metafield_values.get(col, "") != ""
            )

            # Collections
            if pid in product_collections:
                row["Collection Names"]      = ", ".join(product_collections[pid]["names"])
                row["Collection Handles"]    = ", ".join(product_collections[pid]["handles"])
                row["Collection Metafields"] = " | ".join(product_collections[pid]["metafields"])
            else:
                row["Collection Names"]      = ""
                row["Collection Handles"]    = ""
                row["Collection Metafields"] = ""

        log(f"✅ Full sync complete in {time.time()-t0:.1f}s")

        # Attach choice_map to snapshot for frontend use
        snapshot["__choice_map__"] = choice_map

        return rows, snapshot

    # =========================================================
    # CACHED METAFIELD DEFINITIONS
    # =========================================================

    def _fetch_metafield_defs_cached(self) -> dict:
        """Return metafield definitions from cache or fetch fresh."""
        global _DEFS_CACHE, _DEFS_CACHE_TIME
        if _DEFS_CACHE and time.time() - _DEFS_CACHE_TIME < _DEFS_CACHE_TTL:
            return _DEFS_CACHE
        try:
            result = fetch_and_store_metafield_defs(self.client)
            _DEFS_CACHE      = result
            _DEFS_CACHE_TIME = time.time()
            return result
        except Exception as e:
            print(f"[DEFS] Failed to fetch: {e}")
            return load_metafield_defs()

    # =========================================================
    # EXPORT TO EXCEL
    # =========================================================

    MAX_WIDTH_SAMPLE_ROWS = 5000

    @staticmethod
    def _flatten_metafields(edges):
        if not isinstance(edges, list):
            return []
        items = []
        for edge in edges:
            node = (edge or {}).get("node") if isinstance(edge, dict) else None
            if not isinstance(node, dict):
                continue
            ns  = str(node.get("namespace") or "").strip()
            key = str(node.get("key") or "").strip()
            if not ns or not key:
                continue
            value     = node.get("value")
            value_str = "" if value is None else str(value)
            items.append(f"{ns}.{key}={value_str}")
        return items

    @staticmethod
    def _flatten_metafield_nodes(nodes):
        if not isinstance(nodes, list):
            return []
        items = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            ns  = str(node.get("namespace") or "").strip()
            key = str(node.get("key") or "").strip()
            if not ns or not key:
                continue
            value     = node.get("value")
            value_str = "" if value is None else str(value)
            items.append(f"{ns}.{key}={value_str}")
        return items

    def export_to_excel(self, progress_callback=None) -> bytes:
        rows, snapshot = self.full_sync(progress_callback=progress_callback)

        # Remove internal key
        snapshot.pop("__choice_map__", None)

        df = pd.DataFrame.from_records(rows)

        # Detect metafield columns (contain a dot = ns.key fallback, or match display names)
        defs        = load_metafield_defs().get("product", {})
        display_names = {meta["name"] for meta in defs.values() if meta.get("name")}
        metafield_cols = sorted([
            c for c in df.columns
            if c in display_names or ("." in c and not c.startswith("Inventory"))
        ])

        priority = [
            "Image URLs", "Image Alt Text",
            "Product ID", "Handle", "Title", "Body (HTML)", "Vendor", "Type",
            "Tags", "Status",
            "SEO Title", "SEO Description",
            "Product Metafields",
            *metafield_cols,
            "Collection Metafields",
            "Variant ID", "Variant SKU", "Variant Price", "Variant Compare At Price",
            "Variant Barcode", "Variant Inventory Policy",
            "Cost per item", "Variant Grams", "Variant Weight Unit",
            "Inventory Tracked", "Requires Shipping",
            "Collection Names", "Collection Handles",
            "Created At", "Updated At", "Last Synced",
        ]

        existing_priority = [c for c in priority if c in df.columns]
        remaining         = [c for c in df.columns if c not in existing_priority]
        df = df[existing_priority + remaining]

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Products")
            ws      = writer.sheets["Products"]
            wb      = writer.book

            # Column formats
            header_fmt = wb.add_format({
                "bold": True, "bg_color": "#1a1a2e", "font_color": "#ffffff",
                "border": 1, "font_size": 10,
            })
            mf_fmt = wb.add_format({
                "bg_color": "#e8f4fd", "border": 1, "font_size": 10,
            })

            width_sample = df.head(self.MAX_WIDTH_SAMPLE_ROWS).fillna("")
            for i, col in enumerate(df.columns):
                sample_values = width_sample[col].astype(str)
                sample_max    = 0 if sample_values.empty else int(sample_values.str.len().max())
                max_len       = max(sample_max, len(str(col)))
                ws.set_column(i, i, min(max_len + 2, 60),
                              mf_fmt if col in display_names else None)
                ws.write(0, i, col, header_fmt)

        buf.seek(0)
        return buf.read()

    # =========================================================
    # BULK OPERATION
    # =========================================================

    def _start_bulk_operation(self):
        bulk_query = """
        {
          products {
            edges {
              node {
                id
                handle
                title
                descriptionHtml
                vendor
                productType
                tags
                status
                createdAt
                updatedAt
                seo { title description }
                metafields(first: 80) {
                  edges {
                    node { namespace key value type }
                  }
                }
                media(first: 50) {
                  edges {
                    node {
                      ... on MediaImage {
                        id
                        image { url altText }
                      }
                    }
                  }
                }
                variants(first: 250) {
                  edges {
                    node {
                      id sku price compareAtPrice barcode inventoryPolicy
                      inventoryItem {
                        id tracked requiresShipping
                        unitCost { amount }
                        measurement { weight { value unit } }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        mutation = """
        mutation bulkOperationRunQuery($query: String!) {
          bulkOperationRunQuery(query: $query) {
            bulkOperation { id status }
            userErrors { field message }
          }
        }
        """

        result = self.client.graphql(mutation, {"query": bulk_query})
        errors = result["bulkOperationRunQuery"]["userErrors"]
        if errors:
            raise Exception(errors)

    def _wait_for_bulk_operation(self, timeout=120, interval=2):
        """Poll with short interval for fast detection of completion."""
        query = """
        {
          currentBulkOperation {
            id status url errorCode
          }
        }
        """
        elapsed = 0
        while elapsed < timeout:
            result = self.client.graphql(query)
            op     = result.get("currentBulkOperation")
            if not op:
                raise Exception("No bulk operation found")
            if op["status"] == "COMPLETED":
                return op
            if op["status"] in ("FAILED", "CANCELED"):
                raise Exception(f"Bulk operation failed: {op['errorCode']}")
            time.sleep(interval)
            elapsed += interval
        raise Exception("Bulk operation timed out")

    # =========================================================
    # STREAM DOWNLOAD
    # =========================================================

    def _download_jsonl(self, url, filename):
        tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                for chunk in r.iter_content(4 * 1024 * 1024):  # 4MB chunks
                    if chunk:
                        tmp.write(chunk)
            tmp.close()
            return tmp.name
        except Exception:
            tmp.close()
            os.unlink(tmp.name)
            raise

    # =========================================================
    # PARSE JSONL
    # =========================================================

    def _loads_json(self, raw):
        if _orjson is not None:
            return _orjson.loads(raw)
        return json.loads(raw)

    def _parse_jsonl(self, path):
        rows                    = []
        snapshot                = {}
        products                = {}
        variants                = []
        product_images          = {}
        product_metafield_nodes = {}

        with open(path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                obj    = self._loads_json(line)
                obj_id = obj.get("id", "")

                if obj_id.startswith("gid://shopify/Product/"):
                    products[obj_id] = obj
                    snapshot[obj_id] = {"product": obj, "variants": [], "images": []}

                elif obj_id.startswith("gid://shopify/ProductVariant/"):
                    variants.append(obj)

                elif obj_id.startswith("gid://shopify/MediaImage/"):
                    parent = obj.get("__parentId")
                    img    = obj.get("image", {})
                    if parent and img.get("url"):
                        product_images.setdefault(parent, []).append(img)

                elif obj_id.startswith("gid://shopify/Metafield/"):
                    parent = obj.get("__parentId")
                    if parent:
                        product_metafield_nodes.setdefault(parent, []).append(obj)

        for pid in snapshot:
            snapshot[pid]["images"] = product_images.get(pid, [])

        for v in variants:
            parent = v.get("__parentId")
            if parent in snapshot:
                snapshot[parent]["variants"].append(v)

        now = datetime.now(timezone.utc).isoformat()

        for pid, data in snapshot.items():
            product  = data["product"]
            images   = data["images"]

            embedded_metafields = self._flatten_metafields(
                ((product.get("metafields") or {}).get("edges") or [])
            )
            linked_metafields = self._flatten_metafield_nodes(
                product_metafield_nodes.get(pid, [])
            )
            product_metafields = list(dict.fromkeys(embedded_metafields + linked_metafields))

            image_urls      = ", ".join(img["url"] for img in images)
            image_alt_texts = ", ".join(img.get("altText") or "" for img in images)

            for variant in data["variants"]:
                inventory = variant.get("inventoryItem") or {}
                weight    = (inventory.get("measurement") or {}).get("weight") or {}

                row = {
                    "Product ID":               pid,
                    "Handle":                   product.get("handle"),
                    "Title":                    product.get("title"),
                    "Body (HTML)":              product.get("descriptionHtml"),
                    "Vendor":                   product.get("vendor"),
                    "Type":                     product.get("productType"),
                    "Tags":                     ", ".join(product.get("tags", [])),
                    "Status":                   product.get("status"),
                    "Created At":               product.get("createdAt"),
                    "Updated At":               product.get("updatedAt"),
                    "SEO Title":                (product.get("seo") or {}).get("title"),
                    "SEO Description":          (product.get("seo") or {}).get("description"),
                    "Product Metafields":       " | ".join(product_metafields),
                    "Image URLs":               image_urls,
                    "Image Alt Text":           image_alt_texts,
                    "Variant ID":               variant.get("id"),
                    "Variant SKU":              variant.get("sku"),
                    "Variant Price":            variant.get("price"),
                    "Variant Compare At Price": variant.get("compareAtPrice"),
                    "Variant Barcode":          variant.get("barcode"),
                    "Variant Inventory Policy": variant.get("inventoryPolicy"),
                    "Inventory Item ID":        inventory.get("id"),
                    "Inventory Tracked":        inventory.get("tracked"),
                    "Requires Shipping":        inventory.get("requiresShipping"),
                    "Cost per item":            (inventory.get("unitCost") or {}).get("amount"),
                    "Variant Grams":            weight.get("value"),
                    "Variant Weight Unit":      weight.get("unit"),
                    "Last Synced":              now,
                }

                # Individual metafield columns by ns.key
                for mf in product_metafields:
                    if "=" in mf:
                        col_key, _, col_val = mf.partition("=")
                        row[col_key.strip()] = col_val.strip()

                rows.append(row)

        return rows, snapshot

    # =========================================================
    # LOCATIONS
    # =========================================================

    def _fetch_locations(self):
        result = self.client.graphql("""
        {
          locations(first: 50) {
            edges { node { id name } }
          }
        }
        """)
        return {
            e["node"]["id"]: e["node"]["name"]
            for e in result["locations"]["edges"]
        }

    # =========================================================
    # FAST INVENTORY — larger batches, parallel
    # =========================================================

    def _fetch_inventory_levels_fast(self, inventory_item_ids, locations):
        if not inventory_item_ids or not locations:
            return {}

        query = """
        query($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on InventoryItem {
              id
              inventoryLevels(first: 30) {
                edges {
                  node {
                    location { id }
                    quantities(names: ["available"]) { quantity }
                  }
                }
              }
            }
          }
        }
        """

        inventory_data = {}
        BATCH = 100  # increased from 50 → 100

        def fetch_batch(batch):
            result = self.client.graphql(query, {"ids": batch})
            data   = {}
            for node in result.get("nodes", []):
                if not node:
                    continue
                inv_id = node["id"]
                loc_data = {}
                for level in node["inventoryLevels"]["edges"]:
                    loc  = level["node"]["location"]["id"]
                    qty  = level["node"]["quantities"][0]["quantity"]
                    name = locations.get(loc)
                    if name:
                        loc_data[name] = qty
                data[inv_id] = loc_data
            return data

        batches = [
            inventory_item_ids[i:i + BATCH]
            for i in range(0, len(inventory_item_ids), BATCH)
        ]

        # Fetch batches in parallel (max 3 concurrent)
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(fetch_batch, b) for b in batches]
            for f in as_completed(futures):
                try:
                    inventory_data.update(f.result())
                except Exception as e:
                    print(f"[INVENTORY] Batch error: {e}")

        return inventory_data

    # =========================================================
    # METAFIELDS — parallel batches
    # =========================================================

    def _fetch_product_metafields(self, product_ids):
        query = """
        query($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Product {
              id
              metafields(first: 80) {
                edges {
                  node { namespace key value type }
                }
              }
            }
          }
        }
        """

        output      = {}
        all_columns = set()
        BATCH       = 100  # increased from 50 → 100

        def fetch_batch(batch):
            result  = self.client.graphql(query, {"ids": batch})
            data    = {}
            cols    = set()
            for node in result.get("nodes", []):
                if not node:
                    continue
                pid    = node.get("id")
                if not pid:
                    continue
                edges  = ((node.get("metafields") or {}).get("edges") or [])
                fields = {}
                for edge in edges:
                    mf_node = (edge or {}).get("node") if isinstance(edge, dict) else None
                    if not isinstance(mf_node, dict):
                        continue
                    ns  = str(mf_node.get("namespace") or "").strip()
                    key = str(mf_node.get("key") or "").strip()
                    if not ns or not key:
                        continue
                    col_name         = f"{ns}.{key}"
                    value            = mf_node.get("value")
                    fields[col_name] = "" if value is None else str(value)
                    cols.add(col_name)
                data[pid] = fields
            return data, cols

        batches = [
            product_ids[i:i + BATCH]
            for i in range(0, len(product_ids), BATCH)
        ]

        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(fetch_batch, b) for b in batches]
            for f in as_completed(futures):
                try:
                    data, cols = f.result()
                    output.update(data)
                    all_columns.update(cols)
                except Exception as e:
                    print(f"[METAFIELDS] Batch error: {e}")

        return output, sorted(all_columns)

    # =========================================================
    # COLLECTIONS
    # =========================================================

    def _fetch_collections(self):
        result = self.client.graphql("""
        {
          collections(first: 250) {
            edges {
              node {
                id title handle
                metafields(first: 20) {
                  edges {
                    node { namespace key value type }
                  }
                }
                products(first: 250) {
                  edges { node { id } }
                }
              }
            }
          }
        }
        """)

        product_collections = {}
        for edge in result["collections"]["edges"]:
            col = edge["node"]
            collection_metafields = self._flatten_metafields(
                ((col.get("metafields") or {}).get("edges") or [])
            )
            collection_metafields_text = (
                f"[{col.get('handle')}] " + " ; ".join(collection_metafields)
                if collection_metafields else ""
            )
            for p in col["products"]["edges"]:
                pid = p["node"]["id"]
                product_collections.setdefault(pid, {"names": [], "handles": [], "metafields": []})
                product_collections[pid]["names"].append(col["title"])
                product_collections[pid]["handles"].append(col["handle"])
                if collection_metafields_text:
                    product_collections[pid]["metafields"].append(collection_metafields_text)

        return product_collections