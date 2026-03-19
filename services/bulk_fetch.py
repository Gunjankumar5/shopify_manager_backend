import time
import os
import io
import tempfile
import requests
import pandas as pd
import importlib
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

_orjson = None
try:
    _orjson = importlib.import_module("orjson")
except Exception:
    _orjson = None

from routes.store_utils import get_shopify_client


class BulkFetchService:

    def __init__(self):
        self.client = get_shopify_client()

    # =========================================================
    # FULL SYNC
    # =========================================================

    def full_sync(self, progress_callback=None):

        def log(msg):
            if progress_callback:
                progress_callback(msg)
            print(msg)

        log("Starting Shopify bulk operation...")
        self._start_bulk_operation()

        log("Waiting for bulk operation to complete...")
        op = self._wait_for_bulk_operation()

        log("Downloading JSONL...")
        file_path = self._download_jsonl(op["url"], "products_bulk.jsonl")

        try:
            rows, snapshot = self._parse_jsonl(file_path)
        finally:
            os.unlink(file_path)
        log(f"Parsed {len(snapshot)} products, {len(rows)} variants.")

        log("Fetching locations...")
        locations = self._fetch_locations()

        inventory_ids = list(set(filter(None, (r["Inventory Item ID"] for r in rows))))
        product_ids   = list(set(filter(None, (r["Product ID"] for r in rows))))

        log(f"Inventory items: {len(inventory_ids)}")
        log(f"Products: {len(product_ids)}")

        with ThreadPoolExecutor(max_workers=3) as executor:
            inventory_future          = executor.submit(self._fetch_inventory_levels, inventory_ids, locations)
            collections_future        = executor.submit(self._fetch_collections)
            product_metafields_future = executor.submit(self._fetch_product_metafields, product_ids)

        inventory_levels             = inventory_future.result()
        product_collections          = collections_future.result()
        product_metafield_result     = product_metafields_future.result() or ({}, [])
        product_metafield_values, product_metafield_columns = product_metafield_result

        log("Applying inventory levels...")
        for row in rows:
            inv_id = row.get("Inventory Item ID")
            if inv_id and inv_id in inventory_levels:
                for loc_name, qty in inventory_levels[inv_id].items():
                    row[f"Inventory Qty - {loc_name}"] = qty

        log("Applying metafields and collections...")
        for row in rows:
            pid = row["Product ID"]

            # ── ONE COLUMN PER METAFIELD ──────────────────────────────────
            # Each metafield gets its own column named "Metafield: ns.key"
            # e.g. "Metafield: custom.color", "Metafield: custom.size"
            metafield_values = product_metafield_values.get(pid, {})
            for col in product_metafield_columns:
                row[col] = metafield_values.get(col, "")

            # Keep the combined column as well for backward compatibility
            combined = " | ".join(
                f"{col}={metafield_values[col]}"
                for col in product_metafield_columns
                if metafield_values.get(col, "") != ""
            )
            row["Product Metafields"] = combined

            # ── Collections ───────────────────────────────────────────────
            if pid in product_collections:
                row["Collection Names"]     = ", ".join(product_collections[pid]["names"])
                row["Collection Handles"]   = ", ".join(product_collections[pid]["handles"])
                row["Collection Metafields"] = " | ".join(product_collections[pid]["metafields"])
            else:
                row["Collection Names"]     = ""
                row["Collection Handles"]   = ""
                row["Collection Metafields"] = ""

        log("Sync complete.")
        return rows, snapshot

    # =========================================================
    # EXPORT
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

        rows, _ = self.full_sync(progress_callback=progress_callback)

        df = pd.DataFrame.from_records(rows)

        # Separate metafield columns from the rest
        metafield_cols = sorted([c for c in df.columns if "." in c])

        priority = [
            "Image URLs", "Image Alt Text",
            "Product ID", "Handle", "Title", "Body (HTML)", "Vendor", "Type",
            "Tags", "Status",
            "SEO Title", "SEO Description",
            "Product Metafields",       # combined (kept for backward compat)
            *metafield_cols,            # individual metafield columns
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
            ws = writer.sheets["Products"]

            width_sample = df.head(self.MAX_WIDTH_SAMPLE_ROWS).fillna("")
            for i, col in enumerate(df.columns):
                sample_values = width_sample[col].astype(str)
                sample_max    = 0 if sample_values.empty else int(sample_values.str.len().max())
                max_len       = max(sample_max, len(str(col)))
                ws.set_column(i, i, min(max_len + 2, 60))

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
                    node {
                      namespace
                      key
                      value
                      type
                    }
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
                      id
                      sku
                      price
                      compareAtPrice
                      barcode
                      inventoryPolicy
                      inventoryItem {
                        id
                        tracked
                        requiresShipping
                        unitCost { amount }
                        measurement {
                          weight { value unit }
                        }
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

    def _wait_for_bulk_operation(self, timeout=600, interval=3):

        query = """
        {
          currentBulkOperation {
            id
            status
            url
            errorCode
          }
        }
        """

        elapsed = 0
        while elapsed < timeout:
            result = self.client.graphql(query)
            op = result.get("currentBulkOperation")
            if not op:
                raise Exception("No bulk operation found")
            if op["status"] == "COMPLETED":
                return op
            if op["status"] in ("FAILED", "CANCELED"):
                raise Exception(f"Bulk operation failed: {op['errorCode']}")
            time.sleep(interval)
            elapsed += interval

        raise Exception("Bulk operation timed out after 10 minutes")

    # =========================================================
    # STREAM DOWNLOAD
    # =========================================================

    def _download_jsonl(self, url, filename):

        tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                for chunk in r.iter_content(1024 * 1024):
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

        rows     = []
        snapshot = {}

        products              = {}
        variants              = []
        product_images        = {}
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
                    if parent and (
                        obj.get("namespace") is not None
                        or obj.get("key")       is not None
                        or obj.get("value")     is not None
                    ):
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
                    "Product ID":                pid,
                    "Handle":                    product.get("handle"),
                    "Title":                     product.get("title"),
                    "Body (HTML)":               product.get("descriptionHtml"),
                    "Vendor":                    product.get("vendor"),
                    "Type":                      product.get("productType"),
                    "Tags":                      ", ".join(product.get("tags", [])),
                    "Status":                    product.get("status"),
                    "Created At":                product.get("createdAt"),
                    "Updated At":                product.get("updatedAt"),
                    "SEO Title":                 (product.get("seo") or {}).get("title"),
                    "SEO Description":           (product.get("seo") or {}).get("description"),
                    # Combined metafields column (all in one)
                    "Product Metafields":        " | ".join(product_metafields),
                    "Image URLs":                image_urls,
                    "Image Alt Text":            image_alt_texts,
                    "Variant ID":                variant.get("id"),
                    "Variant SKU":               variant.get("sku"),
                    "Variant Price":             variant.get("price"),
                    "Variant Compare At Price":  variant.get("compareAtPrice"),
                    "Variant Barcode":           variant.get("barcode"),
                    "Variant Inventory Policy":  variant.get("inventoryPolicy"),
                    "Inventory Item ID":         inventory.get("id"),
                    "Inventory Tracked":         inventory.get("tracked"),
                    "Requires Shipping":         inventory.get("requiresShipping"),
                    "Cost per item":             (inventory.get("unitCost") or {}).get("amount"),
                    "Variant Grams":             weight.get("value"),
                    "Variant Weight Unit":       weight.get("unit"),
                    "Last Synced":               now,
                }

                # ── Individual metafield columns ──────────────────────────
                # Parse "ns.key=value" strings into separate columns
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
            edges {
              node { id name }
            }
          }
        }
        """)

        return {
            e["node"]["id"]: e["node"]["name"]
            for e in result["locations"]["edges"]
        }

    # =========================================================
    # INVENTORY
    # =========================================================

    def _fetch_inventory_levels(self, inventory_item_ids, locations):

        query = """
        query($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on InventoryItem {
              id
              inventoryLevels(first: 20) {
                edges {
                  node {
                    location { id }
                    quantities(names: ["available"]) {
                      quantity
                    }
                  }
                }
              }
            }
          }
        }
        """

        inventory_data = {}
        BATCH = 50

        for i in range(0, len(inventory_item_ids), BATCH):
            batch  = inventory_item_ids[i:i + BATCH]
            result = self.client.graphql(query, {"ids": batch})

            for node in result["nodes"]:
                if not node:
                    continue
                inv_id = node["id"]
                data   = {}
                for level in node["inventoryLevels"]["edges"]:
                    loc  = level["node"]["location"]["id"]
                    qty  = level["node"]["quantities"][0]["quantity"]
                    name = locations.get(loc)
                    if name:
                        data[name] = qty
                inventory_data[inv_id] = data

        return inventory_data

    # =========================================================
    # METAFIELDS
    # =========================================================

    def _fetch_product_metafields(self, product_ids):

        query = """
        query($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Product {
              id
              metafields(first: 80) {
                edges {
                  node {
                    namespace
                    key
                    value
                    type
                  }
                }
              }
            }
          }
        }
        """

        output      = {}
        all_columns = set()
        batch_size  = 50

        for i in range(0, len(product_ids), batch_size):
            batch  = product_ids[i:i + batch_size]
            result = self.client.graphql(query, {"ids": batch})

            for node in result.get("nodes", []):
                if not node:
                    continue
                pid = node.get("id")
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
                    col_name        = f"{ns}.{key}"
                    value           = mf_node.get("value")
                    fields[col_name] = "" if value is None else str(value)
                    all_columns.add(col_name)
                output[pid] = fields

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
                id
                title
                handle
                metafields(first: 80) {
                  edges {
                    node {
                      namespace
                      key
                      value
                      type
                    }
                  }
                }
                products(first: 250) {
                  edges {
                    node { id }
                  }
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