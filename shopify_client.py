import time
import requests
from typing import Any, Callable, Dict, Optional


class ShopifyClient:
    def __init__(self, shop_name=None, access_token=None, api_version="2026-01", token_refresh_callback: Optional[Callable[[], Optional[str]]] = None):
        """Initialize Shopify client.
        
        Args:
            shop_name: Store name (e.g., 'mystore.myshopify.com' or 'mystore')
            access_token: Access token for API authentication
            api_version: Shopify API version (default: 2026-01)
        """
        self.shop_name = shop_name
        self.access_token = access_token
        self.api_version = api_version
        self.token_refresh_callback = token_refresh_callback

        if not self.shop_name:
            raise ValueError("Missing shop_name. Connect a Shopify store before creating the client.")

        if not self.access_token:
            raise ValueError("Missing access token. Reconnect the Shopify store and try again.")

        if not self.shop_name.endswith(".myshopify.com"):
            self.shop_name = f"{self.shop_name}.myshopify.com"

    def _refresh_access_token(self) -> bool:
        if not self.token_refresh_callback:
            return False
        try:
            refreshed_token = self.token_refresh_callback()
        except Exception:
            return False
        if not refreshed_token:
            return False
        self.access_token = refreshed_token
        return True

    def _request(self, method: str, url: str, retry_on_unauthorized: bool = True, **kwargs):
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
            **(kwargs.pop("headers", {}) or {}),
        }
        response = requests.request(method, url, headers=headers, **kwargs)
        if response.status_code == 401 and retry_on_unauthorized and self._refresh_access_token():
            headers["X-Shopify-Access-Token"] = self.access_token
            response = requests.request(method, url, headers=headers, **kwargs)
        return response

    # =========================================================
    # GRAPHQL
    # =========================================================

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a Shopify Admin GraphQL query or mutation.
        Returns the contents of the top-level `data` key.
        Raises on HTTP errors and on GraphQL-level errors.
        """
        url = f"{self._get_base_url()}/graphql.json"

        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        r = self._request("POST", url, json=payload, timeout=60)

        r.raise_for_status()

        result = r.json()

        if "errors" in result:
            raise Exception(result["errors"])

        return result.get("data", {})

    # =========================================================
    # PRODUCTS (REST)
    # =========================================================
    # =========================================================
    # PRODUCTS (REST)
    # =========================================================
    def _get_base_url(self):
        """Get the base URL for API calls"""
        return f"https://{self.shop_name}/admin/api/{self.api_version}"

    def get_products(self, limit=None, status=None, title=None, fetch_all=False):
        """Fetch products using Shopify cursor pagination.

        fetch_all=False  → first page only (250 items, very fast).
        fetch_all=True   → all pages via Link: next cursor.
        limit            → cap final result count (independent of pagination).
        """
        import logging as _log

        page_size = 250  # Shopify hard max per request
        all_products = []

        params = {
            "limit": page_size,
            "fields": "id,title,vendor,status,handle,image,images,variants",
        }
        if status and status != "any":
            params["status"] = status
        if title:
            params["title"] = title

        next_url = f"{self._get_base_url()}/products.json"
        page_num = 0

        try:
            while next_url:
                page_num += 1
                _log.info(f"[ShopifyClient] Page {page_num}: fetching products…")

                # Small delay between pages to respect Shopify's rate limit (2 req/s)
                if page_num > 1:
                    time.sleep(0.6)

                retries = 0
                while True:
                    r = self._request(
                        "GET",
                        next_url,
                        params=params if page_num == 1 else None,
                        timeout=20,
                    )
                    if r.status_code == 429 and retries < 5:
                        wait = float(r.headers.get("Retry-After", 2))
                        _log.warning(f"[ShopifyClient] 429 rate limited — waiting {wait}s (retry {retries+1})")
                        time.sleep(wait)
                        retries += 1
                        continue
                    break

                r.raise_for_status()
                batch = r.json().get("products", [])
                all_products.extend(batch)
                _log.info(f"[ShopifyClient] Page {page_num}: +{len(batch)} → total {len(all_products)}")

                # Stop early if last page or fetch_all disabled
                if not batch or not fetch_all:
                    break

                # Shopify cursor: follow Link: rel="next" header
                next_url = r.links.get("next", {}).get("url")

                # Stop if explicit limit reached
                if limit and len(all_products) >= limit:
                    all_products = all_products[:limit]
                    break

            if limit:
                all_products = all_products[:limit]

            _log.info(f"[ShopifyClient] ✅ Done: {len(all_products)} products (pages={page_num})")
            return {"products": all_products}

        except requests.exceptions.Timeout:
            _log.error(f"[ShopifyClient] ❌ Timeout after page {page_num} — returning {len(all_products)} so far")
            return {"products": all_products}
        except requests.exceptions.HTTPError as e:
            _log.error(f"[ShopifyClient] ❌ HTTP {e.response.status_code}: {e.response.text[:200]}")
            return {"products": all_products}
        except Exception as e:
            _log.error(f"[ShopifyClient] ❌ Error: {e}")
            return {"products": all_products}

    def search_products(self, query):
        r = self._request(
            "GET",
            f"{self._get_base_url()}/products.json",
            params={"title": query},
        )
        r.raise_for_status()
        return r.json().get("products", [])

    def get_product(self, product_id):
        r = self._request("GET", f"{self._get_base_url()}/products/{product_id}.json")
        r.raise_for_status()
        return r.json()

    def get_product_seo(self, product_id):
        product_gid = f"gid://shopify/Product/{product_id}"
        data = self.graphql(
            """
            query ProductSeo($id: ID!) {
              product(id: $id) {
                titleTag: metafield(namespace: \"global\", key: \"title_tag\") {
                  value
                }
                descriptionTag: metafield(namespace: \"global\", key: \"description_tag\") {
                  value
                }
              }
            }
            """,
            {"id": product_gid},
        )
        product = data.get("product") or {}
        return {
            "title": ((product.get("titleTag") or {}).get("value") or "").strip(),
            "description": ((product.get("descriptionTag") or {}).get("value") or "").strip(),
        }

    def set_product_seo(self, product_id, title=None, description=None):
        product_gid = f"gid://shopify/Product/{product_id}"
        metafields = []
        deletions = []

        def prepare_value(value):
            if value is None:
                return None
            return str(value).strip()

        title_value = prepare_value(title)
        if title is not None:
            if title_value:
                metafields.append(
                    {
                        "ownerId": product_gid,
                        "namespace": "global",
                        "key": "title_tag",
                        "type": "single_line_text_field",
                        "value": title_value,
                    }
                )
            else:
                deletions.append(
                    {
                        "ownerId": product_gid,
                        "namespace": "global",
                        "key": "title_tag",
                    }
                )

        description_value = prepare_value(description)
        if description is not None:
            if description_value:
                metafields.append(
                    {
                        "ownerId": product_gid,
                        "namespace": "global",
                        "key": "description_tag",
                        "type": "multi_line_text_field",
                        "value": description_value,
                    }
                )
            else:
                deletions.append(
                    {
                        "ownerId": product_gid,
                        "namespace": "global",
                        "key": "description_tag",
                    }
                )

        result = {"metafields": [], "deletedMetafields": []}

        if metafields:
            data = self.graphql(
                """
                mutation SetProductSeo($metafields: [MetafieldsSetInput!]!) {
                  metafieldsSet(metafields: $metafields) {
                    metafields {
                      id
                      namespace
                      key
                      value
                    }
                    userErrors {
                      field
                      message
                      code
                    }
                  }
                }
                """,
                {"metafields": metafields},
            )
            set_result = data.get("metafieldsSet") or {}
            user_errors = set_result.get("userErrors") or []
            if user_errors:
                raise Exception(user_errors)
            result["metafields"] = set_result.get("metafields") or []

        if deletions:
            data = self.graphql(
                """
                mutation DeleteProductSeo($metafields: [MetafieldIdentifierInput!]!) {
                  metafieldsDelete(metafields: $metafields) {
                    deletedMetafields {
                      key
                      namespace
                      ownerId
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
                """,
                {"metafields": deletions},
            )
            delete_result = data.get("metafieldsDelete") or {}
            user_errors = delete_result.get("userErrors") or []
            if user_errors:
                raise Exception(user_errors)
            result["deletedMetafields"] = delete_result.get("deletedMetafields") or []

        return result

    def create_product(self, data):
        r = self._request(
            "POST",
            f"{self._get_base_url()}/products.json",
            json={"product": data},
        )
        r.raise_for_status()
        return r.json()

    def update_product(self, product_id, data):
        r = self._request(
            "PUT",
            f"{self._get_base_url()}/products/{product_id}.json",
            json={"product": data},
        )
        r.raise_for_status()
        return r.json()

    def delete_product(self, product_id):
        r = self._request("DELETE", f"{self._get_base_url()}/products/{product_id}.json")
        r.raise_for_status()
        return {"deleted": True}

    def update_product_variant(self, product_id, variant_id, data):
        r = self._request(
            "PUT",
            f"{self._get_base_url()}/variants/{variant_id}.json",
            json={"variant": data},
        )
        r.raise_for_status()
        return r.json()

    def count_products(self):
        r = self._request("GET", f"{self._get_base_url()}/products/count.json")
        r.raise_for_status()
        return r.json()

    # =========================================================
    # COLLECTIONS (REST)
    # =========================================================

    def get_custom_collections(self, limit=50):
        r = self._request(
            "GET",
            f"{self._get_base_url()}/custom_collections.json",
            params={"limit": limit},
        )
        r.raise_for_status()
        return r.json()

    def get_collections(self, limit=50):
        custom = self._request(
            "GET",
            f"{self._get_base_url()}/custom_collections.json",
            params={"limit": limit},
        )
        custom.raise_for_status()

        smart = self._request(
            "GET",
            f"{self._get_base_url()}/smart_collections.json",
            params={"limit": limit},
        )
        smart.raise_for_status()

        custom_collections = custom.json().get("custom_collections", [])
        smart_collections = smart.json().get("smart_collections", [])

        for collection in custom_collections:
            collection["collection_type"] = "custom"
        for collection in smart_collections:
            collection["collection_type"] = "smart"

        # Keep backward-compatible response key expected by frontend.
        return {"custom_collections": custom_collections + smart_collections}

    def get_collection(self, collection_id):
        r = self._request(
            "GET",
            f"{self._get_base_url()}/custom_collections/{collection_id}.json",
        )
        r.raise_for_status()
        return r.json()

    def create_collection(self, data):
        r = self._request(
            "POST",
            f"{self._get_base_url()}/custom_collections.json",
            json={"custom_collection": data},
        )
        r.raise_for_status()
        return r.json()

    def update_collection(self, collection_id, data):
        r = self._request(
            "PUT",
            f"{self._get_base_url()}/custom_collections/{collection_id}.json",
            json={"custom_collection": {"id": collection_id, **data}},
        )
        r.raise_for_status()
        return r.json()

    def add_products_to_collection(self, collection_id, product_ids):
        collects = []
        for product_id in product_ids:
            r = self._request(
                "POST",
                f"{self._get_base_url()}/collects.json",
                json={"collect": {"product_id": product_id, "collection_id": collection_id}},
            )
            r.raise_for_status()
            collects.append(r.json().get("collect"))
        return {"collects": collects}

    def get_collects(self, product_id=None, collection_id=None, limit=250):
        params = {"limit": limit}
        if product_id is not None:
            params["product_id"] = product_id
        if collection_id is not None:
            params["collection_id"] = collection_id

        r = self._request(
            "GET",
            f"{self._get_base_url()}/collects.json",
            params=params,
        )
        r.raise_for_status()
        return r.json()

    def get_product_collection_ids(self, product_id):
        collects = self.get_collects(product_id=product_id).get("collects", [])
        return [collect.get("collection_id") for collect in collects if collect.get("collection_id")]

    def delete_collect(self, collect_id):
        r = self._request(
            "DELETE",
            f"{self._get_base_url()}/collects/{collect_id}.json",
        )
        r.raise_for_status()
        return {"deleted": True}

    def sync_product_collections(self, product_id, collection_ids):
        target_ids = {int(cid) for cid in collection_ids if cid not in (None, "")}
        existing_collects = self.get_collects(product_id=product_id).get("collects", [])
        existing_by_collection = {
            int(collect["collection_id"]): collect
            for collect in existing_collects
            if collect.get("collection_id")
        }

        for collection_id, collect in existing_by_collection.items():
            if collection_id not in target_ids:
                self.delete_collect(collect["id"])

        for collection_id in sorted(target_ids):
            if collection_id in existing_by_collection:
                continue
            self._request(
                "POST",
                f"{self._get_base_url()}/collects.json",
                json={"collect": {"product_id": product_id, "collection_id": collection_id}},
            ).raise_for_status()

        return {"collection_ids": sorted(target_ids)}

    def delete_collection(self, collection_id):
        r = self._request(
            "DELETE",
            f"{self._get_base_url()}/custom_collections/{collection_id}.json",
        )
        r.raise_for_status()
        return {"deleted": True}

    def create_custom_collection(self, title, image_url=None, product_ids=None):
        data = {"custom_collection": {"title": title}}
        if image_url:
            data["custom_collection"]["image"] = {"src": image_url}

        r = self._request("POST", f"{self._get_base_url()}/custom_collections.json", json=data)
        r.raise_for_status()
        result = r.json()

        if product_ids and result.get("custom_collection"):
            col_id = result["custom_collection"]["id"]
            for pid in product_ids:
                self._request(
                    "POST",
                    f"{self._get_base_url()}/collects.json",
                    json={"collect": {"product_id": pid, "collection_id": col_id}},
                )
        return result

    # =========================================================
    # INVENTORY (REST)
    # =========================================================

    def get_locations(self):
        r = self._request("GET", f"{self._get_base_url()}/locations.json")
        r.raise_for_status()
        return r.json()

    def _build_variant_index(self):
        """Build inventory_item_id → product/variant metadata index.

        Uses a lightweight fields-only product request to keep this fast.
        Only fetches id, title, variants (inventory_item_id, variant info).
        """
        import logging as _log
        variant_index = {}
        next_url = f"{self._get_base_url()}/products.json"
        params = {
            "limit": 250,
            "status": "active",
            "fields": "id,title,variants",
        }
        page = 0
        try:
            while next_url:
                page += 1
                r = self._request(
                    "GET",
                    next_url,
                    params=params if page == 1 else None,
                    timeout=20,
                )
                r.raise_for_status()
                for product in r.json().get("products", []):
                    for v in product.get("variants", []):
                        iid = v.get("inventory_item_id")
                        if iid:
                            variant_index[iid] = {
                                "product_id":    product.get("id"),
                                "product_title": product.get("title"),
                                "variant_id":    v.get("id"),
                                "variant_title": v.get("title"),
                            }
                next_url = r.links.get("next", {}).get("url")
        except Exception as e:
            _log.warning(f"[ShopifyClient] variant index build failed: {e}")
        return variant_index

    def _enrich_inventory_levels(self, inventory_levels, variant_index=None):
        """Attach product metadata to each inventory row for UI display."""
        if variant_index is None:
            variant_index = self._build_variant_index()
        enriched = []
        for level in inventory_levels:
            item_id = level.get("inventory_item_id")
            metadata = variant_index.get(item_id, {})
            if metadata:
                enriched.append({**level, **metadata})
        return enriched

    def get_inventory_levels(self, location_ids=None):
        import logging as _log

        params: Dict[str, Any] = {"limit": 250}
        if location_ids:
            params["location_ids"] = ",".join(str(x) for x in location_ids) if isinstance(location_ids, list) else str(location_ids)
        else:
            try:
                locs = self.get_locations().get("locations", [])
                ids = [str(loc["id"]) for loc in locs if loc.get("id")]
                if ids:
                    params["location_ids"] = ",".join(ids)
            except Exception:
                pass

        # Build variant index once — reused for enrichment
        variant_index = self._build_variant_index()

        all_levels = []
        next_url = f"{self._get_base_url()}/inventory_levels.json"
        first = True

        while next_url:
            try:
                r = self._request(
                    "GET",
                    next_url,
                    params=params if first else None,
                    timeout=20,
                )
                r.raise_for_status()
            except requests.HTTPError:
                # Fallback: derive inventory from variant data already in index
                _log.warning("[ShopifyClient] inventory_levels API failed, falling back to variant quantities")
                fallback = []
                try:
                    locs = self.get_locations().get("locations", [])
                    default_loc = locs[0]["id"] if locs else None
                except Exception:
                    default_loc = None
                # Re-fetch variants with quantity since index only has metadata fields
                try:
                    next_p = f"{self._get_base_url()}/products.json"
                    p1 = {"limit": 250, "status": "active", "fields": "id,variants"}
                    pg = 0
                    while next_p:
                        pg += 1
                        rp = self._request("GET", next_p, params=p1 if pg == 1 else None, timeout=20)
                        rp.raise_for_status()
                        for prod in rp.json().get("products", []):
                            for v in prod.get("variants", []):
                                iid = v.get("inventory_item_id")
                                qty = v.get("inventory_quantity")
                                if iid is not None and qty is not None:
                                    fallback.append({"inventory_item_id": iid, "location_id": default_loc, "available": qty})
                        next_p = rp.links.get("next", {}).get("url")
                except Exception as fe:
                    _log.error(f"[ShopifyClient] fallback failed: {fe}")
                return {"inventory_levels": self._enrich_inventory_levels(fallback, variant_index)}

            batch = r.json().get("inventory_levels", [])
            all_levels.extend(batch)
            _log.info(f"[ShopifyClient] inventory page: +{len(batch)} → total {len(all_levels)}")
            next_url = r.links.get("next", {}).get("url")
            first = False

        return {"inventory_levels": self._enrich_inventory_levels(all_levels, variant_index)}

    def update_inventory(self, inventory_item_id, location_id, available):
        r = self._request(
            "POST",
            f"{self._get_base_url()}/inventory_levels/set.json",
            json={
                "location_id": location_id,
                "inventory_item_id": inventory_item_id,
                "available": available,
            },
        )
        r.raise_for_status()
        return r.json()

    def get_inventory_item(self, inventory_item_id):
        r = self._request(
            "GET",
            f"{self._get_base_url()}/inventory_items/{inventory_item_id}.json",
        )
        r.raise_for_status()
        return r.json()

    def update_inventory_item(self, inventory_item_id, data):
        r = self._request(
            "PUT",
            f"{self._get_base_url()}/inventory_items/{inventory_item_id}.json",
            json={"inventory_item": {"id": inventory_item_id, **data}},
        )
        r.raise_for_status()
        return r.json()