import time
import random
import logging
import requests
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urlparse
import re


class ShopifyClient:
    def __init__(self, shop_name=None, access_token=None, api_version="2026-01", token_refresh_callback: Optional[Callable[[], Optional[str]]] = None, max_retries: int = 5, backoff_factor: float = 0.6):
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
        # HTTP session for connection pooling and performance
        self.session = requests.Session()
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)
        self.logger = logging.getLogger("shopify_client")

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
        """Perform an HTTP request with retries and backoff.

        Preserves the same signature as before. Retries on 429 and 5xx responses,
        and will attempt a single token refresh on 401 if a callback is provided.
        """
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
            **(kwargs.pop("headers", {}) or {}),
        }

        attempt = 0
        last_exc = None
        while True:
            attempt += 1
            try:
                resp = self.session.request(method, url, headers=headers, **kwargs)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt > self.max_retries:
                    self.logger.exception("Request failed after retries: %s %s", method, url)
                    raise
                sleep = self.backoff_factor * (2 ** (attempt - 1)) * (0.8 + random.random() * 0.4)
                time.sleep(sleep)
                continue

            # Unauthorized: try refreshing token once and retry immediately.
            if resp.status_code == 401 and retry_on_unauthorized and self._refresh_access_token():
                headers["X-Shopify-Access-Token"] = self.access_token
                try:
                    resp = self.session.request(method, url, headers=headers, **kwargs)
                except requests.RequestException as exc:
                    last_exc = exc
                    if attempt > self.max_retries:
                        raise
                    sleep = self.backoff_factor * (2 ** (attempt - 1))
                    time.sleep(sleep)
                    continue

            # Rate limited: respect Retry-After when present, otherwise exponential backoff
            if resp.status_code == 429 and attempt <= self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after is not None else None
                except Exception:
                    wait = None
                if wait is None:
                    wait = self.backoff_factor * (2 ** (attempt - 1))
                self.logger.warning("429 rate limited — waiting %.2fs (attempt %d) for %s", wait, attempt, url)
                time.sleep(wait)
                continue

            # Server error: retry a few times
            if 500 <= resp.status_code < 600 and attempt <= self.max_retries:
                sleep = self.backoff_factor * (2 ** (attempt - 1))
                self.logger.warning("Server error %s — retrying after %.2fs (attempt %d)", resp.status_code, sleep, attempt)
                time.sleep(sleep)
                continue

            return resp

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

    def _extract_next_page_info(self, response: requests.Response) -> Optional[str]:
        """Extract cursor from Shopify Link header with a robust fallback parser."""
        next_url = response.links.get("next", {}).get("url")
        if not next_url:
            link_header = response.headers.get("Link", "")
            if link_header:
                match = re.search(r'<([^>]+)>\s*;\s*rel="next"', link_header)
                if match:
                    next_url = match.group(1)

        if not next_url:
            return None

        parsed = urlparse(next_url)
        return parse_qs(parsed.query).get("page_info", [None])[0]

    def get_products(self, limit=None, status=None, title=None, fetch_all=False, page_info=None):
        """Fetch products using Shopify cursor pagination.

        fetch_all=False  → first page only (250 items, very fast).
        fetch_all=True   → all pages via Link: next cursor.
        limit            → cap final result count (independent of pagination).
        """
        import logging as _log

        page_size = 250  # Shopify hard max per request
        all_products = []

        requested_limit = None
        try:
            if limit is not None:
                requested_limit = max(1, min(int(limit), page_size))
        except Exception:
            requested_limit = None

        params = {
            "limit": requested_limit or page_size,
        }
        if status and status != "any":
            params["status"] = status
        if page_info:
            params["page_info"] = page_info

        search_term = (title or "").strip().lower()
        scan_across_pages = fetch_all or bool(search_term)

        next_url = f"{self._get_base_url()}/products.json"
        page_num = 0
        next_page_info = None

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
                if search_term:
                    filtered_batch = []
                    for product in batch:
                        p_title = str(product.get("title") or "").lower()
                        p_vendor = str(product.get("vendor") or "").lower()
                        p_handle = str(product.get("handle") or "").lower()
                        if search_term in p_title or search_term in p_vendor or search_term in p_handle:
                            filtered_batch.append(product)
                else:
                    filtered_batch = batch

                all_products.extend(filtered_batch)
                _log.info(
                    f"[ShopifyClient] Page {page_num}: matched +{len(filtered_batch)} (raw {len(batch)}) → total {len(all_products)}"
                )

                next_page_info = self._extract_next_page_info(r)
                next_url = r.links.get("next", {}).get("url")
                if not next_url and next_page_info:
                    # When response.links is empty but Link header exists, keep looping via cursor.
                    next_url = f"{self._get_base_url()}/products.json?page_info={next_page_info}&limit={params['limit']}"
                    if status and status != "any":
                        next_url += f"&status={status}"
                    if title:
                        next_url += f"&title={title}"

                # Stop early if there are no more source products.
                if not batch:
                    break

                # For normal browsing (no search, no fetch_all), only fetch one page.
                if not scan_across_pages:
                    break

                # Shopify cursor: follow Link: rel="next" header
                # next_url already resolved above.

                # Stop if explicit limit reached
                if limit and len(all_products) >= limit:
                    all_products = all_products[:limit]
                    break

            if limit:
                all_products = all_products[:limit]

            _log.info(f"[ShopifyClient] ✅ Done: {len(all_products)} products (pages={page_num})")
            return {
                "products": all_products,
                "next_page_info": next_page_info,
                "has_next_page": bool(next_page_info),
            }

        except requests.exceptions.Timeout:
            _log.error(f"[ShopifyClient] ❌ Timeout after page {page_num} — returning {len(all_products)} so far")
            return {
                "products": all_products,
                "next_page_info": next_page_info,
                "has_next_page": bool(next_page_info),
                "partial": True,
                "error": "Timeout while fetching products from Shopify",
            }
        except requests.exceptions.HTTPError as e:
            _log.error(f"[ShopifyClient] ❌ HTTP {e.response.status_code}: {e.response.text[:200]}")
            return {
                "products": all_products,
                "next_page_info": next_page_info,
                "has_next_page": bool(next_page_info),
                "partial": True,
                "error": f"Shopify HTTP error {e.response.status_code}",
            }
        except Exception as e:
            _log.error(f"[ShopifyClient] ❌ Error: {e}")
            return {
                "products": all_products,
                "next_page_info": next_page_info,
                "has_next_page": bool(next_page_info),
                "partial": True,
                "error": str(e),
            }

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

    def _normalize_collection_response(self, payload, collection_type):
        key = "smart_collection" if collection_type == "smart" else "custom_collection"
        collection = payload.get(key, {})
        if collection:
            collection["collection_type"] = collection_type
        return payload

    def _get_collection_products(self, collection_id, limit=250):
        r = self._request(
            "GET",
            f"{self._get_base_url()}/products.json",
            params={
                "collection_id": collection_id,
                "limit": limit,
                "fields": "id,title,status,images",
            },
        )
        r.raise_for_status()
        products = r.json().get("products", [])
        return [
            {
                "id": product.get("id"),
                "title": product.get("title"),
                "status": product.get("status"),
                "image": (product.get("images") or [{}])[0].get("src"),
            }
            for product in products
        ]

    def _collection_payload(self, data, collection_type, include_id=None):
        allowed_keys = {
            "title",
            "body_html",
            "image",
            "published",
            "published_at",
            "published_scope",
            "sort_order",
            "template_suffix",
            "handle",
        }
        if collection_type == "smart":
            allowed_keys.update({"rules", "disjunctive"})

        payload = {key: value for key, value in data.items() if key in allowed_keys}
        if include_id is not None:
            payload["id"] = include_id
        return payload

    def get_collection(self, collection_id):
        custom = self._request(
            "GET",
            f"{self._get_base_url()}/custom_collections/{collection_id}.json",
        )
        if custom.ok:
            data = self._normalize_collection_response(custom.json(), "custom")
            data["products"] = self._get_collection_products(collection_id)
            return data
        if custom.status_code != 404:
            custom.raise_for_status()

        smart = self._request(
            "GET",
            f"{self._get_base_url()}/smart_collections/{collection_id}.json",
        )
        smart.raise_for_status()
        data = self._normalize_collection_response(smart.json(), "smart")
        data["products"] = self._get_collection_products(collection_id)
        smart_collection = data.get("smart_collection", {})
        data["rules"] = smart_collection.get("rules", [])
        data["disjunctive"] = smart_collection.get("disjunctive", False)
        return data

    def create_collection(self, data):
        collection_type = data.get("collection_type", "custom")
        endpoint = "smart_collections" if collection_type == "smart" else "custom_collections"
        key = "smart_collection" if collection_type == "smart" else "custom_collection"
        payload = self._collection_payload(data, collection_type)
        r = self._request(
            "POST",
            f"{self._get_base_url()}/{endpoint}.json",
            json={key: payload},
        )
        r.raise_for_status()
        return self._normalize_collection_response(r.json(), collection_type)

    def _convert_collection(self, collection_id, current_type, target_type, data):
        existing = self.get_collection(collection_id)
        existing_collection = existing.get(
            "smart_collection" if current_type == "smart" else "custom_collection",
            {},
        )
        create_payload = {
            **existing_collection,
            **data,
            "collection_type": target_type,
        }
        create_payload.pop("id", None)
        create_payload.pop("admin_graphql_api_id", None)
        create_payload.pop("updated_at", None)
        create_payload.pop("products_count", None)

        created = self.create_collection(create_payload)

        if target_type == "custom":
            new_collection = created.get("custom_collection", {})
            product_ids = [product.get("id") for product in existing.get("products", []) if product.get("id")]
            if new_collection.get("id") and product_ids:
                self.add_products_to_collection(new_collection["id"], product_ids)

        self.delete_collection(collection_id)
        return created

    def update_collection(self, collection_id, data):
        requested_type = data.get("collection_type")
        current = self.get_collection(collection_id)
        current_type = "smart" if current.get("smart_collection") else "custom"

        if requested_type and requested_type != current_type:
            return self._convert_collection(collection_id, current_type, requested_type, data)

        collection_type = requested_type or current_type
        endpoint = "smart_collections" if collection_type == "smart" else "custom_collections"
        key = "smart_collection" if collection_type == "smart" else "custom_collection"
        payload = self._collection_payload(data, collection_type, include_id=collection_id)
        custom = self._request(
            "PUT",
            f"{self._get_base_url()}/{endpoint}/{collection_id}.json",
            json={key: payload},
        )
        if custom.ok:
            return self._normalize_collection_response(custom.json(), collection_type)
        if custom.status_code != 404:
            custom.raise_for_status()

        smart = self._request(
            "PUT",
            f"{self._get_base_url()}/smart_collections/{collection_id}.json",
            json={"smart_collection": self._collection_payload(data, "smart", include_id=collection_id)},
        )
        smart.raise_for_status()
        return self._normalize_collection_response(smart.json(), "smart")

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

    def sync_collection_products(self, collection_id, product_ids):
        target_ids = {int(product_id) for product_id in product_ids if product_id not in (None, "")}
        existing_collects = self.get_collects(collection_id=collection_id).get("collects", [])
        existing_by_product = {
            int(collect["product_id"]): collect
            for collect in existing_collects
            if collect.get("product_id")
        }

        for product_id, collect in existing_by_product.items():
            if product_id not in target_ids:
                self.delete_collect(collect["id"])

        for product_id in sorted(target_ids):
            if product_id in existing_by_product:
                continue
            self._request(
                "POST",
                f"{self._get_base_url()}/collects.json",
                json={"collect": {"product_id": product_id, "collection_id": collection_id}},
            ).raise_for_status()

        return {"product_ids": sorted(target_ids)}

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
        custom = self._request(
            "DELETE",
            f"{self._get_base_url()}/custom_collections/{collection_id}.json",
        )
        if custom.ok:
            return {"deleted": True}
        if custom.status_code != 404:
            custom.raise_for_status()

        smart = self._request(
            "DELETE",
            f"{self._get_base_url()}/smart_collections/{collection_id}.json",
        )
        smart.raise_for_status()
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
    
    # =========================================================
    # METAFIELDS (REST)
    # =========================================================

    def get_metafields(self, resource: str, resource_id: int) -> dict:
        all_metafields = []
        url = f"{self._get_base_url()}/{resource}/{resource_id}/metafields.json"
        params = {"limit": 250}
        first = True

        while url:
            r = self._request("GET", url, params=params if first else {}, timeout=30)
            r.raise_for_status()
            all_metafields.extend(r.json().get("metafields", []))
            url = r.links.get("next", {}).get("url")
            first = False

        return {"metafields": all_metafields}

    def create_metafield(self, resource: str, resource_id: int, data: dict) -> dict:
        import logging as _log
        logger = _log.getLogger(__name__)
        logger.info(f"Creating metafield for {resource}/{resource_id}: {data}")
        
        # The data dict should contain: namespace, key, type, value
        # Shopify REST API requires this specific format
        payload = {
            "metafield": {
                "namespace": data.get("namespace", "custom"),
                "key": data.get("key"),
                "type": data.get("type"),
                "value": data.get("value", ""),
            }
        }
        logger.info(f"Payload being sent to Shopify: {payload}")
        
        try:
            r = self._request(
                "POST",
                f"{self._get_base_url()}/{resource}/{resource_id}/metafields.json",
                json=payload,
                timeout=30,
            )
            if not r.ok:
                error_body = r.text
                logger.error(f"Shopify error ({r.status_code}): {error_body}")
                try:
                    error_json = r.json()
                    raise Exception(f"Shopify {r.status_code}: {error_json}")
                except:
                    raise Exception(f"Shopify {r.status_code}: {error_body}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Error creating metafield: {e}")
            raise

    def update_metafield(self, metafield_id: int, data: dict) -> dict:
        r = self._request(
            "PUT",
            f"{self._get_base_url()}/metafields/{metafield_id}.json",
            json={"metafield": {"id": metafield_id, **data}},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def delete_metafield(self, metafield_id: int) -> dict:
        r = self._request(
            "DELETE",
            f"{self._get_base_url()}/metafields/{metafield_id}.json",
            timeout=30,
        )
        r.raise_for_status()
        return {"deleted": True}

    def get_metafield_definitions(self, owner_type: str) -> dict:
        query = """
        query GetMetafieldDefinitions($ownerType: MetafieldOwnerType!, $first: Int!) {
            metafieldDefinitions(first: $first, ownerType: $ownerType) {
                edges {
                    node {
                        id
                        namespace
                        key
                        name
                        description
                        type {
                            name
                        }
                        validations {
                            name
                            type
                            value
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        try:
            import logging as _log
            logger = _log.getLogger(__name__)
            logger.info(f"Fetching metafield definitions for owner_type: {owner_type}")
            
            result = self.graphql(query, {"ownerType": owner_type, "first": 250})
            logger.info(f"GraphQL result for metafield definitions: {result}")
            
            definitions = [
                edge["node"]
                for edge in result.get("metafieldDefinitions", {}).get("edges", [])
            ]
            logger.info(f"Extracted {len(definitions)} definitions")
            return {"definitions": definitions}
        except Exception as e:
            import logging as _log
            logger = _log.getLogger(__name__)
            logger.error(f"Error in get_metafield_definitions: {e}", exc_info=True)
            raise