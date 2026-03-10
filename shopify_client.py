import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

SHOP_NAME = os.getenv("SHOPIFY_SHOP_NAME")
API_KEY = os.getenv("SHOPIFY_API_KEY")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")
API_VERSION = os.getenv("SHOPIFY_API_VERSION") or "2026-01"
BASE_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}"

_cache = {
    "token": os.getenv("SHOPIFY_API_PASSWORD"),
    "expires_at": time.time() + 3600,
}


def _refresh_token():
    """Fetch a new access token using client credentials when available."""
    try:
        r = requests.post(
            f"https://{SHOP_NAME}.myshopify.com/admin/oauth/access_token",
            data={
                "client_id": API_KEY,
                "client_secret": CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            _cache["token"] = data["access_token"]
            _cache["expires_at"] = time.time() + data.get("expires_in", 86400)
            return _cache["token"]
    except Exception:
        pass
    return _cache["token"]


def _get_token():
    if time.time() >= _cache["expires_at"] - 300:
        return _refresh_token()
    return _cache["token"]


def _headers():
    return {
        "X-Shopify-Access-Token": _get_token(),
        "Content-Type": "application/json",
    }


class ShopifyClient:
    def __init__(self):
        if not SHOP_NAME:
            raise ValueError("Missing SHOPIFY_SHOP_NAME in .env")

    def get_products(self, limit=None, status=None, title=None, fetch_all=True):
        params = {}
        if limit is not None:
            params["limit"] = min(limit, 250)
        if status and status != "any":
            params["status"] = status
        if title:
            params["title"] = title

        if not fetch_all:
            r = requests.get(f"{BASE_URL}/products.json", headers=_headers(), params=params)
            if r.status_code == 401:
                _refresh_token()
                r = requests.get(f"{BASE_URL}/products.json", headers=_headers(), params=params)
            r.raise_for_status()
            return r.json()

        all_products = []
        next_url = f"{BASE_URL}/products.json"

        def _request_products_page(url, is_first_request):
            """Retry transient Shopify failures so pagination doesn't abort entire tasks."""
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                try:
                    if is_first_request:
                        response = requests.get(url, headers=_headers(), params=params, timeout=30)
                    else:
                        response = requests.get(url, headers=_headers(), timeout=30)

                    if response.status_code == 401:
                        _refresh_token()
                        if is_first_request:
                            response = requests.get(url, headers=_headers(), params=params, timeout=30)
                        else:
                            response = requests.get(url, headers=_headers(), timeout=30)

                    if response.status_code in (429, 500, 502, 503, 504):
                        if attempt < max_attempts:
                            time.sleep(1.5 * attempt)
                            continue

                    response.raise_for_status()
                    return response
                except requests.RequestException:
                    if attempt >= max_attempts:
                        raise
                    time.sleep(1.5 * attempt)

        while next_url:
            r = _request_products_page(next_url, next_url == f"{BASE_URL}/products.json")
            data = r.json()
            all_products.extend(data.get("products", []))

            next_url = r.links.get("next", {}).get("url")

        return {"products": all_products}

    def search_products(self, query):
        r = requests.get(
            f"{BASE_URL}/products.json",
            headers=_headers(),
            params={"title": query},
        )
        r.raise_for_status()
        return r.json().get("products", [])

    def get_product(self, product_id):
        r = requests.get(f"{BASE_URL}/products/{product_id}.json", headers=_headers())
        r.raise_for_status()
        return r.json()

    def create_product(self, data):
        r = requests.post(
            f"{BASE_URL}/products.json",
            headers=_headers(),
            json={"product": data},
        )
        r.raise_for_status()
        return r.json()

    def update_product(self, product_id, data):
        r = requests.put(
            f"{BASE_URL}/products/{product_id}.json",
            headers=_headers(),
            json={"product": data},
        )
        r.raise_for_status()
        return r.json()

    def delete_product(self, product_id):
        r = requests.delete(f"{BASE_URL}/products/{product_id}.json", headers=_headers())
        r.raise_for_status()
        return {"deleted": True}

    def update_product_variant(self, product_id, variant_id, data):
        r = requests.put(
            f"{BASE_URL}/variants/{variant_id}.json",
            headers=_headers(),
            json={"variant": data},
        )
        r.raise_for_status()
        return r.json()

    def count_products(self):
        r = requests.get(f"{BASE_URL}/products/count.json", headers=_headers())
        r.raise_for_status()
        return r.json()

    def get_custom_collections(self, limit=50):
        r = requests.get(
            f"{BASE_URL}/custom_collections.json",
            headers=_headers(),
            params={"limit": limit},
        )
        r.raise_for_status()
        return r.json()

    def get_collections(self, limit=50):
        custom = requests.get(
            f"{BASE_URL}/custom_collections.json",
            headers=_headers(),
            params={"limit": limit},
        )
        custom.raise_for_status()

        smart = requests.get(
            f"{BASE_URL}/smart_collections.json",
            headers=_headers(),
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
        r = requests.get(
            f"{BASE_URL}/custom_collections/{collection_id}.json",
            headers=_headers(),
        )
        r.raise_for_status()
        return r.json()

    def create_collection(self, data):
        r = requests.post(
            f"{BASE_URL}/custom_collections.json",
            headers=_headers(),
            json={"custom_collection": data},
        )
        r.raise_for_status()
        return r.json()

    def update_collection(self, collection_id, data):
        r = requests.put(
            f"{BASE_URL}/custom_collections/{collection_id}.json",
            headers=_headers(),
            json={"custom_collection": {"id": collection_id, **data}},
        )
        r.raise_for_status()
        return r.json()

    def add_products_to_collection(self, collection_id, product_ids):
        collects = []
        for product_id in product_ids:
            r = requests.post(
                f"{BASE_URL}/collects.json",
                headers=_headers(),
                json={"collect": {"product_id": product_id, "collection_id": collection_id}},
            )
            r.raise_for_status()
            collects.append(r.json().get("collect"))
        return {"collects": collects}

    def delete_collection(self, collection_id):
        r = requests.delete(
            f"{BASE_URL}/custom_collections/{collection_id}.json",
            headers=_headers(),
        )
        r.raise_for_status()
        return {"deleted": True}

    def create_custom_collection(self, title, image_url=None, product_ids=None):
        data = {"custom_collection": {"title": title}}
        if image_url:
            data["custom_collection"]["image"] = {"src": image_url}

        r = requests.post(f"{BASE_URL}/custom_collections.json", headers=_headers(), json=data)
        r.raise_for_status()
        result = r.json()

        if product_ids and result.get("custom_collection"):
            col_id = result["custom_collection"]["id"]
            for pid in product_ids:
                requests.post(
                    f"{BASE_URL}/collects.json",
                    headers=_headers(),
                    json={"collect": {"product_id": pid, "collection_id": col_id}},
                )
        return result

    def get_locations(self):
        r = requests.get(f"{BASE_URL}/locations.json", headers=_headers())
        r.raise_for_status()
        return r.json()

    def _enrich_inventory_levels(self, inventory_levels):
        """Attach product metadata to each inventory row for UI display."""
        try:
            # Inventory should only surface active products.
            products = self.get_products(status="active", fetch_all=True).get("products", [])
        except Exception:
            # Never fail inventory endpoint only because product enrichment failed.
            products = []
        variant_index = {}

        for product in products:
            for variant in product.get("variants", []):
                inventory_item_id = variant.get("inventory_item_id")
                if inventory_item_id is None:
                    continue
                variant_index[inventory_item_id] = {
                    "product_id": product.get("id"),
                    "product_title": product.get("title"),
                    "variant_id": variant.get("id"),
                    "variant_title": variant.get("title"),
                }

        enriched = []
        for level in inventory_levels:
            item_id = level.get("inventory_item_id")
            metadata = variant_index.get(item_id, {})
            if metadata:
                enriched.append({**level, **metadata})
        return enriched

    def get_inventory_levels(self, location_ids=None):
        params = {"limit": 250}
        if location_ids:
            if isinstance(location_ids, list):
                params["location_ids"] = ",".join(str(x) for x in location_ids)
            else:
                params["location_ids"] = str(location_ids)
        else:
            # Shopify requires filters for inventory_levels endpoint; default to all known locations.
            try:
                locations = self.get_locations().get("locations", [])
            except Exception:
                locations = []
            all_location_ids = [str(loc.get("id")) for loc in locations if loc.get("id") is not None]
            if all_location_ids:
                params["location_ids"] = ",".join(all_location_ids)

        all_levels = []
        next_url = f"{BASE_URL}/inventory_levels.json"
        first_request = True

        while next_url:
            if first_request:
                r = requests.get(next_url, headers=_headers(), params=params)
            else:
                r = requests.get(next_url, headers=_headers())

            if r.status_code == 401:
                _refresh_token()
                if first_request:
                    r = requests.get(next_url, headers=_headers(), params=params)
                else:
                    r = requests.get(next_url, headers=_headers())

            try:
                r.raise_for_status()
            except requests.HTTPError:
                if r.status_code not in (422, 500, 502, 503, 504):
                    raise

                # Fallback: derive inventory from product variants when inventory_levels API is unavailable.
                try:
                    products = self.get_products(status="active", fetch_all=True).get("products", [])
                except Exception:
                    products = []
                fallback_levels = []
                default_location_id = None
                try:
                    locations = self.get_locations().get("locations", [])
                except Exception:
                    locations = []
                if locations:
                    default_location_id = locations[0].get("id")

                for product in products:
                    for variant in product.get("variants", []):
                        inv_item_id = variant.get("inventory_item_id")
                        qty = variant.get("inventory_quantity")
                        if inv_item_id is None or qty is None:
                            continue
                        fallback_levels.append(
                            {
                                "inventory_item_id": inv_item_id,
                                "location_id": default_location_id,
                                "available": qty,
                            }
                        )

                return {"inventory_levels": self._enrich_inventory_levels(fallback_levels)}

            data = r.json()
            all_levels.extend(data.get("inventory_levels", []))

            next_url = r.links.get("next", {}).get("url")
            first_request = False

        return {"inventory_levels": self._enrich_inventory_levels(all_levels)}

    def update_inventory(self, inventory_item_id, location_id, available):
        r = requests.post(
            f"{BASE_URL}/inventory_levels/set.json",
            headers=_headers(),
            json={
                "location_id": location_id,
                "inventory_item_id": inventory_item_id,
                "available": available,
            },
        )
        r.raise_for_status()
        return r.json()
