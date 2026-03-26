"""
services/metafield_defs.py

Fetches PRODUCT and PRODUCTVARIANT metafield definitions from Shopify's
Admin GraphQL API and stores them as metafield_defs.json in backend/data/.

Definition structure in metafield_defs.json:
{
  "product": {
    "custom.snowboard_length": {
      "name":    "Snowboard length",
      "type":    "single_line_text_field",
      "choices": null,
      "min":     null,
      "max":     null,
      "regex":   null
    },
    "custom.material": {
      "name":    "Material",
      "type":    "list.single_line_text_field",
      "choices": ["Cotton", "Polyester", "Silk"],
      ...
    }
  },
  "variant": {
    "custom.size_guide": { ... }
  }
}

Used by:
  - bulk_fetch.py  → map ns.key to display name for column headers
  - frontend grid  → pick right cell editor (dropdown for choices, toggle for boolean)
  - sync_bridge.py → validate values before pushing to Shopify
"""

import json
import os
from pathlib import Path

DATA_DIR         = Path(__file__).parent.parent / "data"
DEFS_FILE        = DATA_DIR / "metafield_defs.json"

# ── GraphQL query (paginated) ─────────────────────────────────────────────────

_DEFS_QUERY = """
query metafieldDefs($ownerType: MetafieldOwnerType!, $cursor: String) {
  metafieldDefinitions(ownerType: $ownerType, first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      namespace
      key
      name
      type { name }
      validations { name value }
    }
  }
}
"""


# ── Validation parser ─────────────────────────────────────────────────────────

def _parse_validations(validations):
    """
    Parse Shopify metafield validations into structured fields.
    Returns (choices, min_val, max_val, regex).
    """
    choices = None
    min_val = None
    max_val = None
    regex   = None

    for v in (validations or []):
        name  = v.get("name", "")
        value = v.get("value", "")

        if name == "choices":
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    choices = [str(x).strip() for x in parsed if str(x).strip()]
                else:
                    parsed_str = str(parsed).strip()
                    choices = [parsed_str] if parsed_str else []
            except Exception:
                choices = [x.strip() for x in str(value).split(",") if x.strip()]

        elif name == "min":
            min_val = value

        elif name == "max":
            max_val = value

        elif name == "regex":
            regex = value

    return choices, min_val, max_val, regex


# ── Paginated fetcher ─────────────────────────────────────────────────────────

def _fetch_definitions(client, owner_type: str) -> dict:
    """
    Fetch all metafield definitions for PRODUCT or PRODUCTVARIANT.
    Uses your existing ShopifyClient.graphql() method.
    Handles Shopify pagination automatically.

    Returns { "ns.key": { name, type, choices, min, max, regex } }
    """
    all_defs = {}
    cursor   = None

    while True:
        variables = {"ownerType": owner_type}
        if cursor:
            variables["cursor"] = cursor

        result    = client.graphql(_DEFS_QUERY, variables)
        data      = (result.get("metafieldDefinitions") or {})
        nodes     = data.get("nodes") or []

        for node in nodes:
            ns  = node.get("namespace", "").strip()
            key = node.get("key", "").strip()
            if not ns or not key:
                continue

            ns_key = f"{ns}.{key}"
            choices, min_val, max_val, regex = _parse_validations(
                node.get("validations")
            )

            all_defs[ns_key] = {
                "name":    node.get("name", ns_key),
                "type":    (node.get("type") or {}).get("name", "single_line_text_field"),
                "choices": choices,
                "min":     min_val,
                "max":     max_val,
                "regex":   regex,
            }

        page_info = data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return all_defs


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_and_store_metafield_defs(client=None) -> dict:
    """
    Fetch PRODUCT + PRODUCTVARIANT metafield definitions from Shopify.
    Saves to backend/data/metafield_defs.json.

    Called during bulk_fetch so definitions stay in sync with the snapshot.

    Args:
        client: ShopifyClient instance. If None, creates one from active store.

    Returns the defs dict.
    """
    if client is None:
        from routes.store_utils import get_shopify_client
        client = get_shopify_client()

    DATA_DIR.mkdir(exist_ok=True)

    print("[METAFIELD DEFS] Fetching product definitions...")
    try:
        product_defs = _fetch_definitions(client, "PRODUCT")
        print(f"[METAFIELD DEFS] {len(product_defs)} product definition(s) found")
    except Exception as e:
        print(f"[METAFIELD DEFS] Failed to fetch product defs: {e}")
        product_defs = {}

    print("[METAFIELD DEFS] Fetching variant definitions...")
    try:
        variant_defs = _fetch_definitions(client, "PRODUCTVARIANT")
        print(f"[METAFIELD DEFS] {len(variant_defs)} variant definition(s) found")
    except Exception as e:
        print(f"[METAFIELD DEFS] Failed to fetch variant defs: {e}")
        variant_defs = {}

    defs = {"product": product_defs, "variant": variant_defs}

    DEFS_FILE.write_text(json.dumps(defs, indent=2, ensure_ascii=False))
    print(f"[METAFIELD DEFS] Saved to {DEFS_FILE}")

    return defs


def load_metafield_defs() -> dict:
    """
    Load stored metafield definitions from backend/data/metafield_defs.json.
    Returns empty structure if file doesn't exist yet.
    """
    if not DEFS_FILE.exists():
        return {"product": {}, "variant": {}}
    try:
        return json.loads(DEFS_FILE.read_text())
    except Exception:
        return {"product": {}, "variant": {}}


def get_display_name_map() -> dict:
    """
    Returns a flat dict mapping ns.key → display name for product metafields.
    Used by bulk_fetch.py to rename columns.

    Example: { "custom.snowboard_length": "Snowboard length" }
    """
    defs = load_metafield_defs()
    return {
        ns_key: meta["name"]
        for ns_key, meta in defs.get("product", {}).items()
        if meta.get("name")
    }