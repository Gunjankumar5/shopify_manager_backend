import json
import os
from collections import Counter
from shopify_client import ShopifyClient
from services.bulk_fetch import BulkFetchService

with open("data/stores.json", "r", encoding="utf-8") as f:
    data = json.load(f)

users = data.get("users", {})
first_store = None
for _, u in users.items():
    stores = u.get("stores", {})
    if stores:
        first_store = next(iter(stores.values()))
        break

if not first_store:
    print(json.dumps({"error": "no connected store"}))
    raise SystemExit(0)

client = ShopifyClient(
    shop_name=first_store.get("shop") or first_store.get("shop_name"),
    access_token=first_store.get("access_token"),
    api_version=first_store.get("api_version", "2026-01"),
)

svc = object.__new__(BulkFetchService)
svc.client = client

svc._start_bulk_operation()
op = svc._wait_for_bulk_operation(timeout=600, interval=2)
path = svc._download_jsonl(op["url"], "diag_products_bulk.jsonl")

metafield_nodes = 0
metafield_parents = Counter()
product_nodes = 0
sample_parent = None

with open(path, "rb") as f:
    for line in f:
        if not line.strip():
            continue
        obj = svc._loads_json(line)
        obj_id = obj.get("id", "")
        if str(obj_id).startswith("gid://shopify/Product/"):
            product_nodes += 1
        elif str(obj_id).startswith("gid://shopify/Metafield/"):
            metafield_nodes += 1
            p = obj.get("__parentId")
            if p:
                metafield_parents[p] += 1
                if sample_parent is None:
                    sample_parent = p

rows, _ = svc._parse_jsonl(path)
non_empty_product_mf = sum(1 for r in rows if str(r.get("Product Metafields") or "").strip())

collection_map = svc._fetch_collections()
collection_mf_linked = sum(1 for _, v in collection_map.items() if v.get("metafields"))

print(json.dumps({
    "product_nodes": product_nodes,
    "metafield_nodes_in_bulk_jsonl": metafield_nodes,
    "metafield_parent_products": len(metafield_parents),
    "sample_metafield_parent": sample_parent,
    "rows_total": len(rows),
    "rows_with_product_metafields": non_empty_product_mf,
    "collection_product_links": len(collection_map),
    "collection_links_with_metafields": collection_mf_linked,
}, indent=2))

try:
    os.unlink(path)
except Exception:
    pass
