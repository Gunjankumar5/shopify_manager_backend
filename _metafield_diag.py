import json
from shopify_client import ShopifyClient

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

out = {}

q1 = '''
{
  products(first: 1) {
    edges {
      node {
        id
        title
        metafields(first: 5) {
          edges {
            node { namespace key value }
          }
        }
      }
    }
  }
}
'''
try:
    r1 = client.graphql(q1)
    edges = (((r1.get("products") or {}).get("edges")) or [])
    out["product_query_ok"] = True
    out["product_metafields_count_sample"] = len(((((edges[0].get("node") or {}).get("metafields") or {}).get("edges")) or [])) if edges else 0
except Exception as e:
    out["product_query_ok"] = False
    out["product_query_error"] = str(e)

q2 = '''
{
  collections(first: 1) {
    edges {
      node {
        id
        title
        metafields(first: 5) {
          edges {
            node { namespace key value }
          }
        }
      }
    }
  }
}
'''
try:
    r2 = client.graphql(q2)
    edges = (((r2.get("collections") or {}).get("edges")) or [])
    out["collection_query_ok"] = True
    out["collection_metafields_count_sample"] = len(((((edges[0].get("node") or {}).get("metafields") or {}).get("edges")) or [])) if edges else 0
except Exception as e:
    out["collection_query_ok"] = False
    out["collection_query_error"] = str(e)

try:
    out["access_scopes"] = client.get_access_scopes()
except Exception as e:
    out["access_scopes_error"] = str(e)

print(json.dumps(out, indent=2))
