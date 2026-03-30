import tempfile, json, os
from services.bulk_fetch import BulkFetchService

# Create a small JSONL file simulating bulk op output
lines = []
# Product node
prod = {
    "id": "gid://shopify/Product/1001",
    "handle": "test-product",
    "title": "Test Product",
    "descriptionHtml": "<p>Test</p>",
    "vendor": "ACME",
    "productType": "Gadget",
    "tags": ["tag1","tag2"],
    "status": "ACTIVE",
    "createdAt": "2026-03-30T00:00:00Z",
    "updatedAt": "2026-03-30T00:00:00Z",
    "seo": {"title": "SEO", "description": "desc"},
    "metafields": {"edges": []}
}
lines.append(json.dumps(prod))
# Variant node
var = {
    "id": "gid://shopify/ProductVariant/2001",
    "__parentId": "gid://shopify/Product/1001",
    "sku": "SKU123",
    "price": "9.99",
    "compareAtPrice": "12.99",
    "barcode": "1234567890",
    "inventoryItem": {"id": "gid://shopify/InventoryItem/3001", "tracked": True, "requiresShipping": True, "unitCost": {"amount": "1.23"}, "measurement": {"weight": {"value": 100, "unit": "g"}}}
}
lines.append(json.dumps(var))
# Metafield node linked to product
mf = {
    "id": "gid://shopify/Metafield/4001",
    "__parentId": "gid://shopify/Product/1001",
    "namespace": "global",
    "key": "color",
    "value": "red",
}
lines.append(json.dumps(mf))

fd, path = tempfile.mkstemp(suffix=".jsonl")
with os.fdopen(fd, 'w', encoding='utf-8') as f:
    for l in lines:
        f.write(l + "\n")

# Instantiate without calling __init__ to avoid Shopify client
svc = object.__new__(BulkFetchService)
rows, snapshot = svc._parse_jsonl(path)
print(json.dumps({"rows": rows, "snapshot_keys": list(snapshot.keys())}, indent=2))

os.unlink(path)
