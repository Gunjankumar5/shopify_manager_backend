# Shopify Product Manager Backend Reference

## 1. What This Project Is

This repository is a FastAPI backend that acts as a thin application layer in front of the Shopify Admin REST API.

Its responsibilities are:

- expose product, collection, inventory, and upload endpoints for a frontend
- translate incoming requests into Shopify Admin API calls
- add a small amount of business logic such as duplicate detection, pagination aggregation, file parsing, and inventory enrichment
- normalize some responses into frontend-friendly shapes

This project does **not** have its own database or ORM layer. Most persistent data lives in Shopify.

## 2. High-Level Architecture

Core runtime files:

- [main.py](main.py): creates the FastAPI app, enables CORS, mounts all routers, and exposes root and health endpoints
- [shopify_client.py](shopify_client.py): central Shopify API client used by every route module
- [routes/products.py](routes/products.py): product APIs, sync, duplicate scanning, bulk create/update, variant updates
- [routes/collections.py](routes/collections.py): collection APIs and product-to-collection linking
- [routes/inventory.py](routes/inventory.py): inventory locations, levels, and quantity updates
- [routes/upload.py](routes/upload.py): file preview, parse, validation, and push-to-Shopify workflow
- [README.md](README.md): short setup overview, but it is not fully aligned with the code in a few places
- [requirements.txt](requirements.txt): Python dependencies
- [railway.json](railway.json): Railway deployment start command and healthcheck config
- [routes/**init**.py](routes/__init__.py): placeholder only

Non-core files currently in the repo root:

- [const response = await fetch(`${API_BASE.js](const%20response%20=%20await%20fetch(`${API_BASE.js): scratch JavaScript snippet, not used by the backend runtime
- [Untitled-1.js](Untitled-1.js): scratch JavaScript snippet, not used by the backend runtime

## 3. App Startup Flow

When the server starts:

1. [main.py](main.py) loads environment variables with `load_dotenv()`.
2. It imports routers from `routes.products`, `routes.collections`, `routes.inventory`, and `routes.upload`.
3. Each route module imports and instantiates `ShopifyClient()` at module import time, except the upload module which creates clients inside functions.
4. FastAPI registers routers with these prefixes:
   - `/api/products`
   - `/api/collections`
   - `/api/inventory`
   - `/api/upload`
5. CORS is configured to allow all origins, all methods, and all headers.

## 4. Environment and Configuration

### Required Runtime Variables

The actual code expects these variables in `.env`:

- `SHOPIFY_SHOP_NAME`
- `SHOPIFY_API_KEY`
- `SHOPIFY_CLIENT_SECRET`
- `SHOPIFY_API_PASSWORD`
- `SHOPIFY_API_VERSION` optional, defaults to `2026-01`

Important mismatch:

- [README.md](README.md) documents `SHOPIFY_SHOP_NAME`
- [.env.example](.env.example) documents `SHOPIFY_STORE_NAME`
- [shopify_client.py](shopify_client.py) actually reads `SHOPIFY_SHOP_NAME`

So the code follows `SHOPIFY_SHOP_NAME`, not `SHOPIFY_STORE_NAME`.

### Shopify Client Setup

[shopify_client.py](shopify_client.py) builds:

- `BASE_URL = https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}`
- request headers using `X-Shopify-Access-Token`

The token source is:

1. initial value from `SHOPIFY_API_PASSWORD`
2. optionally refreshed through `/admin/oauth/access_token` using `SHOPIFY_API_KEY` and `SHOPIFY_CLIENT_SECRET`

The client keeps an in-memory token cache with expiry tracking.

### CORS

Actual behavior from [main.py](main.py):

- `allow_origins=["*"]`
- `allow_credentials=False`
- `allow_methods=["*"]`
- `allow_headers=["*"]`

Important mismatch:

- [README.md](README.md) says CORS is configured for localhost frontend origins
- the code actually allows all origins

### Deployment

[railway.json](railway.json) starts the app with:

```json
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Healthcheck path is `/health`.

## 5. Request Flow Inside the App

For most requests the flow is:

1. FastAPI receives request.
2. Route function validates path/query/body according to the function signature.
3. Route calls a method on `ShopifyClient`.
4. `ShopifyClient` makes one or more `requests` calls to Shopify.
5. Route either returns the Shopify JSON as-is or wraps it in a custom response object.

There are four important exceptions to that simple pattern:

- product listing fetches **all** pages by default instead of respecting a single Shopify page
- collection listing merges custom collections and smart collections into one array
- inventory level listing enriches inventory rows with product and variant metadata
- upload routes use Pandas to parse files and apply duplicate/validation logic before pushing products to Shopify

## 6. API Surface Summary

The app exposes these top-level endpoints:

- `GET /`
- `GET /health`
- product endpoints under `/api/products`
- collection endpoints under `/api/collections/`
- inventory endpoints under `/api/inventory`
- upload endpoints under `/api/upload`

Important routing detail:

- `redirect_slashes=False` is set in [main.py](main.py)
- product routes are defined at `""`, so the collection-free form is `/api/products`
- collection routes are defined at `"/"`, so the correct list/create path is `/api/collections/` with a trailing slash
- because slash redirects are disabled, `/api/collections` may not auto-redirect to `/api/collections/`

## 7. Root and Health Endpoints

### `GET /`

Purpose:

- basic API info

Success response:

```json
{
  "message": "Shopify Product Manager API",
  "docs": "/docs",
  "version": "1.0.0"
}
```

### `GET /health`

Purpose:

- simple health probe used by Railway and external monitors

Success response:

```json
{
  "status": "healthy"
}
```

## 8. Product Endpoints

Mounted at `/api/products`.

### `GET /api/products`

Purpose:

- list products
- optionally filter by status
- optionally search by title

Query parameters:

- `limit`: optional integer. Passed into the Shopify client, but because `fetch_all=True` is hardcoded for the non-search path, it does not behave like strict pagination.
- `status`: optional string, default `any`
- `search`: optional string

Behavior:

- if `search` is provided, the route calls `shopify.search_products(search)` and returns a wrapped response
- otherwise, it calls `shopify.get_products(limit=limit, status=status, fetch_all=True)` and returns the raw result from the client

Success responses:

Search path:

```json
{
  "products": [
    {
      "id": 123,
      "title": "Example Product"
    }
  ],
  "count": 1
}
```

Non-search path:

```json
{
  "products": [
    {
      "id": 123,
      "title": "Example Product",
      "status": "active",
      "variants": []
    }
  ]
}
```

Error responses:

- `500` with `{ "detail": "..." }`
- `422` if query types are invalid

### `GET /api/products/sync`

Purpose:

- fetch all products from Shopify and compare that count with Shopify's reported count

Behavior:

- calls `/products/count.json`
- calls paginated `/products.json`
- reports count difference

Success response shape:

```json
{
  "message": "Successfully synced N products from Shopify",
  "synced_count": 100,
  "actual_store_count": 100,
  "difference": 0,
  "products": [],
  "count": 100
}
```

Error response:

- `500` with `{ "detail": "Failed to sync: ..." }`

### `GET /api/products/find-duplicates`

Purpose:

- detect duplicate products by normalized title without modifying Shopify data

Duplicate rule:

- lowercase + trimmed `product.title`

Success response:

```json
{
  "total_scanned": 100,
  "duplicates_found": 2,
  "duplicates": [
    {
      "id": 456,
      "title": "Example Product",
      "status": "active",
      "created_at": "2026-03-01T00:00:00Z",
      "duplicate_of_id": 123
    }
  ]
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `POST /api/products/remove-duplicates`

Purpose:

- delete duplicate products, keeping the first title occurrence encountered in the fetched product list

Request body:

- none

Success response:

```json
{
  "total_scanned": 100,
  "duplicates_found": 2,
  "deleted": 2,
  "failed": 0,
  "deleted_products": [
    {
      "id": 456,
      "title": "Example Product",
      "duplicate_of": 123
    }
  ],
  "errors": []
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `POST /api/products/bulk-create`

Purpose:

- create multiple Shopify products from a JSON array

Request body:

```json
[
  {
    "title": "Product A",
    "body_html": "<p>Description</p>",
    "vendor": "Vendor A",
    "product_type": "Type A",
    "variants": [
      {
        "price": "99.00",
        "sku": "SKU-1"
      }
    ]
  }
]
```

Behavior:

- each item is sent to Shopify as `{ "product": product_data }`
- partial success is supported

Success response:

```json
{
  "created": 1,
  "failed": 0,
  "results": [
    {
      "product": {
        "id": 123,
        "title": "Product A"
      }
    }
  ],
  "errors": []
}
```

Error responses:

- route-level `400` with `{ "detail": "..." }`
- per-item failures appear inside `errors`

### `POST /api/products/bulk-update`

Purpose:

- update multiple products from a JSON array

Request body rule:

- every item must include `id`
- the route removes `id` from the payload before sending the update object to Shopify

Example request body:

```json
[
  {
    "id": 123,
    "title": "Updated Product A",
    "status": "draft"
  }
]
```

Success response:

```json
{
  "updated": 1,
  "failed": 0,
  "results": [
    {
      "product": {
        "id": 123,
        "title": "Updated Product A"
      }
    }
  ],
  "errors": []
}
```

Possible item-level error:

```json
{
  "error": "Missing product id"
}
```

### `GET /api/products/{product_id}`

Purpose:

- fetch one product by Shopify product ID

Success response:

```json
{
  "product": {
    "id": 123,
    "title": "Example Product"
  }
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `POST /api/products`

Purpose:

- create one product

Request body:

- arbitrary JSON object accepted by Shopify's `product` payload
- `title` is required by this route

Example request body:

```json
{
  "title": "Example Product",
  "body_html": "<p>Description</p>",
  "vendor": "Vendor A",
  "product_type": "Type A",
  "variants": [
    {
      "price": "49.00",
      "sku": "SKU-ABC"
    }
  ]
}
```

Success response:

```json
{
  "product": {
    "id": 123,
    "title": "Example Product"
  }
}
```

Error responses:

- `400` with `{ "detail": "Product title is required" }`
- `400` with Shopify or request error details

### `PUT /api/products/{product_id}`

Purpose:

- update one product by ID

Request body:

- arbitrary JSON fields accepted by Shopify product updates

Success response:

```json
{
  "product": {
    "id": 123,
    "title": "Updated Product"
  }
}
```

Error response:

- `400` with `{ "detail": "..." }`

### `DELETE /api/products/{product_id}`

Purpose:

- delete one product by ID

Success response:

```json
{
  "message": "Product 123 deleted successfully"
}
```

Error response:

- `400` with `{ "detail": "..." }`

### `PUT /api/products/{product_id}/variants/{variant_id}`

Purpose:

- update one variant

Important detail:

- `product_id` is part of the URL but is not used by the Shopify client call
- the update is sent to `/variants/{variant_id}.json`

Request body:

- arbitrary variant fields accepted by Shopify

Example request body:

```json
{
  "id": 999,
  "price": "79.00",
  "sku": "SKU-NEW"
}
```

Success response:

```json
{
  "variant": {
    "id": 999,
    "price": "79.00",
    "sku": "SKU-NEW"
  }
}
```

Error response:

- `400` with `{ "detail": "..." }`

## 9. Collection Endpoints

Mounted at `/api/collections` with route functions defined using `"/"`.

Use the trailing-slash collection root endpoints exactly as written below.

### `GET /api/collections/`

Purpose:

- list collections

Query parameters:

- `limit`: integer, default `50`, max `250`

Behavior:

- fetches custom collections and smart collections separately from Shopify
- merges both arrays into one list under the key `custom_collections`
- adds `collection_type: "custom"` or `collection_type: "smart"` to each item

Success response:

```json
{
  "custom_collections": [
    {
      "id": 1,
      "title": "Manual Collection",
      "collection_type": "custom"
    },
    {
      "id": 2,
      "title": "Smart Collection",
      "collection_type": "smart"
    }
  ]
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `GET /api/collections/{collection_id}`

Purpose:

- fetch a single custom collection by ID

Important detail:

- this uses Shopify's `/custom_collections/{id}.json`
- it does not fetch smart collections by ID

Success response:

```json
{
  "custom_collection": {
    "id": 1,
    "title": "Manual Collection"
  }
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `POST /api/collections/`

Purpose:

- create a custom collection

Request body:

- arbitrary collection fields accepted by Shopify's `custom_collection` payload
- `title` is required by this route

Duplicate rule:

- compares lowercase-trimmed title against all collection titles returned from `get_collections(limit=250)`

Example request body:

```json
{
  "title": "Summer Picks",
  "body_html": "<p>Seasonal products</p>",
  "published_scope": "web"
}
```

Success response:

```json
{
  "custom_collection": {
    "id": 10,
    "title": "Summer Picks"
  }
}
```

Error responses:

- `409` with `{ "detail": "Collection with this title already exists" }`
- `400` with `{ "detail": "Collection title is required" }`
- `400` with other Shopify or request errors

### `PUT /api/collections/{collection_id}`

Purpose:

- update a custom collection

Behavior:

- if `title` is included, duplicate-title protection runs before update
- the payload sent to Shopify becomes:

```json
{
  "custom_collection": {
    "id": 10,
    "...": "...fields from request body..."
  }
}
```

Success response:

```json
{
  "custom_collection": {
    "id": 10,
    "title": "Updated Title"
  }
}
```

Error responses:

- `409` duplicate title
- `400` with `{ "detail": "..." }`

### `DELETE /api/collections/{collection_id}`

Purpose:

- delete a custom collection

Success response:

```json
{
  "deleted": true
}
```

Error response:

- `400` with `{ "detail": "..." }`

### `POST /api/collections/{collection_id}/products`

Purpose:

- add products to a collection by creating Shopify collects

Request body:

```json
[123, 456, 789]
```

Behavior:

- loops through product IDs
- posts one collect per product to Shopify

Success response:

```json
{
  "collects": [
    {
      "id": 1,
      "product_id": 123,
      "collection_id": 10
    }
  ]
}
```

Error response:

- `400` with `{ "detail": "..." }`

## 10. Inventory Endpoints

Mounted at `/api/inventory`.

### Important Input Detail

These two POST routes do **not** accept JSON bodies:

- `POST /api/inventory/update`
- `POST /api/inventory/adjust`

Because their handler arguments are plain scalar parameters, FastAPI treats them as **query parameters**.

Example:

```http
POST /api/inventory/update?inventory_item_id=111&location_id=222&quantity=15
```

### `GET /api/inventory/levels`

Purpose:

- fetch inventory levels and enrich them with product/variant metadata

Query parameters:

- `location_ids`: optional comma-separated list of IDs, for example `123,456`

Behavior:

- if `location_ids` is given, it is split into a list of integers
- if omitted, the Shopify client first fetches all locations, then queries inventory for all location IDs
- the client paginates through `/inventory_levels.json`
- if Shopify inventory-level API fails with `422`, `500`, `502`, `503`, or `504`, the client falls back to active products and uses variant `inventory_quantity`
- final response is enriched with:
  - `product_id`
  - `product_title`
  - `variant_id`
  - `variant_title`

Success response:

```json
{
  "inventory_levels": [
    {
      "inventory_item_id": 111,
      "location_id": 222,
      "available": 15,
      "product_id": 123,
      "product_title": "Example Product",
      "variant_id": 999,
      "variant_title": "Default Title"
    }
  ]
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `GET /api/inventory/locations`

Purpose:

- list Shopify inventory locations

Success response:

```json
{
  "locations": [
    {
      "id": 222,
      "name": "Main Warehouse"
    }
  ]
}
```

Error response:

- `500` with `{ "detail": "..." }`

### `POST /api/inventory/update`

Purpose:

- set inventory quantity to an absolute value

Required query parameters:

- `inventory_item_id`
- `location_id`
- `quantity`

Underlying Shopify call:

- `/inventory_levels/set.json`

Success response:

- raw Shopify response from inventory set endpoint

Typical shape:

```json
{
  "inventory_level": {
    "inventory_item_id": 111,
    "location_id": 222,
    "available": 15
  }
}
```

Error responses:

- `400` with `{ "detail": "..." }`
- `422` if required query parameters are missing or invalid

### `POST /api/inventory/adjust`

Purpose:

- intended to adjust inventory relatively, but currently behaves like an absolute set operation

Required query parameters:

- `inventory_item_id`
- `location_id`
- `adjustment`

Important behavior:

- despite the route name and docstring, the code passes `adjustment` directly as `available` to Shopify's set endpoint
- this means it does **not** read current quantity and apply a delta

Success response:

- same shape as inventory update route, because both use the same client method

Error responses:

- `400` with `{ "detail": "..." }`
- `422` validation errors for missing query parameters

### `POST /api/inventory/bulk-update`

Purpose:

- set inventory for multiple items from one JSON array

Request body:

```json
[
  {
    "inventory_item_id": 111,
    "location_id": 222,
    "quantity": 15
  }
]
```

Behavior:

- loops over updates sequentially
- partial success is supported

Success response:

```json
{
  "updated": 1,
  "failed": 0,
  "results": [
    {
      "inventory_level": {
        "inventory_item_id": 111,
        "location_id": 222,
        "available": 15
      }
    }
  ],
  "errors": []
}
```

Per-item failure shape:

```json
{
  "error": "...",
  "item": {
    "inventory_item_id": 111,
    "location_id": 222,
    "quantity": 15
  }
}
```

## 11. Upload Endpoints

Mounted at `/api/upload`.

This module supports CSV, XLSX, and XLS uploads using Pandas.

### Supported File Types

- `.csv`
- `.xlsx`
- `.xls`

Unsupported files return:

```json
{
  "detail": "Unsupported file type. Use .csv, .xlsx or .xls"
}
```

### `POST /api/upload/preview`

Purpose:

- read a file and return only a preview

Request type:

- multipart form-data with file field named `file`

Behavior:

- reads the uploaded file into a Pandas DataFrame
- replaces `NaN` with empty strings
- returns first 10 rows only

Success response:

```json
{
  "filename": "products.csv",
  "total_rows": 100,
  "columns": ["Title", "Variant SKU"],
  "preview": [
    {
      "Title": "Example Product",
      "Variant SKU": "SKU-1"
    }
  ]
}
```

Error response:

- `400` with `{ "detail": "..." }`

### `POST /api/upload/parse`

Purpose:

- read the full uploaded file and return all parsed rows

Request type:

- multipart form-data with file field named `file`

Success response:

```json
{
  "filename": "products.csv",
  "total_rows": 100,
  "columns": ["Title", "Variant SKU"],
  "data": [
    {
      "Title": "Example Product",
      "Variant SKU": "SKU-1"
    }
  ]
}
```

Error response:

- `400` with `{ "detail": "..." }`

### `POST /api/upload/validate`

Purpose:

- validate uploaded product rows before pushing to Shopify

Request body:

- JSON array of row objects, usually the output of `/api/upload/parse`

Validation logic:

1. fetches **all** existing Shopify products
2. builds lookup sets for existing titles, handles, and SKUs
3. checks each uploaded row for:
   - missing title when the row has data
   - duplicate title inside the uploaded file
   - duplicate title already present in Shopify
   - duplicate SKU already present in Shopify

Accepted title fields:

- `title`
- `Title`

Accepted SKU fields:

- `sku`
- `Variant SKU`

Rows considered metadata-only for the missing-title check exclude these lowercased keys:

- `handle`
- `published`
- `option1 name`
- `option1 value`

Success response:

```json
{
  "valid": false,
  "error_count": 1,
  "duplicate_count": 2,
  "errors": [
    {
      "row": 2,
      "field": "title",
      "message": "Row 2 has data but is missing a Title"
    }
  ],
  "duplicates": [
    {
      "row": 3,
      "title": "Example Product",
      "reason": "Duplicate in uploaded file"
    }
  ],
  "existing_duplicates": [
    {
      "row": 4,
      "title": "Existing Product",
      "reason": "Already exists in Shopify"
    }
  ],
  "total": 10,
  "valid_products": 7
}
```

### `POST /api/upload/push-to-shopify`

Purpose:

- convert uploaded rows into Shopify product payloads and create products in Shopify

Request body:

- JSON array of row objects

High-level workflow:

1. fetch all existing Shopify products again before pushing
2. build duplicate lookup sets for titles, SKUs, and handles
3. group rows by `Handle` or fallback `Title`
4. merge multiple variant rows into one product group
5. skip products already present by title or handle
6. create one Shopify product per group

Recognized input columns for product-level fields:

- `Handle` or `handle`
- `Title` or `title`
- `Body (HTML)` or `body_html`
- `Vendor` or `vendor`
- `Type` or `product_type`
- `Tags` or `tags`

Recognized input columns for variant-level fields:

- `Variant Price` or `price`
- `Variant SKU` or `sku`
- `Variant Inventory Qty` or `inventory_quantity`
- `Variant Barcode` or `barcode`
- `Variant Compare At Price` or `compare_at_price`

Recognized image columns:

- `Image Src` or `image_src`

Product payload defaults:

- `status` is hardcoded to `draft`

Variant/image handling:

- empty `variants` and `images` arrays are removed before create
- image URLs are deduplicated within each product group

Success response:

```json
{
  "success": [
    {
      "title": "New Product",
      "id": 123
    }
  ],
  "errors": [
    {
      "title": "Broken Product",
      "error": "..."
    }
  ],
  "skipped": [
    {
      "title": "Existing Product",
      "reason": "Already exists in Shopify"
    }
  ],
  "total": 3,
  "created": 1,
  "skipped_count": 1
}
```

Error response:

- `400` with `{ "detail": "..." }`

## 12. Shopify Client Behavior

The backend logic depends heavily on the behavior of [shopify_client.py](shopify_client.py).

### Product Fetching

`get_products()`:

- can pass `limit`, `status`, and `title`
- if `fetch_all=False`, makes a single request to `/products.json`
- if `fetch_all=True`, follows pagination via response `Link` headers until all pages are collected
- retries transient failures for `429`, `500`, `502`, `503`, and `504`
- refreshes token and retries on `401`

### Search

`search_products(query)`:

- calls `/products.json?title=query`
- returns only the `products` array rather than the full JSON wrapper

### Collections

`get_collections(limit=50)`:

- fetches both `/custom_collections.json` and `/smart_collections.json`
- adds `collection_type`
- returns everything under the key `custom_collections` for backward compatibility

### Inventory Enrichment

`_enrich_inventory_levels()`:

- fetches active products
- indexes variants by `inventory_item_id`
- attaches product and variant metadata to inventory rows
- drops inventory rows that do not match any indexed active-product variant

This means inventory rows without a matching active product variant are not returned in the final response.

### Inventory Fallback Mode

If `/inventory_levels.json` fails with certain Shopify errors, the client:

1. fetches active products
2. reads `variant.inventory_quantity`
3. assigns the first location ID as a default location if one exists
4. returns synthesized inventory rows

This keeps the endpoint working even when Shopify inventory-level retrieval is unavailable, but the data may be less precise than the normal API result.

## 13. Error Model

There is no single unified error schema beyond FastAPI's default pattern and custom `HTTPException` usage.

Common forms are:

### FastAPI validation errors

```json
{
  "detail": [
    {
      "loc": ["query", "quantity"],
      "msg": "Field required",
      "type": "missing"
    }
  ]
}
```

These usually return `422`.

### Application errors

```json
{
  "detail": "...message..."
}
```

Depending on the route, these return `400`, `409`, or `500`.

### Partial success endpoints

These routes return both results and per-item error arrays instead of failing the whole request immediately:

- `POST /api/products/bulk-create`
- `POST /api/products/bulk-update`
- `POST /api/inventory/bulk-update`
- `POST /api/upload/push-to-shopify`

## 14. Dependencies and Why They Exist

From [requirements.txt](requirements.txt):

- `fastapi`, `starlette`, `uvicorn`: HTTP API framework and ASGI server
- `requests`: Shopify HTTP client
- `python-dotenv`: environment loading
- `python-multipart`: file upload parsing
- `pandas`, `openpyxl`, `xlrd`, `numpy`: CSV/Excel parsing and tabular transforms
- `annotated-types`, `pydantic`, `pydantic_core`, `typing_extensions`, `typing-inspection`: FastAPI/Pydantic typing stack

Packages currently present but not used in the visible backend code paths:

- `APScheduler`
- `beautifulsoup4`
- `lxml`
- `playwright`
- `psycopg2-binary`
- `greenlet`
- `git-filter-repo`

These may be leftovers, used by tooling outside the files inspected, or intended for future work.

## 15. Operational Notes and Quirks

These are the most important implementation details to know before integrating with this API.

### 1. Collection root path requires a trailing slash

- correct: `/api/collections/`
- potentially wrong: `/api/collections`

### 2. Inventory update and adjust use query parameters, not JSON bodies

- `POST /api/inventory/update?inventory_item_id=...&location_id=...&quantity=...`
- `POST /api/inventory/adjust?inventory_item_id=...&location_id=...&adjustment=...`

### 3. Inventory adjust is misnamed in behavior

- it performs an absolute set using the supplied `adjustment` number
- it does not apply a relative delta

### 4. Product list is effectively an all-products fetch

- the non-search list path always calls `fetch_all=True`
- `limit` is not acting like normal backend pagination there

### 5. Collection get/update/delete only target custom collections

- smart collections are only included in the merged list response
- smart collection detail/update/delete is not supported in these routes

### 6. Upload push creates products as draft

- `status` is hardcoded to `draft` during push-to-Shopify

### 7. Duplicate checks are title/handle/SKU based, not full-content based

- duplicate product detection uses normalized title
- upload skip logic uses title and handle
- SKU duplication is used during validation, not as the main push grouping key

### 8. Route error codes are inconsistent across modules

- some read failures return `500`
- many write failures return `400`
- duplicate collection titles return `409`

### 9. Router instantiation depends on environment availability at import time

- `ShopifyClient()` is created when route modules import
- missing required env values can break app startup before any request is served

## 16. How To Run and Inspect It

Local startup command from [README.md](README.md):

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Useful local URLs:

- `/docs` Swagger UI
- `/redoc` ReDoc
- `/openapi.json` generated OpenAPI schema
- `/health` health probe

## 17. Recommended Mental Model

The cleanest way to think about this backend is:

- FastAPI provides request validation and routing
- route modules add small business rules and response shaping
- Shopify is the real system of record
- upload endpoints are the only area with meaningful transformation logic inside this codebase

If you need to extend the project, most new work will fall into one of these categories:

- add a new route in `routes/`
- add a corresponding Shopify call in [shopify_client.py](shopify_client.py)
- update frontend expectations for response shape
- tighten validation with Pydantic models, since most current handlers accept loose `dict` and `List[dict]` payloads

## 18. Complete Endpoint List

For quick reference, these are all application endpoints currently exposed:

- `GET /`
- `GET /health`
- `GET /api/products`
- `POST /api/products`
- `GET /api/products/sync`
- `GET /api/products/find-duplicates`
- `POST /api/products/remove-duplicates`
- `POST /api/products/bulk-create`
- `POST /api/products/bulk-update`
- `GET /api/products/{product_id}`
- `PUT /api/products/{product_id}`
- `DELETE /api/products/{product_id}`
- `PUT /api/products/{product_id}/variants/{variant_id}`
- `GET /api/collections/`
- `POST /api/collections/`
- `GET /api/collections/{collection_id}`
- `PUT /api/collections/{collection_id}`
- `DELETE /api/collections/{collection_id}`
- `POST /api/collections/{collection_id}/products`
- `GET /api/inventory/levels`
- `GET /api/inventory/locations`
- `POST /api/inventory/update`
- `POST /api/inventory/adjust`
- `POST /api/inventory/bulk-update`
- `POST /api/upload/preview`
- `POST /api/upload/parse`
- `POST /api/upload/validate`
- `POST /api/upload/push-to-shopify`
