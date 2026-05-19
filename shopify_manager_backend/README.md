# Shopify Product Manager API (Backend)

FastAPI backend for Shopify product, collection, inventory, and bulk upload operations with multi-store support and Supabase authentication.

## Features

- **Multi-Store Support**: Connect and manage multiple Shopify stores per user
- **Product CRUD and Variant Updates**: Create, read, update, delete products and variants
- **Product Sync & Duplicate Detection**: Sync products from Shopify and identify duplicates
- **Collection Management**: CRUD operations for collections with product assignment
- **Inventory Management**: View locations, levels, and bulk quantity updates
- **Bulk Upload**: Parse CSV/XLSX files, preview, validate, and push to Shopify
- **User & Role-Based Access Control**: Admin, Manager, and Junior user roles with granular permissions
- **Supabase Authentication**: Secure user authentication with Bearer token validation
- **Persistent Caching**: 30-second response cache scoped by user + store to prevent data leaks

## Tech Stack

- Python 3.10+
- FastAPI
- Uvicorn
- Requests
- Pandas / OpenPyXL / xlrd

## Project Structure

- `main.py` - FastAPI app entrypoint, middleware, and router registration
- `shopify_client.py` - Shopify Admin API client wrapper
- `routes/auth.py` - Store connection and active-store management
- `routes/auth_utils.py` - Supabase token validation and user extraction
- `routes/products.py` - Product endpoints with caching
- `routes/collections.py` - Collection endpoints
- `routes/inventory.py` - Inventory endpoints
- `routes/upload.py` - File upload and bulk import endpoints
- `routes/export.py` - Export endpoints (JSON, Excel, sync logs)
- `routes/users.py` - User management endpoints (admin/manager only)
- `routes/store_utils.py` - Multi-user store persistence
- `data/stores.json` - Connected stores and credentials (persisted per user)
- `data/active_store.json` - Active store selection (persisted per user)
- `data/users.json` - User profiles and permissions

## Prerequisites

- Python 3.10 or newer
- Shopify store with API credentials (obtained via OAuth during app usage)
- Supabase account with a project (see [SUPABASE_SETUP.md](../SUPABASE_SETUP.md))

## Setup

### 1. Create Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

For Linux/Mac:

```bash
python -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 3. Configure Environment

Create a `.env` file in the `backend/` directory (see templates below).

**Development `.env`:**

```env
# Supabase (REQUIRED)
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key-here

# Backend server
BACKEND_PORT=8000
```

**Replace with your actual Supabase credentials** from https://app.supabase.com → Settings → API

For detailed Supabase setup instructions, see [SUPABASE_SETUP.md](../SUPABASE_SETUP.md).

## Running the Server

### Development Mode (with auto-reload)

```powershell
python main.py
```

Or with Uvicorn directly:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### API Documentation

Once running, access:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **Health Check**: `http://localhost:8000/health`

The health endpoint includes active store name and connection status.

## API Route Groups

All endpoints require `Authorization: Bearer <supabase_token>` header.

### Authentication Routes (`/api/auth`)

- `POST /connect` - Initiate Shopify OAuth connection
- `GET /callback` - OAuth callback handler
- `GET /stores` - List connected stores for current user
- `GET /active-store` - Get current active store
- `POST /active-store/{shop_key}` - Switch active store (clears cache)
- `DELETE /stores/{shop_key}` - Disconnect a store

### Products Routes (`/api/products`)

- `GET /` - List products (paginated, cached 30s)
- `GET /all` - Fetch all products
- `GET /count` - Total product count
- `GET /sync` - Sync all products from Shopify
- `GET /find-duplicates` - Find duplicate products
- `POST /remove-duplicates` - Merge/remove duplicates
- `POST /bulk-create` - Create multiple products
- `POST /bulk-update` - Update multiple products
- `GET /{product_id}` - Get single product
- `POST /` - Create product
- `PUT /{product_id}` - Update product
- `DELETE /{product_id}` - Delete product
- `PUT /{product_id}/variants/{variant_id}` - Update variant

### Collections Routes (`/api/collections/`)

- `GET /` - List collections
- `GET /{collection_id}` - Get collection details
- `POST /` - Create collection
- `PUT /{collection_id}` - Update collection
- `DELETE /{collection_id}` - Delete collection
- `POST /{collection_id}/products` - Manage collection products

### Inventory Routes (`/api/inventory`)

- `GET /levels` - Get inventory levels
- `GET /locations` - List inventory locations
- `POST /update` - Update quantity
- `POST /adjust` - Adjust quantity (increment/decrement)
- `POST /bulk-update` - Bulk update quantities

### Upload Routes (`/api/upload`)

- `POST /preview` - Preview file before import
- `POST /parse` - Parse CSV/XLSX file
- `POST /validate` - Validate parsed data
- `POST /push-to-shopify` - Import to Shopify

Supported formats: CSV, XLSX, XLS

### Export Routes (`/api/export`)

- `GET /excel` - Export products as Excel
- `GET /json` - Export products as JSON
- `POST /sync` - Get sync operation status
- `POST /grid-save` - Save Handsontable grid edits

### Users Routes (`/api/users/`) - Admin/Manager Only

- `GET /` - List all users
- `POST /create-junior` - Create junior user
- `GET /{user_id}` - Get user info
- `PUT /{user_id}` - Update user
- `DELETE /{user_id}` - Deactivate user
- `GET /me/permissions` - Get current user permissions

See [USER_MANAGEMENT.md](../USER_MANAGEMENT.md) for detailed user management documentation.

## Key Features Explained

### Multi-User & Multi-Store

Each authenticated Supabase user can:

- Connect multiple Shopify stores
- Switch between stores (state persisted per user)
- See isolated product cache per user + store combination
- Manage permissions for other users (admin/manager only)

Connected stores and active store selection are persisted in JSON files scoped by `user_id`.

### Supabase Authentication

Every request must include a valid Supabase Bearer token:

```
Authorization: Bearer eyJhbGc...
```

The backend validates tokens by calling Supabase's `/auth/v1/user` endpoint. Token validation is cached for 5 minutes to reduce external API calls.

### Caching Strategy

- **Scope**: Per user + active store (prevents cross-store data leaks)
- **Duration**: 30 seconds for GET requests
- **Invalidation**: Automatically cleared on store switch, POST/PUT/DELETE mutations
- **Cache Key Format**: `{user_id}:{shop_key}:{endpoint}:{params_hash}`

### Response Format

Successful responses (2xx):

```json
{
  "products": [...],
  "has_next_page": true,
  "next_page_info": "cursor..."
}
```

Error responses (4xx/5xx):

```json
{
  "detail": "error description"
}
```

## Configuration Details

### Environment Variables

| Variable            | Required | Purpose                                     |
| ------------------- | -------- | ------------------------------------------- |
| `SUPABASE_URL`      | Yes      | Supabase project URL                        |
| `SUPABASE_ANON_KEY` | Yes      | Supabase anonymous key for token validation |
| `BACKEND_PORT`      | No       | Server port (default: 8000)                 |

### Store Persistence

Stores are persisted in `backend/data/stores.json` with structure:

```json
{
  "users": {
    "supabase-user-id": {
      "stores": {
        "store_key": {
          "shop_name": "example",
          "shop_domain": "example.myshopify.com",
          "api_key": "...",
          "api_secret": "...",
          "access_token": "...",
          "token_expiry": "2024-02-15T10:30:00Z"
        }
      }
    }
  }
}
```

On first run, legacy single-user data is automatically migrated to this multi-user schema.

## Deployment

### Railway Deployment

The project includes `railway.json` for Railway deployment:

```json
{
  "buildCommand": "pip install -r requirements.txt",
  "startCommand": "python main.py",
  "numReplicas": 1,
  "healthCheckPath": "/health"
}
```

Set environment variables in Railway project settings:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

### Notes

- CORS is enabled for all origins, methods, and headers (`allow_origins=["*"]`)
- Each request is validated against Supabase for authentication
- Store data is persisted locally; consider adding a database layer for production at scale

## Troubleshooting

### "401 Unauthorized" on all requests

- Check that `SUPABASE_URL` and `SUPABASE_ANON_KEY` are correctly set in `.env`
- Verify your Supabase project exists and has Authentication enabled
- Ensure the Bearer token in the request is valid and not expired

### Products are cached incorrectly

- Cache is scoped by user + store
- Clear cache by switching stores or restarting the server
- Frontend calls `api.clearCache()` on mutations for safety

### File upload fails

- Check that uploaded file is CSV, XLSX, or XLS format
- Verify the file isn't corrupted
- Check backend logs for detailed parse errors

### "Shop not found" when connecting store

- Ensure Shopify credentials (API key, secret) are correct
- Verify the store exists and API credentials have proper permissions
- Check that the store OAuth callback URL is properly configured

## Development Notes

- `backend/const response = await fetch...js` and `backend/Untitled-1.js` are scratch files and not used
- Legacy documentation mentions `.env.example` but setup is now handled by `SUPABASE_SETUP.md`
- Token refresh is handled automatically via a background task every 30 minutes

## Related Documentation

- [SUPABASE_SETUP.md](../SUPABASE_SETUP.md) - Supabase configuration guide
- [USER_MANAGEMENT.md](../USER_MANAGEMENT.md) - User roles and permissions
- [PROJECT_DOCUMENTATION.md](../PROJECT_DOCUMENTATION.md) - Full architecture overview
