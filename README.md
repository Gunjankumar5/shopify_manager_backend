# Shopify Product Manager API (Backend)

FastAPI backend for Shopify product, collection, inventory, and bulk upload operations.

## Features

- Product CRUD and variant updates
- Product sync and duplicate detection/removal
- Collection CRUD with duplicate title checks
- Inventory locations, levels, and bulk updates
- CSV/XLS/XLSX upload parsing, preview, validation, and Shopify push
- CORS preconfigured for local frontend development

## Tech Stack

- Python 3.10+
- FastAPI
- Uvicorn
- Requests
- Pandas / OpenPyXL / xlrd

## Project Structure

- `main.py` - FastAPI app entrypoint and router registration
- `shopify_client.py` - Shopify Admin API client and helper methods
- `routes/products.py` - Product endpoints
- `routes/collections.py` - Collection endpoints
- `routes/inventory.py` - Inventory endpoints
- `routes/upload.py` - File upload and bulk import endpoints

## Prerequisites

- Python 3.10 or newer
- Shopify store credentials and API access

## Setup

1. Create and activate a virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Create a `.env` file in the project root (or copy from `.env.example`) and add:

```env
SHOPIFY_SHOP_NAME=your-shop-name
SHOPIFY_API_KEY=your-api-key
SHOPIFY_CLIENT_SECRET=your-client-secret
SHOPIFY_API_PASSWORD=your-access-token
SHOPIFY_API_VERSION=2026-01
```

## Run the Server

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

API docs:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- Health check: `http://localhost:8000/health`

## API Route Groups

- `/api/products`
  - list/search, sync, duplicate checks, bulk create/update, CRUD
- `/api/collections`
  - list/get/create/update/delete, add products to collection
- `/api/inventory`
  - levels, locations, update/adjust, bulk update
- `/api/upload`
  - preview, parse, validate, push to Shopify

## Notes

- CORS is enabled for `http://localhost:3000` and `http://localhost:5173`.
- Uploaded files supported: `.csv`, `.xlsx`, `.xls`.
- Keep `.env` private and never commit it.
