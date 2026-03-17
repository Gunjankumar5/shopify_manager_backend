from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import List
import pandas as pd
import io
import sys, os
import requests as _requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .store_utils import get_shopify_client

router = APIRouter()

def get_existing_products():
    """Fetch ALL existing products from Shopify to check for duplicates"""
    existing = {"titles": set(), "skus": set(), "handles": set()}
    try:
        client = get_shopify_client()

        try:
            try:
                result = client.get_products(fetch_all=True)
            except _requests.exceptions.HTTPError as http_err:
                # Token expired mid-fetch — refresh and retry
                if http_err.response is not None and http_err.response.status_code == 401:
                    print("🔄 Token expired while fetching existing products, refreshing...")
                    client = get_shopify_client()
                    result = client.get_products(fetch_all=True)
                else:
                    raise
            products = result.get("products", [])

            for product in products:
                title = product.get("title", "").lower().strip()
                if title:
                    existing["titles"].add(title)

                handle = product.get("handle", "").lower().strip()
                if handle:
                    existing["handles"].add(handle)

                for variant in product.get("variants", []):
                    sku = variant.get("sku", "").lower().strip()
                    if sku:
                        existing["skus"].add(sku)

            print(f"✅ Fetched {len(products)} existing products for duplicate check")

        except Exception as e:
            print(f"⚠️ Error fetching products: {e}")

    except Exception as e:
        print(f"⚠️ Could not fetch existing products: {e}")

    print(f"📦 Total existing: {len(existing['titles'])} titles, {len(existing['skus'])} SKUs")
    return existing


def parse_file(content, filename):
    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    elif filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(content))
    raise ValueError("Unsupported file type. Use .csv, .xlsx or .xls")


@router.post("/preview")
async def preview_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = parse_file(content, file.filename)
        df = df.fillna("")
        return {
            "filename": file.filename,
            "total_rows": len(df),
            "columns": list(df.columns),
            "preview": df.head(10).to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/parse")
async def parse_full_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = parse_file(content, file.filename)
        df = df.fillna("")
        return {
            "filename": file.filename,
            "total_rows": len(df),
            "columns": list(df.columns),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/validate")
async def validate_products(products: List[dict]):
    errors = []
    valid_count = 0
    duplicates = []
    existing_duplicates = []

    existing_products = get_existing_products()
    seen_titles = set()
    seen_skus = set()

    for i, product in enumerate(products):
        title = (product.get("title") or product.get("Title") or "").strip()
        sku = (product.get("sku") or product.get("Variant SKU") or "").strip()

        has_data = any(
            str(v).strip() for k, v in product.items()
            if k.lower() not in ["handle", "published", "option1 name", "option1 value"]
            and str(v).strip()
        )

        if not title and has_data:
            errors.append({
                "row": i + 1,
                "field": "title",
                "message": f"Row {i + 1} has data but is missing a Title"
            })
        elif title:
            title_lower = title.lower().strip()

            if title_lower in seen_titles:
                duplicates.append({"row": i + 1, "title": title, "reason": "Duplicate in uploaded file"})
            elif title_lower in existing_products["titles"]:
                existing_duplicates.append({"row": i + 1, "title": title, "reason": "Already exists in Shopify"})
            elif sku and sku.lower() in existing_products["skus"]:
                existing_duplicates.append({"row": i + 1, "title": title, "sku": sku, "reason": "SKU already exists in Shopify"})
            else:
                valid_count += 1

            seen_titles.add(title_lower)
            if sku:
                seen_skus.add(sku.lower())

    all_issues = errors + duplicates + existing_duplicates
    return {
        "valid": len(all_issues) == 0,
        "error_count": len(errors),
        "duplicate_count": len(duplicates) + len(existing_duplicates),
        "errors": errors,
        "duplicates": duplicates,
        "existing_duplicates": existing_duplicates,
        "total": len(products),
        "valid_products": valid_count
    }


@router.post("/push-to-shopify")
async def push_to_shopify(products: List[dict]):
    try:
        client = get_shopify_client()
        results = {
            "success": [],
            "errors": [],
            "skipped": [],
            "total": 0,
            "created": 0,
            "skipped_count": 0
        }

        # Fetch existing products FRESH before every push
        print("🔍 Fetching existing Shopify products before push...")
        existing_products = get_existing_products()
        print(f"🔍 Found {len(existing_products['titles'])} existing titles to check against")

        seen_titles = set()  # track titles within this upload batch

        # Group rows by Handle for multi-variant products
        grouped = {}
        for row in products:
            handle = (row.get("Handle") or row.get("handle") or "").strip()
            title = (row.get("Title") or row.get("title") or "").strip()

            if not handle and not title:
                continue

            key = handle or title
            title_lower = title.lower().strip()

            # DUPLICATE CHECK 1: variant row for already-grouped product
            if title_lower and title_lower in seen_titles:
                if key in grouped:
                    _add_variant_to_group(grouped[key], row)
                continue

            # DUPLICATE CHECK 2: already exists in Shopify by title
            if title_lower and title_lower in existing_products["titles"]:
                print(f"⏭️ Skipping '{title}' — already exists in Shopify")
                results["skipped"].append({"title": title, "reason": "Already exists in Shopify"})
                results["skipped_count"] += 1
                continue

            # DUPLICATE CHECK 3: handle already exists in Shopify
            if handle and handle.lower() in existing_products["handles"]:
                print(f"⏭️ Skipping '{title}' — handle already exists")
                results["skipped"].append({"title": title, "reason": f"Handle '{handle}' already exists in Shopify"})
                results["skipped_count"] += 1
                continue

            # New product — add to group
            if title_lower:
                seen_titles.add(title_lower)

            if key not in grouped:
                grouped[key] = {
                    "title": title,
                    "body_html": row.get("Body (HTML)") or row.get("body_html") or "",
                    "vendor": row.get("Vendor") or row.get("vendor") or "",
                    "product_type": row.get("Type") or row.get("product_type") or "",
                    "tags": row.get("Tags") or row.get("tags") or "",
                    "status": "draft",
                    "variants": [],
                    "images": []
                }

            _add_variant_to_group(grouped[key], row)

        results["total"] = len(grouped) + results["skipped_count"]

        # Push each unique product to Shopify
        for key, product_data in grouped.items():
            try:
                if not product_data["variants"]:
                    del product_data["variants"]
                if not product_data["images"]:
                    del product_data["images"]

                print(f"🚀 Creating: {product_data.get('title')}")
                try:
                    r = client.create_product(product_data)
                except _requests.exceptions.HTTPError as http_err:
                    # On 401 (token expired mid-upload), refresh the client and retry once
                    if http_err.response is not None and http_err.response.status_code == 401:
                        print(f"🔄 Token expired mid-upload, refreshing...")
                        client = get_shopify_client()
                        r = client.create_product(product_data)
                    else:
                        raise
                results["success"].append({
                    "title": product_data.get("title"),
                    "id": r.get("product", {}).get("id")
                })
                results["created"] += 1
            except Exception as e:
                print(f"❌ Failed: {product_data.get('title')} — {e}")
                results["errors"].append({
                    "title": product_data.get("title"),
                    "error": str(e)
                })

        print(f"✅ Done: {results['created']} created, {results['skipped_count']} skipped, {len(results['errors'])} errors")
        return results

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _add_variant_to_group(product_group: dict, row: dict):
    """Extract variant data from a row and add to product group"""
    variant = {}
    price = row.get("Variant Price") or row.get("price") or ""
    sku = row.get("Variant SKU") or row.get("sku") or ""
    qty = row.get("Variant Inventory Qty") or row.get("inventory_quantity") or ""
    barcode = row.get("Variant Barcode") or row.get("barcode") or ""
    compare_price = row.get("Variant Compare At Price") or row.get("compare_at_price") or ""

    if price:
        variant["price"] = str(price)
    if sku:
        variant["sku"] = str(sku)
    if qty:
        try:
            variant["inventory_quantity"] = int(float(str(qty)))
        except:
            pass
    if barcode:
        variant["barcode"] = str(barcode)
    if compare_price:
        variant["compare_at_price"] = str(compare_price)

    if variant:
        product_group["variants"].append(variant)

    img = row.get("Image Src") or row.get("image_src") or ""
    if img:
        img_src = str(img)
        existing_srcs = [i["src"] for i in product_group["images"]]
        if img_src not in existing_srcs:
            product_group["images"].append({"src": img_src})
