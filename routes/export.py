"""
routes/export.py

Endpoints:
  GET  /api/export/excel          → download .xlsx
  GET  /api/export/json           → load grid rows + snapshot (for sync baseline)
  POST /api/export/sync           → start sync (returns session_id)
  WS   /api/export/sync/progress  → stream row-by-row progress
  POST /api/export/grid-save      → simple push (no delta detection)
"""

import asyncio
import json
import time
import threading
import uuid
from typing import Dict, Any, List

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import Response

from services.bulk_fetch import BulkFetchService
from services.sync_bridge import (
    get_queue, pop_queue, remove_queue, run_sync, DONE_SENTINEL
)
from .store_utils import get_shopify_client

router = APIRouter()


# ── Excel export ──────────────────────────────────────────────────────────────

@router.get("/excel", summary="Export products to Excel")
def export_excel():
    try:
        service = BulkFetchService()
        excel_bytes = service.export_to_excel()
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="shopify_products_export.xlsx"'},
    )


# ── JSON load (rows + snapshot) ───────────────────────────────────────────────

@router.get("/json", summary="Load products as JSON rows + snapshot for sync")
def export_json():
    """
    Returns rows (for grid) AND snapshot (for sync delta detection baseline).
    Frontend stores the snapshot in memory and sends it back on sync.
    """
    try:
        service = BulkFetchService()
        rows, snapshot = service.full_sync()
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    cleaned = [
        {k: ("" if v is None else v) for k, v in row.items()}
        for row in rows
    ]

    return {
        "rows": cleaned,
        "snapshot": snapshot,
        "count": len(cleaned),
    }


# ── Start sync ────────────────────────────────────────────────────────────────

@router.post("/sync", status_code=202)
def start_sync(body: Dict[str, Any]):
    """
    Body: { "rows": [...], "snapshot": {...} }
    Returns: { "session_id": "...", "status": "running" }
    Connect to WS /api/export/sync/progress?session={session_id}
    """
    rows     = body.get("rows", [])
    snapshot = body.get("snapshot", {})
    shop_key = body.get("shop_key")

    if not rows:
        raise HTTPException(status_code=400, detail="No rows provided")

    session_id = str(uuid.uuid4())
    get_queue(session_id)  # register BEFORE thread starts
    print(f"[EXCEL_SYNC] start requested session={session_id} rows={len(rows)}")

    thread = threading.Thread(
        target=run_sync,
        args=(session_id, rows, snapshot, shop_key),
        daemon=True,
    )
    thread.start()
    print(f"[EXCEL_SYNC] worker started session={session_id}")

    return {"session_id": session_id, "status": "running"}


# ── WebSocket: live sync progress ─────────────────────────────────────────────

@router.websocket("/sync/progress")
async def sync_progress_ws(websocket: WebSocket, session: str = ""):
    await websocket.accept()
    print(f"[EXCEL_SYNC] ws connected session={session or 'missing'}")

    if not session:
        await websocket.send_text(json.dumps({"error": "session param required"}))
        await websocket.close()
        print("[EXCEL_SYNC] ws rejected reason=missing session")
        return

    # Wait up to 10s for queue
    deadline = time.time() + 10
    queue = None
    while time.time() < deadline:
        queue = pop_queue(session)
        if queue is not None:
            break
        await asyncio.sleep(0.05)

    if queue is None:
        await websocket.send_text(json.dumps({"error": "Session not found"}))
        await websocket.close()
        print(f"[EXCEL_SYNC] ws rejected session={session} reason=Session not found")
        return

    try:
        empty_ticks = 0
        while True:
            try:
                msg = queue.get_nowait()
                empty_ticks = 0
            except Exception:
                empty_ticks += 1
                if empty_ticks > 6000:  # 300s max
                    break
                await asyncio.sleep(0.05)
                continue

            if msg == DONE_SENTINEL:
                await asyncio.sleep(0.1)
                while True:
                    try:
                        rem = queue.get_nowait()
                        if rem != DONE_SENTINEL:
                            await websocket.send_text(rem)
                    except Exception:
                        break
                break

            await websocket.send_text(msg)

    except WebSocketDisconnect:
        print(f"[EXCEL_SYNC] ws disconnected session={session}")
    except Exception as e:
        print(f"[WS] sync/progress error: {e}")
    finally:
        remove_queue(session)
        print(f"[EXCEL_SYNC] ws closed session={session}")
        try:
            await websocket.close()
        except Exception:
            pass


# ── Simple grid-save (no delta detection) ────────────────────────────────────

@router.post("/grid-save", summary="Quick push edits to Shopify via REST")
def grid_save(payload: Dict[str, Any]):
    changes: List[Dict] = payload.get("changes", [])
    if not changes:
        raise HTTPException(status_code=400, detail="No changes provided")

    client = get_shopify_client()
    updated, failed, errors = 0, 0, []

    for row in changes:
        raw_product_id = str(row.get("Product ID", ""))
        raw_variant_id = str(row.get("Variant ID", ""))
        try:
            product_id = raw_product_id.split("/")[-1]
            variant_id = raw_variant_id.split("/")[-1]
        except Exception:
            errors.append({"row": row.get("Title", "?"), "error": "Invalid IDs"})
            failed += 1
            continue

        if not product_id.isdigit() or not variant_id.isdigit():
            errors.append({"row": row.get("Title", "?"), "error": f"Bad IDs"})
            failed += 1
            continue

        product_payload = {}
        if row.get("Title"):       product_payload["title"]        = row["Title"]
        if row.get("Body (HTML)"): product_payload["body_html"]    = row["Body (HTML)"]
        if row.get("Vendor"):      product_payload["vendor"]       = row["Vendor"]
        if row.get("Type"):        product_payload["product_type"] = row["Type"]
        if row.get("Tags"):        product_payload["tags"]         = row["Tags"]
        if row.get("Status"):      product_payload["status"]       = row["Status"].lower()

        variant_payload = {"id": str(variant_id)}
        if row.get("Variant Price"):            variant_payload["price"]            = str(row["Variant Price"])
        if row.get("Variant Compare At Price"): variant_payload["compare_at_price"] = str(row["Variant Compare At Price"])
        if row.get("Variant SKU"):              variant_payload["sku"]              = row["Variant SKU"]
        if row.get("Variant Barcode"):          variant_payload["barcode"]          = row["Variant Barcode"]

        try:
            if product_payload:
                client.update_product(product_id, product_payload)
            variant_fields = {k: v for k, v in variant_payload.items() if k != "id"}
            if variant_fields:
                client.update_product_variant(product_id, variant_id, variant_payload)
            updated += 1
        except Exception as e:
            errors.append({"row": row.get("Title", product_id), "error": str(e)})
            failed += 1

    return {"updated": updated, "failed": failed, "errors": errors}