"""FastAPI ingress — accepts external triggers and publishes them to the event bus.
Also serves the dashboard and provides API endpoints for frontend data.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID, uuid4
import os

import structlog
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import text

from common.bus.redis_bus import get_bus
from common.events.schemas import OrderReceived, VendorDelayAlert
from common.storage.postgres import get_session
from common.events.schemas import AlertRaised 

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    bus = await get_bus()
    app.state.bus = bus
    yield
    await bus.close()


app = FastAPI(title="Supply chain ingress API", lifespan=lifespan)


# ─── Dashboard static files ──────────────────────────────
dashboard_path = os.path.join(os.path.dirname(__file__), "../dashboard")
if os.path.exists(dashboard_path):
    app.mount("/dashboard", StaticFiles(directory=dashboard_path, html=True), name="dashboard")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect root to dashboard if it exists, otherwise show a simple message."""
    index_path = os.path.join(dashboard_path, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r") as f:
            return f.read()
    return HTMLResponse("<h1>Supply Chain Agent System</h1><p>Dashboard not found. Place index.html in dashboard/ folder.</p>")


# ─── Health check ────────────────────────────────────────
@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


# ─── Order ingestion ─────────────────────────────────────
class OrderIn(BaseModel):
    customer_id: UUID
    sku: str
    quantity: int
    total_value: float
    deadline: datetime


@app.post("/orders", status_code=202)
async def create_order(order: OrderIn) -> dict[str, str]:
    if order.quantity <= 0:
        raise HTTPException(400, "quantity must be > 0")
    correlation_id = uuid4()
    event = OrderReceived(
        correlation_id=correlation_id,
        source_agent="api",
        order_id=uuid4(),
        customer_id=order.customer_id,
        sku=order.sku,
        quantity=order.quantity,
        total_value=order.total_value,
        deadline=order.deadline,
    )
    await app.state.bus.publish(event)
    return {"correlation_id": str(correlation_id), "order_id": str(event.order_id)}


# ─── Vendor delay webhook ────────────────────────────────
class VendorDelayIn(BaseModel):
    po_id: UUID
    vendor_id: UUID
    original_eta: datetime
    new_eta: datetime


@app.post("/vendor/delays", status_code=202)
async def vendor_delay(payload: VendorDelayIn) -> dict[str, str]:
    delay_h = int((payload.new_eta - payload.original_eta).total_seconds() // 3600)
    event = VendorDelayAlert(
        correlation_id=uuid4(),
        source_agent="api",
        po_id=payload.po_id,
        vendor_id=payload.vendor_id,
        original_eta=payload.original_eta,
        new_eta=payload.new_eta,
        delay_hours=delay_h,
    )
    await app.state.bus.publish(event)
    return {"event_id": str(event.event_id)}


# ─── Dashboard API endpoints ─────────────────────────────
@app.get("/api/orders")
async def get_orders(limit: int = 20):
    """Return recent orders for dashboard."""
    async with get_session() as session:
        result = await session.execute(
            text("""
                SELECT order_id, sku, quantity, total_value, status, created_at
                FROM orders
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit}
        )
        orders = [
            {
                "order_id": str(row[0]),
                "sku": row[1],
                "quantity": row[2],
                "total_value": float(row[3]),
                "status": row[4],
                "created_at": row[5].isoformat()
            }
            for row in result
        ]
    return orders


@app.get("/api/inventory")
async def get_inventory():
    """Return current inventory levels."""
    async with get_session() as session:
        result = await session.execute(
            text("""
                SELECT sku, on_hand_qty, reserved_qty, reorder_point, safety_stock
                FROM inventory
            """)
        )
        inventory = [
            {
                "sku": row[0],
                "on_hand": row[1],
                "reserved": row[2],
                "reorder_point": row[3],
                "safety_stock": row[4]
            }
            for row in result
        ]
    return inventory


@app.get("/api/alerts")
async def get_alerts(limit: int = 10):
    """Return recent alerts."""
    async with get_session() as session:
        result = await session.execute(
            text("""
                SELECT severity, category, message, created_at
                FROM alerts
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit}
        )
        alerts = [
            {
                "severity": row[0],
                "category": row[1],
                "message": row[2],
                "created_at": row[3].isoformat()
            }
            for row in result
        ]
    return alerts
# ─── AI Issue Resolution endpoints ───────────────────────

@app.get("/api/incidents")
async def get_incidents(limit: int = 20):
    """Return recent AI-resolved incidents with full reasoning trace."""
    async with get_session() as session:
        result = await session.execute(
            text("""
                SELECT incident_id, severity, category, summary, reasoning,
                       action_taken, action_payload, confidence, requires_human,
                       resolved, created_at
                FROM incidents
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit}
        )
        incidents = [
            {
                "incident_id": str(row[0]),
                "severity": row[1],
                "category": row[2],
                "summary": row[3],
                "reasoning": row[4],
                "action_taken": row[5],
                "action_payload": row[6],
                "confidence": float(row[7]) if row[7] is not None else None,
                "requires_human": row[8],
                "resolved": row[9],
                "created_at": row[10].isoformat(),
            }
            for row in result
        ]
    return incidents


@app.get("/api/incidents/{correlation_id}")
async def get_incident_trace(correlation_id: UUID):
    """Get the full trace for one correlation_id."""
    async with get_session() as session:
        alert_row = (await session.execute(
            text("SELECT severity, category, message, context, created_at "
                 "FROM alerts WHERE correlation_id = :cid LIMIT 1"),
            {"cid": correlation_id}
        )).first()

        incident_row = (await session.execute(
            text("SELECT summary, reasoning, action_taken, action_payload, "
                 "       confidence, requires_human, created_at "
                 "FROM incidents WHERE correlation_id = :cid LIMIT 1"),
            {"cid": correlation_id}
        )).first()

    return {
        "correlation_id": str(correlation_id),
        "alert": {
            "severity": alert_row[0],
            "category": alert_row[1],
            "message": alert_row[2],
            "context": alert_row[3],
            "created_at": alert_row[4].isoformat(),
        } if alert_row else None,
        "ai_decision": {
            "summary": incident_row[0],
            "reasoning": incident_row[1],
            "action_taken": incident_row[2],
            "action_payload": incident_row[3],
            "confidence": float(incident_row[4]) if incident_row and incident_row[4] is not None else None,
            "requires_human": incident_row[5],
            "created_at": incident_row[6].isoformat(),
        } if incident_row else None,
    }


@app.post("/api/test/fire-alert")
async def fire_test_alert():
    """Fire a sample stockout alert to demo the AI issue-resolution agent."""
    event = AlertRaised(
        correlation_id=uuid4(),
        source_agent="api",
        severity="high",
        category="recurring_stockout",
        message="SKU-002 stock low repeatedly — backup vendor needed",
        context={"sku": "SKU-002", "count": 6, "demo": True},
    )
    await app.state.bus.publish(event)
    return {"correlation_id": str(event.correlation_id), "fired": True}