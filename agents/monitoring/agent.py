"""Monitoring agent — alerts persistence, SLA breach detection, anomaly tracking.

This agent is observe-only. It never mutates business state; only writes to the
alerts table and publishes alert_raised events when thresholds are tripped.
"""
import json
from datetime import datetime, timezone

from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    AlertRaised,
    BaseEvent,
    DeliveryConfirmed,
    EventType,
    OrderReceived,
    POCreated,
    ProductionComplete,
    StockLow,
)
from common.storage.postgres import get_session


class MonitoringAgent(BaseAgent):
    name = "monitoring"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Monitoring agent started")

    def subscriptions(self):
        return {
            EventType.ALERT_RAISED: (AlertRaised, self.handle_alert),
            EventType.DELIVERY_CONFIRMED: (DeliveryConfirmed, self.handle_delivery),
            EventType.ORDER_RECEIVED: (OrderReceived, self.handle_order),
            EventType.PRODUCTION_COMPLETE: (ProductionComplete, self.handle_production),
            EventType.PO_CREATED: (POCreated, self.handle_po),
            EventType.STOCK_LOW: (StockLow, self.handle_stock_low),
        }

    async def handle_alert(self, event: BaseEvent) -> None:
        assert isinstance(event, AlertRaised)
        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO alerts (severity, category, message, context, correlation_id) "
                    "VALUES (:severity, :category, :message, CAST(:context AS JSONB), :correlation_id)"
                ),
                {
                    "severity": event.severity,
                    "category": event.category,
                    "message": event.message,
                    "context": json.dumps(event.context),
                    "correlation_id": event.correlation_id,
                },
            )
        self.log.warning("Alert stored", severity=event.severity, category=event.category)

    async def handle_delivery(self, event: BaseEvent) -> None:
        """SLA breach detection only — does NOT mutate orders.status."""
        assert isinstance(event, DeliveryConfirmed)
        async with get_session() as s:
            row = (
                await s.execute(
                    text("SELECT deadline FROM orders WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )
            ).first()
            if not row:
                return
            (deadline,) = row
            if deadline and event.delivered_at > deadline:
                await self.bus.publish(
                    AlertRaised(
                        correlation_id=event.correlation_id,
                        source_agent=self.name,
                        severity="medium",
                        category="sla_breach",
                        message=f"Order {event.order_id} delivered late",
                        context={
                            "order_id": str(event.order_id),
                            "delivered_at": event.delivered_at.isoformat(),
                            "deadline": deadline.isoformat(),
                        },
                    )
                )
                self.log.warning("SLA breach", order_id=str(event.order_id))

    async def handle_order(self, event: BaseEvent) -> None:
        assert isinstance(event, OrderReceived)
        async with get_session() as s:
            count = (
                await s.execute(
                    text("SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '1 hour'")
                )
            ).scalar() or 0
        if count > 100:
            await self.bus.publish(
                AlertRaised(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    severity="low",
                    category="order_volume",
                    message=f"High order volume: {count} orders in last hour",
                    context={"count": count},
                )
            )
        self.log.info("Order tracked", order_id=str(event.order_id), hour_count=count)

    async def handle_production(self, event: BaseEvent) -> None:
        assert isinstance(event, ProductionComplete)
        async with get_session() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT start_time, estimated_completion FROM production_runs "
                        "WHERE run_id = :rid"
                    ),
                    {"rid": event.run_id},
                )
            ).first()
        if not row:
            return
        start, estimated = row
        if not estimated:
            return
        actual_h = (event.actual_completion - start).total_seconds() / 3600
        est_h = (estimated - start).total_seconds() / 3600
        if est_h > 0 and actual_h > est_h * 1.5:
            await self.bus.publish(
                AlertRaised(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    severity="medium",
                    category="production_delay",
                    message=f"Run {event.run_id} took {actual_h:.1f}h (est {est_h:.1f}h)",
                    context={
                        "run_id": str(event.run_id),
                        "actual_hours": actual_h,
                        "estimated_hours": est_h,
                    },
                )
            )
        self.log.info("Production tracked", run_id=str(event.run_id))

    async def handle_po(self, event: BaseEvent) -> None:
        """Vendor agent already wrote the PO — we only check lead-time compliance."""
        assert isinstance(event, POCreated)
        if event.eta and (event.eta - datetime.now(timezone.utc)).days > 7:
            await self.bus.publish(
                AlertRaised(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    severity="low",
                    category="vendor_lead_time",
                    message=f"Long lead time on PO {event.po_id}: ETA {event.eta}",
                    context={"po_id": str(event.po_id), "eta": event.eta.isoformat()},
                )
            )
        self.log.info("PO tracked", po_id=str(event.po_id))

    async def handle_stock_low(self, event: BaseEvent) -> None:
        assert isinstance(event, StockLow)
        async with get_session() as s:
            count = (
                await s.execute(
                    text(
                        "SELECT COUNT(*) FROM alerts "
                        "WHERE category = 'stock' AND context->>'sku' = :sku "
                        "AND created_at > NOW() - INTERVAL '7 days'"
                    ),
                    {"sku": event.sku},
                )
            ).scalar() or 0
        if count > 5:
            await self.bus.publish(
                AlertRaised(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    severity="high",
                    category="recurring_stockout",
                    message=f"SKU {event.sku} stock low {count} times in last 7 days",
                    context={"sku": event.sku, "count": count},
                )
            )
        self.log.warning("Stock low tracked", sku=event.sku, shortfall=event.shortfall_qty)


if __name__ == "__main__":
    import asyncio
    asyncio.run(MonitoringAgent().run())
