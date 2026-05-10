"""Logistics agent — creates outbound shipments + handles inbound POs."""
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    BaseEvent,
    DeliveryConfirmed,
    EventType,
    InventoryAllocated,
    OutboundDelivery,
    POCreated,
    ProductionComplete,
)
from common.storage.postgres import get_session


class LogisticsAgent(BaseAgent):
    name = "logistics"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Logistics agent started")

    def subscriptions(self):
        return {
            EventType.INVENTORY_ALLOCATED: (InventoryAllocated, self.handle_inventory_allocated),
            EventType.PRODUCTION_COMPLETE: (ProductionComplete, self.handle_production_complete),
            EventType.PO_CREATED: (POCreated, self.handle_po_created),
        }

    async def _ship(self, correlation_id, order_id) -> None:
        shipment_id = uuid4()
        carrier_id = uuid4()
        waybill = f"WAY-{shipment_id.hex[:8].upper()}"
        eta = datetime.now(timezone.utc) + timedelta(days=2)

        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO shipments "
                    "(shipment_id, order_id, carrier_id, waybill_no, eta, status) "
                    "VALUES (:sid, :oid, :cid, :wb, :eta, 'in_transit')"
                ),
                {"sid": shipment_id, "oid": order_id, "cid": carrier_id, "wb": waybill, "eta": eta},
            )

        await self.bus.publish(
            OutboundDelivery(
                correlation_id=correlation_id,
                source_agent=self.name,
                shipment_id=shipment_id,
                order_id=order_id,
                carrier_id=carrier_id,
                waybill_no=waybill,
                eta=eta,
            )
        )
        self.log.info("Outbound shipment created", shipment_id=str(shipment_id), order_id=str(order_id))

        # Simulate transit
        await asyncio.sleep(3)

        delivered_at = datetime.now(timezone.utc)
        pod_url = f"s3://supply-chain-docs/pod/{shipment_id}.pdf"
        async with get_session() as s:
            await s.execute(
                text(
                    "UPDATE shipments SET status = 'delivered', delivered_at = :da, pod_url = :pod "
                    "WHERE shipment_id = :sid"
                ),
                {"da": delivered_at, "pod": pod_url, "sid": shipment_id},
            )

        await self.bus.publish(
            DeliveryConfirmed(
                correlation_id=correlation_id,
                source_agent=self.name,
                shipment_id=shipment_id,
                order_id=order_id,
                delivered_at=delivered_at,
                pod_url=pod_url,
            )
        )
        self.log.info("Delivery confirmed", shipment_id=str(shipment_id))

    async def handle_inventory_allocated(self, event: BaseEvent) -> None:
        assert isinstance(event, InventoryAllocated)
        await self._ship(event.correlation_id, event.order_id)

    async def handle_production_complete(self, event: BaseEvent) -> None:
        assert isinstance(event, ProductionComplete)
        await self._ship(event.correlation_id, event.order_id)

    async def handle_po_created(self, event: BaseEvent) -> None:
        """When PO arrives at warehouse, top up inventory."""
        assert isinstance(event, POCreated)
        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO inventory (sku, on_hand_qty) VALUES (:sku, :qty) "
                    "ON CONFLICT (sku) DO UPDATE "
                    "SET on_hand_qty = inventory.on_hand_qty + :qty, updated_at = NOW()"
                ),
                {"sku": event.sku, "qty": event.quantity},
            )
        self.log.info("Inbound goods received", po_id=str(event.po_id), sku=event.sku, qty=event.quantity)


if __name__ == "__main__":
    import asyncio
    asyncio.run(LogisticsAgent().run())
