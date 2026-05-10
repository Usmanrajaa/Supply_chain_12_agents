"""Vendor agent — picks vendor from contracts, persists PO, emits PO_CREATED."""
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import BaseEvent, CreatePO, EventType, POCreated
from common.storage.postgres import get_session

# Fallback vendor if no contract exists for the SKU (matches seed data)
DEFAULT_VENDOR_ID = UUID("11111111-1111-1111-1111-111111111111")
DEFAULT_PRICE = 10.0
DEFAULT_LEAD_TIME_H = 48


class VendorAgent(BaseAgent):
    name = "vendor"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Vendor agent started")

    def subscriptions(self):
        return {EventType.CREATE_PO: (CreatePO, self.handle_create_po)}

    async def _select_vendor(self, sku: str) -> tuple[UUID, float, int]:
        """Select best vendor for SKU. Returns (vendor_id, unit_price, lead_time_h)."""
        async with get_session() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT vc.vendor_id, vc.unit_price, COALESCE(v.avg_lead_time_h, 48) "
                        "FROM vendor_contracts vc "
                        "JOIN vendors v ON v.vendor_id = vc.vendor_id "
                        "WHERE vc.sku = :sku AND v.active = TRUE "
                        "  AND vc.valid_from <= CURRENT_DATE AND vc.valid_to >= CURRENT_DATE "
                        "ORDER BY vc.unit_price ASC LIMIT 1"
                    ),
                    {"sku": sku},
                )
            ).first()
        if row:
            vendor_id, unit_price, lead_h = row
            return vendor_id, float(unit_price), int(lead_h)
        return DEFAULT_VENDOR_ID, DEFAULT_PRICE, DEFAULT_LEAD_TIME_H

    async def handle_create_po(self, event: BaseEvent) -> None:
        assert isinstance(event, CreatePO)
        vendor_id, unit_price, lead_h = await self._select_vendor(event.sku)
        eta = datetime.now(timezone.utc) + timedelta(hours=lead_h)
        po_id = uuid4()

        # Persist PO before publishing event (write-then-emit pattern)
        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO purchase_orders "
                    "(po_id, order_id, vendor_id, sku, quantity, unit_price, eta, status) "
                    "VALUES (:po_id, :order_id, :vendor_id, :sku, :qty, :price, :eta, 'created')"
                ),
                {
                    "po_id": po_id,
                    "order_id": event.order_id,
                    "vendor_id": vendor_id,
                    "sku": event.sku,
                    "qty": event.quantity,
                    "price": unit_price,
                    "eta": eta,
                },
            )

        await self.bus.publish(
            POCreated(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                po_id=po_id,
                order_id=event.order_id,
                vendor_id=vendor_id,
                sku=event.sku,
                quantity=event.quantity,
                unit_price=unit_price,
                eta=eta,
            )
        )
        self.log.info(
            "PO created",
            po_id=str(po_id),
            vendor_id=str(vendor_id),
            sku=event.sku,
            qty=event.quantity,
            unit_price=unit_price,
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(VendorAgent().run())
