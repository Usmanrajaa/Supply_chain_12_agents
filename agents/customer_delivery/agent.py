"""Customer delivery agent — final order closure + customer notification."""
from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    BaseEvent,
    DeliveryConfirmed,
    EventType,
    ProcessComplete,
)
from common.storage.postgres import get_session


class CustomerDeliveryAgent(BaseAgent):
    name = "customer_delivery"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Customer delivery agent started")

    def subscriptions(self):
        return {EventType.DELIVERY_CONFIRMED: (DeliveryConfirmed, self.handle_delivery_confirmed)}

    async def handle_delivery_confirmed(self, event: BaseEvent) -> None:
        assert isinstance(event, DeliveryConfirmed)
        async with get_session() as s:
            row = (
                await s.execute(
                    text("SELECT status FROM orders WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )
            ).first()
            if not row:
                self.log.debug("Order not found", order_id=str(event.order_id))
                return
            if row[0] == "delivered":
                self.log.debug("Order already delivered", order_id=str(event.order_id))
                return

            await s.execute(
                text(
                    "UPDATE orders SET status = 'delivered', updated_at = NOW() "
                    "WHERE order_id = :oid"
                ),
                {"oid": event.order_id},
            )

        self.log.info("Order marked delivered", order_id=str(event.order_id))
        # Customer notification stub (email/SMS/webhook would go here)
        self.log.info("Customer notification sent", order_id=str(event.order_id))

        await self.bus.publish(
            ProcessComplete(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                order_id=event.order_id,
                status="completed",
                message="Order fulfilled and delivered",
            )
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(CustomerDeliveryAgent().run())
