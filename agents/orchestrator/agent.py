"""Orchestrator agent — routes events between trigger sources and domain agents."""
from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.config.settings import get_settings
from common.events.schemas import (
    ApprovalDecision,
    BaseEvent,
    CheckInventory,
    CreatePO,
    EventType,
    OrderReceived,
    ProcessComplete,
    RequestApproval,
    ScheduleProduction,
    StockLow,
)
from common.storage.postgres import get_session

settings = get_settings()


class OrchestratorAgent(BaseAgent):
    name = "orchestrator"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Orchestrator started")

    def subscriptions(self):
        return {
            EventType.ORDER_RECEIVED: (OrderReceived, self.handle_order),
            EventType.STOCK_LOW: (StockLow, self.handle_stock_low),
            EventType.APPROVAL_DECISION: (ApprovalDecision, self.handle_approval_decision),
            EventType.PROCESS_COMPLETE: (ProcessComplete, self.handle_process_complete),
        }

    async def handle_order(self, event: BaseEvent) -> None:
        assert isinstance(event, OrderReceived)
        async with get_session() as session:
            await session.execute(
                text(
                    "INSERT INTO orders (order_id, customer_id, sku, quantity, total_value, status, deadline) "
                    "VALUES (:order_id, :customer_id, :sku, :quantity, :total_value, 'received', :deadline) "
                    "ON CONFLICT (order_id) DO NOTHING"
                ),
                {
                    "order_id": event.order_id,
                    "customer_id": event.customer_id,
                    "sku": event.sku,
                    "quantity": event.quantity,
                    "total_value": event.total_value,
                    "deadline": event.deadline,
                },
            )
        self.log.info("Order stored", order_id=str(event.order_id), value=event.total_value)

        # High-value orders go through approvals first
        if event.total_value >= settings.high_value_threshold:
            await self.bus.publish(
                RequestApproval(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    order_id=event.order_id,
                    sku=event.sku,
                    quantity=event.quantity,
                    total_value=event.total_value,
                    reason=f"Order value ${event.total_value} exceeds threshold ${settings.high_value_threshold}",
                )
            )
            self.log.info("Routed to approvals (high-value)", order_id=str(event.order_id))
            return

        # Standard orders go straight to inventory
        await self.bus.publish(
            CheckInventory(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                order_id=event.order_id,
                sku=event.sku,
                quantity=event.quantity,
            )
        )
        self.log.info("Forwarded to inventory", order_id=str(event.order_id))

    async def handle_approval_decision(self, event: BaseEvent) -> None:
        assert isinstance(event, ApprovalDecision)
        async with get_session() as session:
            row = (
                await session.execute(
                    text("SELECT sku, quantity FROM orders WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )
            ).first()
            if not row:
                self.log.warning("Order not found for approval decision", order_id=str(event.order_id))
                return
            sku, qty = row

            if event.approved:
                await session.execute(
                    text("UPDATE orders SET status = 'approved', updated_at = NOW() WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )
            else:
                await session.execute(
                    text("UPDATE orders SET status = 'rejected', updated_at = NOW() WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )

        if event.approved:
            await self.bus.publish(
                CheckInventory(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    order_id=event.order_id,
                    sku=sku,
                    quantity=qty,
                )
            )
            self.log.info("Approved → inventory", order_id=str(event.order_id))
        else:
            self.log.info("Order rejected", order_id=str(event.order_id), notes=event.notes)

    async def handle_stock_low(self, event: BaseEvent) -> None:
        assert isinstance(event, StockLow)
        # MFG-prefixed SKUs are manufactured in-house; everything else is procured
        if event.sku.startswith("MFG"):
            await self.bus.publish(
                ScheduleProduction(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    order_id=event.order_id,
                    sku=event.sku,
                    quantity=event.shortfall_qty,
                )
            )
            self.log.info("Routed to production", sku=event.sku, order_id=str(event.order_id))
        else:
            await self.bus.publish(
                CreatePO(
                    correlation_id=event.correlation_id,
                    source_agent=self.name,
                    order_id=event.order_id,
                    sku=event.sku,
                    quantity=event.shortfall_qty,
                )
            )
            self.log.info("Routed to vendor", sku=event.sku, order_id=str(event.order_id))

    async def handle_process_complete(self, event: BaseEvent) -> None:
        assert isinstance(event, ProcessComplete)
        self.log.info(
            "Process complete",
            order_id=str(event.order_id),
            status=event.status,
            message=event.message,
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(OrchestratorAgent().run())
