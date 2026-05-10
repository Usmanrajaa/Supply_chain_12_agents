"""Production agent — schedules and completes production runs."""
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    BaseEvent,
    EventType,
    ProductionComplete,
    ProductionScheduled,
    ScheduleProduction,
)
from common.storage.postgres import get_session


class ProductionAgent(BaseAgent):
    name = "production"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Production agent started")

    def subscriptions(self):
        return {EventType.SCHEDULE_PRODUCTION: (ScheduleProduction, self.handle_schedule)}

    async def handle_schedule(self, event: BaseEvent) -> None:
        assert isinstance(event, ScheduleProduction)
        run_id = uuid4()
        start_time = datetime.now(timezone.utc)
        estimated_completion = start_time + timedelta(hours=2)

        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO production_runs "
                    "(run_id, order_id, sku, quantity, line_id, start_time, estimated_completion, status) "
                    "VALUES (:run_id, :order_id, :sku, :qty, :line_id, :start, :est, 'scheduled')"
                ),
                {
                    "run_id": run_id,
                    "order_id": event.order_id,
                    "sku": event.sku,
                    "qty": event.quantity,
                    "line_id": "LINE-A",
                    "start": start_time,
                    "est": estimated_completion,
                },
            )

        await self.bus.publish(
            ProductionScheduled(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                run_id=run_id,
                order_id=event.order_id,
                sku=event.sku,
                quantity=event.quantity,
                line_id="LINE-A",
                start_time=start_time,
                estimated_completion=estimated_completion,
            )
        )
        self.log.info("Production scheduled", run_id=str(run_id), sku=event.sku, qty=event.quantity)

        # Simulate manufacturing time
        await asyncio.sleep(5)

        actual_completion = datetime.now(timezone.utc)
        async with get_session() as s:
            await s.execute(
                text(
                    "UPDATE production_runs "
                    "SET status = 'completed', actual_completion = :ac "
                    "WHERE run_id = :run_id"
                ),
                {"ac": actual_completion, "run_id": run_id},
            )
            # Top up on_hand inventory for the manufactured SKU
            await s.execute(
                text(
                    "INSERT INTO inventory (sku, on_hand_qty) VALUES (:sku, :qty) "
                    "ON CONFLICT (sku) DO UPDATE "
                    "SET on_hand_qty = inventory.on_hand_qty + :qty, updated_at = NOW()"
                ),
                {"sku": event.sku, "qty": event.quantity},
            )

        await self.bus.publish(
            ProductionComplete(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                run_id=run_id,
                order_id=event.order_id,
                sku=event.sku,
                quantity=event.quantity,
                actual_completion=actual_completion,
            )
        )
        self.log.info("Production complete", run_id=str(run_id))


if __name__ == "__main__":
    import asyncio
    asyncio.run(ProductionAgent().run())
