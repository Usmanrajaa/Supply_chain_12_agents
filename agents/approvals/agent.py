"""Approvals agent — handles high-value order approvals.

For the demo, auto-approves anything under $100k; would route to a human
review queue in production. Records every decision in the approvals table.
"""
from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    ApprovalDecision,
    BaseEvent,
    EventType,
    RequestApproval,
)
from common.storage.postgres import get_session

AUTO_APPROVE_CEILING = 100_000.0


class ApprovalsAgent(BaseAgent):
    name = "approvals"

    async def setup(self) -> None:
        self.bus = await get_bus()
        self.log.info("Approvals agent started")

    def subscriptions(self):
        return {EventType.REQUEST_APPROVAL: (RequestApproval, self.handle_request_approval)}

    async def handle_request_approval(self, event: BaseEvent) -> None:
        assert isinstance(event, RequestApproval)
        approved = event.total_value <= AUTO_APPROVE_CEILING
        notes = (
            f"Auto-approved under ${AUTO_APPROVE_CEILING:,.0f} ceiling"
            if approved else "Auto-rejected: exceeds auto-approve ceiling, human review required"
        )

        async with get_session() as s:
            await s.execute(
                text(
                    "INSERT INTO approvals (order_id, decided_at, decision, reason, notes) "
                    "VALUES (:oid, NOW(), :decision, :reason, :notes)"
                ),
                {
                    "oid": event.order_id,
                    "decision": "approved" if approved else "rejected",
                    "reason": event.reason,
                    "notes": notes,
                },
            )

        await self.bus.publish(
            ApprovalDecision(
                correlation_id=event.correlation_id,
                source_agent=self.name,
                order_id=event.order_id,
                approved=approved,
                notes=notes,
            )
        )
        self.log.info(
            "Approval decision",
            order_id=str(event.order_id),
            approved=approved,
            value=event.total_value,
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(ApprovalsAgent().run())
