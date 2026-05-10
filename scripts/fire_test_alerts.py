"""Fire test alerts to trigger the AI issue resolution agent.

Run from inside the api container:
    docker compose exec api python -m scripts.fire_test_alerts

Each alert exercises a different decision path the LLM should reach.
"""
import asyncio
from uuid import uuid4

from common.bus.redis_bus import get_bus
from common.events.schemas import AlertRaised


async def main() -> None:
    bus = await get_bus()

    # Scenario 1: recurring stockout — should auto-resolve with backup vendor
    await bus.publish(
        AlertRaised(
            correlation_id=uuid4(),
            source_agent="test",
            severity="high",
            category="recurring_stockout",
            message="SKU SKU-002 stock low 6 times in last 7 days",
            context={"sku": "SKU-002", "count": 6},
        )
    )
    print("✓ Fired: recurring_stockout for SKU-002 (expect: create_po_backup)")

    # Scenario 2: SLA breach — should escalate (no PO action makes sense)
    await bus.publish(
        AlertRaised(
            correlation_id=uuid4(),
            source_agent="test",
            severity="medium",
            category="sla_breach",
            message="Order delivered 18 hours after deadline",
            context={"order_id": str(uuid4()), "delay_hours": 18},
        )
    )
    print("✓ Fired: sla_breach (expect: escalate_human or monitor)")

    # Scenario 3: critical alert with no vendor alternatives — must escalate
    await bus.publish(
        AlertRaised(
            correlation_id=uuid4(),
            source_agent="test",
            severity="critical",
            category="production_failure",
            message="Production line LINE-A halted: equipment failure",
            context={"line_id": "LINE-A", "downtime_min": 240},
        )
    )
    print("✓ Fired: critical production_failure (expect: escalate_human)")

    # Scenario 4: low severity — agent should ignore (cost optimisation)
    await bus.publish(
        AlertRaised(
            correlation_id=uuid4(),
            source_agent="test",
            severity="low",
            category="order_volume",
            message="Order volume slightly elevated",
            context={"count": 110},
        )
    )
    print("✓ Fired: low-severity (expect: ignored, no LLM call)")

    print("\nWait ~5 seconds, then:")
    print("  curl http://localhost:8000/api/incidents")
    print("Or open the dashboard.")

    await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
