"""Inventory agent - LangGraph state machine for stock allocation."""
from __future__ import annotations

from typing import Literal, TypedDict
from uuid import UUID

import structlog
from langgraph.graph import END, StateGraph
from sqlalchemy import text

from common.agents_base import BaseAgent, EventHandler
from common.bus.redis_bus import get_bus
from common.config.settings import get_settings
from common.events.schemas import (
    AlertRaised,
    BaseEvent,
    CheckInventory,
    DeliveryConfirmed,
    EventType,
    InventoryAllocated,
    StockLow,
)
from common.storage.postgres import get_session

logger = structlog.get_logger(__name__)
settings = get_settings()


class InventoryState(TypedDict, total=False):
    correlation_id: UUID
    order_id: UUID
    sku: str
    requested_qty: int
    on_hand_qty: int
    reserved_qty: int
    available_qty: int
    reorder_point: int
    safety_stock: int
    decision: Literal["allocate", "replenish"]
    shortfall: int
    raise_alert: bool


# ── Graph nodes ────────────────────────────────────────
async def parse_event(state: InventoryState) -> InventoryState:
    logger.info("inv.parse", order_id=str(state["order_id"]), sku=state["sku"])
    return state


async def query_stock(state: InventoryState) -> InventoryState:
    async with get_session() as s:
        result = await s.execute(
            text(
                "SELECT on_hand_qty, reserved_qty, reorder_point, safety_stock "
                "FROM inventory WHERE sku = :sku"
            ),
            {"sku": state["sku"]},
        )
        row = result.first()

    if row is None:
        state.update(
            on_hand_qty=0, reserved_qty=0,
            reorder_point=0, safety_stock=0, available_qty=0,
        )
    else:
        on_hand, reserved, reorder, safety = row
        state.update(
            on_hand_qty=on_hand,
            reserved_qty=reserved,
            available_qty=on_hand - reserved,
            reorder_point=reorder,
            safety_stock=safety,
        )
    return state


async def check_sufficiency(state: InventoryState) -> InventoryState:
    if state["available_qty"] >= state["requested_qty"]:
        state["decision"] = "allocate"
    else:
        state["decision"] = "replenish"
        state["shortfall"] = state["requested_qty"] - state["available_qty"]
    return state


async def allocate(state: InventoryState) -> InventoryState:
    async with get_session() as s:
        await s.execute(
            text(
                "UPDATE inventory SET reserved_qty = reserved_qty + :qty, "
                "updated_at = NOW() WHERE sku = :sku"
            ),
            {"qty": state["requested_qty"], "sku": state["sku"]},
        )

    bus = await get_bus()
    await bus.publish(
        InventoryAllocated(
            correlation_id=state["correlation_id"],
            source_agent="inventory",
            order_id=state["order_id"],
            sku=state["sku"],
            allocated_quantity=state["requested_qty"],
        )
    )
    return state


async def trigger_replenishment(state: InventoryState) -> InventoryState:
    bus = await get_bus()
    await bus.publish(
        StockLow(
            correlation_id=state["correlation_id"],
            source_agent="inventory",
            order_id=state["order_id"],
            sku=state["sku"],
            shortfall_qty=state["shortfall"],
            urgency="normal",
        )
    )
    return state


async def check_safety_stock(state: InventoryState) -> InventoryState:
    state["raise_alert"] = state["available_qty"] < state["safety_stock"]
    return state


async def raise_alert(state: InventoryState) -> InventoryState:
    if state.get("raise_alert"):
        bus = await get_bus()
        await bus.publish(
            AlertRaised(
                correlation_id=state["correlation_id"],
                source_agent="inventory",
                severity="medium",
                category="stock",
                message=f"Stock for {state['sku']} below safety stock",
                context={
                    "sku": state["sku"],
                    "available": state["available_qty"],
                    "safety_stock": state["safety_stock"],
                },
            )
        )
    return state


def build_graph():
    g: StateGraph = StateGraph(InventoryState)
    g.add_node("parse", parse_event)
    g.add_node("query_stock", query_stock)
    g.add_node("check_sufficiency", check_sufficiency)
    g.add_node("allocate", allocate)
    g.add_node("trigger_replenishment", trigger_replenishment)
    g.add_node("check_safety_stock", check_safety_stock)
    g.add_node("raise_alert", raise_alert)

    g.set_entry_point("parse")
    g.add_edge("parse", "query_stock")
    g.add_edge("query_stock", "check_sufficiency")
    g.add_conditional_edges(
        "check_sufficiency",
        lambda s: s["decision"],
        {"allocate": "allocate", "replenish": "trigger_replenishment"},
    )
    g.add_edge("allocate", "check_safety_stock")
    g.add_edge("trigger_replenishment", "check_safety_stock")
    g.add_edge("check_safety_stock", "raise_alert")
    g.add_edge("raise_alert", END)
    return g.compile()


class InventoryAgent(BaseAgent):
    name = "inventory"

    async def setup(self) -> None:
        self.graph = build_graph()
        self.log.info("Inventory agent ready")

    def subscriptions(self) -> dict[EventType, tuple[type[BaseEvent], EventHandler]]:
        return {
            EventType.CHECK_INVENTORY: (CheckInventory, self.handle_check_inventory),
            EventType.DELIVERY_CONFIRMED: (DeliveryConfirmed, self.handle_delivery_confirmed),
        }

    async def handle_check_inventory(self, event: BaseEvent) -> None:
        assert isinstance(event, CheckInventory)
        initial: InventoryState = {
            "correlation_id": event.correlation_id,
            "order_id": event.order_id,
            "sku": event.sku,
            "requested_qty": event.quantity,
        }
        await self.graph.ainvoke(initial)

    async def handle_delivery_confirmed(self, event: BaseEvent) -> None:
        """Release reservation + decrement on-hand when goods are delivered."""
        assert isinstance(event, DeliveryConfirmed)
        async with get_session() as s:
            row = (
                await s.execute(
                    text("SELECT sku, quantity FROM orders WHERE order_id = :oid"),
                    {"oid": event.order_id},
                )
            ).first()
            if not row:
                self.log.debug("Order not found for delivery", order_id=str(event.order_id))
                return
            sku, qty = row
            await s.execute(
                text(
                    "UPDATE inventory "
                    "SET on_hand_qty = GREATEST(on_hand_qty - :q, 0), "
                    "    reserved_qty = GREATEST(reserved_qty - :q, 0), "
                    "    updated_at = NOW() "
                    "WHERE sku = :sku"
                ),
                {"q": qty, "sku": sku},
            )
        self.log.info("Inventory depleted on delivery", order_id=str(event.order_id), sku=sku, qty=qty)


if __name__ == "__main__":
    import asyncio
    asyncio.run(InventoryAgent().run())
