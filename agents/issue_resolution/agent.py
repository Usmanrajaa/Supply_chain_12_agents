"""Issue Resolution agent — LLM-driven incident response with tool use.

This agent:
  1. Listens for ALERT_RAISED events (severity >= medium)
  2. Gathers context using tools: order details, vendor alternatives, similar past incidents
  3. Uses an LLM to decide: auto-resolve, escalate to human, or wait
  4. Takes the decided action by publishing a new event
  5. Writes a structured incident record with full reasoning trace

The reasoning trace is what makes this demo-able: every decision is logged
with the inputs the LLM saw and the rationale it produced.
"""
from __future__ import annotations

import json
from typing import Any, Literal, TypedDict
from uuid import UUID, uuid4

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from sqlalchemy import text

from common.agents_base import BaseAgent
from common.bus.redis_bus import get_bus
from common.events.schemas import (
    AlertRaised,
    BaseEvent,
    CreatePO,
    EventType,
    RequestApproval,
)
from common.llm.client import get_llm
from common.storage.postgres import get_session


# ─── State definition ────────────────────────────────────
class IncidentState(TypedDict, total=False):
    # Inputs
    alert: AlertRaised
    # Gathered context
    order_context: dict
    vendor_alternatives: list[dict]
    similar_incidents: list[dict]
    # LLM output
    diagnosis: str
    recommended_action: Literal["create_po_backup", "escalate_human", "monitor", "ignore"]
    confidence: float
    reasoning: str
    requires_human: bool
    action_payload: dict


SYSTEM_PROMPT = """You are an expert supply chain incident resolver for a D2C e-commerce \
operations team. You receive alerts about stockouts, vendor delays, SLA breaches, and \
production issues. Your job is to decide what to do.

You have four possible actions:

1. create_po_backup  — Place an emergency PO with an alternative vendor. Use when:
   - The alert is a stockout or recurring stockout
   - At least one alternative vendor is available
   - Estimated cost is reasonable (within 30% premium over normal)

2. escalate_human    — Send to human review queue. Use when:
   - Severity is critical
   - No alternative vendors exist
   - Cost premium would be > 30%
   - The situation is novel (no similar past incidents)

3. monitor           — Continue watching. Use when:
   - The issue may auto-resolve (e.g., vendor confirmed delay is short)
   - Action would be premature

4. ignore            — Mark as non-actionable. Use when:
   - Alert is informational (e.g., low severity, normal operations)

You must respond with VALID JSON in this exact shape:
{
  "diagnosis": "1-2 sentence diagnosis of the situation",
  "recommended_action": "create_po_backup" | "escalate_human" | "monitor" | "ignore",
  "confidence": 0.0 to 1.0,
  "reasoning": "2-4 sentence explanation of WHY this action, citing specific facts from the context",
  "action_payload": { ... action-specific fields ... }
}

For create_po_backup, action_payload must include:
  { "vendor_id": "<uuid>", "sku": "<sku>", "quantity": <int>, "reason": "<short reason>" }

For escalate_human, action_payload must include:
  { "priority": "high"|"medium", "summary": "<what human needs to know>" }

For monitor and ignore, action_payload can be empty {}.
"""


# ─── Tool functions (gather context from DB) ─────────────
async def fetch_order_context(alert: AlertRaised) -> dict:
    """Pull the order, inventory, and any active POs related to this alert."""
    sku = alert.context.get("sku")
    if not sku:
        return {}

    async with get_session() as s:
        inv = (await s.execute(
            text("SELECT on_hand_qty, reserved_qty, reorder_point, safety_stock "
                 "FROM inventory WHERE sku = :sku"),
            {"sku": sku},
        )).first()

        pending_orders = (await s.execute(
            text("SELECT order_id, quantity, total_value, deadline "
                 "FROM orders WHERE sku = :sku AND status NOT IN ('delivered','rejected') "
                 "ORDER BY deadline ASC LIMIT 5"),
            {"sku": sku},
        )).fetchall()

        active_pos = (await s.execute(
            text("SELECT po_id, vendor_id, quantity, eta, status "
                 "FROM purchase_orders WHERE sku = :sku AND status != 'delivered' "
                 "ORDER BY eta ASC LIMIT 5"),
            {"sku": sku},
        )).fetchall()

    return {
        "sku": sku,
        "inventory": {
            "on_hand": inv[0] if inv else 0,
            "reserved": inv[1] if inv else 0,
            "reorder_point": inv[2] if inv else 0,
            "safety_stock": inv[3] if inv else 0,
        } if inv else None,
        "pending_orders": [
            {"order_id": str(r[0]), "quantity": r[1], "value": float(r[2]),
             "deadline": r[3].isoformat()}
            for r in pending_orders
        ],
        "active_purchase_orders": [
            {"po_id": str(r[0]), "vendor_id": str(r[1]), "quantity": r[2],
             "eta": r[3].isoformat() if r[3] else None, "status": r[4]}
            for r in active_pos
        ],
    }


async def fetch_vendor_alternatives(sku: str) -> list[dict]:
    """Find alternative vendors for this SKU, ranked by price + lead time."""
    async with get_session() as s:
        rows = (await s.execute(
            text(
                "SELECT vc.vendor_id, v.name, v.rating, v.avg_lead_time_h, vc.unit_price "
                "FROM vendor_contracts vc "
                "JOIN vendors v ON v.vendor_id = vc.vendor_id "
                "WHERE vc.sku = :sku AND v.active = TRUE "
                "  AND vc.valid_from <= CURRENT_DATE AND vc.valid_to >= CURRENT_DATE "
                "ORDER BY vc.unit_price ASC, v.avg_lead_time_h ASC"
            ),
            {"sku": sku},
        )).fetchall()
    return [
        {
            "vendor_id": str(r[0]),
            "name": r[1],
            "rating": float(r[2]) if r[2] else None,
            "lead_time_hours": r[3],
            "unit_price": float(r[4]),
        }
        for r in rows
    ]


async def fetch_similar_incidents(category: str, limit: int = 3) -> list[dict]:
    """Look up past incidents of the same category. In v2 this becomes vector RAG."""
    async with get_session() as s:
        rows = (await s.execute(
            text(
                "SELECT summary, action_taken, reasoning, confidence, resolved "
                "FROM incidents WHERE category = :cat AND resolved = TRUE "
                "ORDER BY created_at DESC LIMIT :lim"
            ),
            {"cat": category, "lim": limit},
        )).fetchall()
    return [
        {
            "summary": r[0],
            "action_taken": r[1],
            "reasoning": r[2],
            "confidence": float(r[3]) if r[3] else None,
            "resolved": r[4],
        }
        for r in rows
    ]


# ─── LangGraph nodes ─────────────────────────────────────
async def gather_context(state: IncidentState) -> IncidentState:
    alert = state["alert"]
    sku = alert.context.get("sku", "")

    state["order_context"] = await fetch_order_context(alert)
    state["vendor_alternatives"] = await fetch_vendor_alternatives(sku) if sku else []
    state["similar_incidents"] = await fetch_similar_incidents(alert.category)
    return state


async def reason_with_llm(state: IncidentState) -> IncidentState:
    alert = state["alert"]
    llm = get_llm(tier="reasoning", temperature=0.1)

    context_blob = {
        "alert": {
            "severity": alert.severity,
            "category": alert.category,
            "message": alert.message,
            "context": alert.context,
        },
        "order_context": state.get("order_context", {}),
        "vendor_alternatives": state.get("vendor_alternatives", []),
        "similar_past_incidents": state.get("similar_incidents", []),
    }

    user_prompt = (
        "An alert has been raised. Analyze the full situation and decide the action.\n\n"
        "FULL CONTEXT:\n"
        f"{json.dumps(context_blob, indent=2, default=str)}\n\n"
        "Respond with valid JSON only — no markdown, no preamble."
    )

    response = await llm.ainvoke([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_prompt),
    ])

    raw = response.content.strip()
    # Strip code fences if the model added them despite instruction
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        decision = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: escalate to human if LLM output is malformed
        decision = {
            "diagnosis": "LLM response could not be parsed as JSON.",
            "recommended_action": "escalate_human",
            "confidence": 0.0,
            "reasoning": f"Malformed LLM output: {raw[:200]}",
            "action_payload": {"priority": "medium", "summary": "Issue resolution agent failed to parse decision."},
        }

    state["diagnosis"] = decision.get("diagnosis", "")
    state["recommended_action"] = decision.get("recommended_action", "escalate_human")
    state["confidence"] = float(decision.get("confidence", 0.5))
    state["reasoning"] = decision.get("reasoning", "")
    state["action_payload"] = decision.get("action_payload", {})
    state["requires_human"] = state["recommended_action"] == "escalate_human"
    return state


async def execute_action(state: IncidentState) -> IncidentState:
    """Translate the LLM's decision into an actual event on the bus."""
    alert = state["alert"]
    action = state["recommended_action"]
    payload = state["action_payload"]
    bus = await get_bus()

    if action == "create_po_backup":
        try:
            vendor_id = UUID(payload["vendor_id"])
            sku = payload["sku"]
            qty = int(payload["quantity"])
            await bus.publish(
                CreatePO(
                    correlation_id=alert.correlation_id,
                    source_agent="issue_resolution",
                    order_id=uuid4(),  # synthetic order id for the backup PO
                    sku=sku,
                    quantity=qty,
                    preferred_vendors=[vendor_id],
                )
            )
        except (KeyError, ValueError, TypeError) as e:
            # If the LLM picked an action but malformed the payload, escalate
            state["recommended_action"] = "escalate_human"
            state["requires_human"] = True
            state["reasoning"] += f" [Auto-escalated: payload error {e}]"
            await bus.publish(
                RequestApproval(
                    correlation_id=alert.correlation_id,
                    source_agent="issue_resolution",
                    order_id=uuid4(),
                    sku=alert.context.get("sku", "UNKNOWN"),
                    quantity=0,
                    total_value=0.0,
                    reason=f"Issue resolution agent: {state['reasoning']}",
                )
            )

    elif action == "escalate_human":
        await bus.publish(
            RequestApproval(
                correlation_id=alert.correlation_id,
                source_agent="issue_resolution",
                order_id=uuid4(),
                sku=alert.context.get("sku", "UNKNOWN"),
                quantity=0,
                total_value=0.0,
                reason=f"{payload.get('summary', 'Manual review required')}: {state['reasoning']}",
            )
        )
    # monitor and ignore: no action, just record

    return state


async def persist_incident(state: IncidentState) -> IncidentState:
    """Write the incident record so the dashboard can show the reasoning."""
    alert = state["alert"]
    async with get_session() as s:
        await s.execute(
            text(
                "INSERT INTO incidents "
                "(alert_id, correlation_id, severity, category, summary, "
                " reasoning, action_taken, action_payload, confidence, requires_human, resolved) "
                "VALUES (:aid, :cid, :sev, :cat, :sum, :reason, :action, "
                "        CAST(:payload AS JSONB), :conf, :human, :resolved)"
            ),
            {
                "aid": alert.event_id,
                "cid": alert.correlation_id,
                "sev": alert.severity,
                "cat": alert.category,
                "sum": state.get("diagnosis", alert.message),
                "reason": state.get("reasoning", ""),
                "action": state["recommended_action"],
                "payload": json.dumps(state.get("action_payload", {})),
                "conf": state.get("confidence", 0.5),
                "human": state.get("requires_human", False),
                "resolved": state["recommended_action"] in ("create_po_backup", "ignore"),
            },
        )
    return state


def build_graph():
    g: StateGraph = StateGraph(IncidentState)
    g.add_node("gather_context", gather_context)
    g.add_node("reason_with_llm", reason_with_llm)
    g.add_node("execute_action", execute_action)
    g.add_node("persist_incident", persist_incident)

    g.set_entry_point("gather_context")
    g.add_edge("gather_context", "reason_with_llm")
    g.add_edge("reason_with_llm", "execute_action")
    g.add_edge("execute_action", "persist_incident")
    g.add_edge("persist_incident", END)
    return g.compile()


# ─── Agent class ────────────────────────────────────────
class IssueResolutionAgent(BaseAgent):
    name = "issue_resolution"

    async def setup(self) -> None:
        self.graph = build_graph()
        self.log.info("Issue resolution agent ready (LLM-powered)")

    def subscriptions(self):
        return {EventType.ALERT_RAISED: (AlertRaised, self.handle_alert)}

    async def handle_alert(self, event: BaseEvent) -> None:
        assert isinstance(event, AlertRaised)
        # Only act on medium+ severity to avoid burning tokens on noise
        if event.severity == "low":
            self.log.info("Ignoring low-severity alert", category=event.category)
            return

        self.log.info(
            "Processing alert",
            severity=event.severity,
            category=event.category,
            correlation_id=str(event.correlation_id),
        )
        await self.graph.ainvoke({"alert": event})


if __name__ == "__main__":
    import asyncio
    asyncio.run(IssueResolutionAgent().run())
