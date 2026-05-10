"""Canonical event schemas published to Redis Streams."""
from datetime import datetime, timezone
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class EventType(StrEnum):
    # Triggers
    ORDER_RECEIVED = "order_received"
    VENDOR_DELAY_ALERT = "vendor_delay_alert"
    IOT_SENSOR_READING = "iot_sensor_reading"
    # Orchestrator → agents
    CHECK_INVENTORY = "check_inventory"
    CHECK_TEAM_CAPACITY = "check_team_capacity"
    CREATE_PO = "create_po"
    SCHEDULE_PRODUCTION = "schedule_production"
    SCHEDULE_TRANSPORT = "schedule_transport"
    REQUEST_APPROVAL = "request_approval"
    # Agents → orchestrator
    INVENTORY_ALLOCATED = "inventory_allocated"
    STOCK_LOW = "stock_low"
    PO_CREATED = "po_created"
    PRODUCTION_SCHEDULED = "production_scheduled"
    PRODUCTION_COMPLETE = "production_complete"
    OUTBOUND_DELIVERY = "outbound_delivery"
    DELIVERY_CONFIRMED = "delivery_confirmed"
    PROCESS_COMPLETE = "process_complete"
    APPROVAL_DECISION = "approval_decision"
    # Oversight
    ALERT_RAISED = "alert_raised"
    ANOMALY_DETECTED = "anomaly_detected"
    POLICY_UPDATE_SUGGESTION = "policy_update_suggestion"


class BaseEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: UUID
    source_agent: str


# ── Trigger events ──────────────────────────────────────
class OrderReceived(BaseEvent):
    event_type: Literal[EventType.ORDER_RECEIVED] = EventType.ORDER_RECEIVED
    order_id: UUID
    customer_id: UUID
    sku: str
    quantity: int
    total_value: float
    deadline: datetime


class VendorDelayAlert(BaseEvent):
    event_type: Literal[EventType.VENDOR_DELAY_ALERT] = EventType.VENDOR_DELAY_ALERT
    po_id: UUID
    vendor_id: UUID
    original_eta: datetime
    new_eta: datetime
    delay_hours: int


# ── Approvals ──────────────────────────────────────────
class RequestApproval(BaseEvent):
    event_type: Literal[EventType.REQUEST_APPROVAL] = EventType.REQUEST_APPROVAL
    order_id: UUID
    sku: str
    quantity: int
    total_value: float
    reason: str


class ApprovalDecision(BaseEvent):
    event_type: Literal[EventType.APPROVAL_DECISION] = EventType.APPROVAL_DECISION
    order_id: UUID
    approved: bool
    notes: str = ""


# ── Inventory events ────────────────────────────────────
class CheckInventory(BaseEvent):
    event_type: Literal[EventType.CHECK_INVENTORY] = EventType.CHECK_INVENTORY
    order_id: UUID
    sku: str
    quantity: int


class InventoryAllocated(BaseEvent):
    event_type: Literal[EventType.INVENTORY_ALLOCATED] = EventType.INVENTORY_ALLOCATED
    order_id: UUID
    sku: str
    allocated_quantity: int


class StockLow(BaseEvent):
    event_type: Literal[EventType.STOCK_LOW] = EventType.STOCK_LOW
    order_id: UUID
    sku: str
    shortfall_qty: int
    urgency: Literal["normal", "expedited"] = "normal"
    preferred_vendors: list[UUID] = []


# ── Vendor events ───────────────────────────────────────
class CreatePO(BaseEvent):
    event_type: Literal[EventType.CREATE_PO] = EventType.CREATE_PO
    order_id: UUID
    sku: str
    quantity: int
    preferred_vendors: list[UUID] = []


class POCreated(BaseEvent):
    event_type: Literal[EventType.PO_CREATED] = EventType.PO_CREATED
    po_id: UUID
    order_id: UUID
    vendor_id: UUID
    sku: str
    quantity: int
    unit_price: float
    eta: datetime


# ── Production events ───────────────────────────────────
class ScheduleProduction(BaseEvent):
    event_type: Literal[EventType.SCHEDULE_PRODUCTION] = EventType.SCHEDULE_PRODUCTION
    order_id: UUID
    sku: str
    quantity: int
    deadline: datetime | None = None


class ProductionScheduled(BaseEvent):
    event_type: Literal[EventType.PRODUCTION_SCHEDULED] = EventType.PRODUCTION_SCHEDULED
    run_id: UUID
    order_id: UUID
    sku: str
    quantity: int
    line_id: str
    start_time: datetime
    estimated_completion: datetime


class ProductionComplete(BaseEvent):
    event_type: Literal[EventType.PRODUCTION_COMPLETE] = EventType.PRODUCTION_COMPLETE
    run_id: UUID
    order_id: UUID
    sku: str
    quantity: int
    actual_completion: datetime


class ProcessComplete(BaseEvent):
    event_type: Literal[EventType.PROCESS_COMPLETE] = EventType.PROCESS_COMPLETE
    order_id: UUID
    status: str = "completed"
    message: str | None = None


# ── Logistics events ────────────────────────────────────
class ScheduleTransport(BaseEvent):
    event_type: Literal[EventType.SCHEDULE_TRANSPORT] = EventType.SCHEDULE_TRANSPORT
    shipment_id: UUID
    order_id: UUID
    origin: str
    destination: str
    eta: datetime


class OutboundDelivery(BaseEvent):
    event_type: Literal[EventType.OUTBOUND_DELIVERY] = EventType.OUTBOUND_DELIVERY
    shipment_id: UUID
    order_id: UUID
    carrier_id: UUID
    waybill_no: str
    eta: datetime


class DeliveryConfirmed(BaseEvent):
    event_type: Literal[EventType.DELIVERY_CONFIRMED] = EventType.DELIVERY_CONFIRMED
    shipment_id: UUID
    order_id: UUID
    delivered_at: datetime
    pod_url: str


# ── Oversight events ────────────────────────────────────
class AlertRaised(BaseEvent):
    event_type: Literal[EventType.ALERT_RAISED] = EventType.ALERT_RAISED
    severity: Literal["low", "medium", "high", "critical"]
    category: str
    message: str
    context: dict = {}


# ── Stream registry ────────────────────────────────────
EVENT_STREAMS: dict[EventType, str] = {
    EventType.ORDER_RECEIVED: "triggers.orders",
    EventType.VENDOR_DELAY_ALERT: "triggers.vendor_delays",
    EventType.IOT_SENSOR_READING: "triggers.iot",
    EventType.REQUEST_APPROVAL: "agents.approvals.in",
    EventType.APPROVAL_DECISION: "agents.approvals.out",
    EventType.CHECK_INVENTORY: "agents.inventory.in",
    EventType.INVENTORY_ALLOCATED: "agents.inventory.out",
    EventType.STOCK_LOW: "orchestrator.in",
    EventType.CREATE_PO: "agents.vendor.in",
    EventType.PO_CREATED: "agents.vendor.out",
    EventType.SCHEDULE_PRODUCTION: "agents.production.in",
    EventType.PRODUCTION_SCHEDULED: "agents.production.scheduled",
    EventType.PRODUCTION_COMPLETE: "agents.production.complete",
    EventType.SCHEDULE_TRANSPORT: "agents.logistics.in",
    EventType.OUTBOUND_DELIVERY: "agents.logistics.out",
    EventType.DELIVERY_CONFIRMED: "agents.delivery.out",
    EventType.ALERT_RAISED: "oversight.alerts",
    EventType.ANOMALY_DETECTED: "oversight.anomalies",
    EventType.POLICY_UPDATE_SUGGESTION: "oversight.policy",
    EventType.CHECK_TEAM_CAPACITY: "agents.teams.in",
    EventType.PROCESS_COMPLETE: "orchestrator.complete",
}
