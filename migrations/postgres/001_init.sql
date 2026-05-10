-- Initial schema for supply-chain agents
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Orders
CREATE TABLE orders (
    order_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id     UUID NOT NULL,
    sku             VARCHAR(64) NOT NULL,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    total_value     NUMERIC(12,2) NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'received',
    deadline        TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_sku ON orders(sku);

-- Inventory
CREATE TABLE inventory (
    sku             VARCHAR(64) PRIMARY KEY,
    on_hand_qty     INTEGER NOT NULL DEFAULT 0,
    reserved_qty    INTEGER NOT NULL DEFAULT 0,
    reorder_point   INTEGER NOT NULL DEFAULT 10,
    safety_stock    INTEGER NOT NULL DEFAULT 5,
    warehouse_id    VARCHAR(64),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (on_hand_qty >= reserved_qty)
);

-- Vendors
CREATE TABLE vendors (
    vendor_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(128) NOT NULL,
    rating          NUMERIC(3,2),
    avg_lead_time_h INTEGER,
    contact_email   VARCHAR(128),
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE vendor_contracts (
    contract_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id       UUID NOT NULL REFERENCES vendors(vendor_id),
    sku             VARCHAR(64) NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    UNIQUE (vendor_id, sku, valid_from)
);

-- Purchase orders (no FK on vendor_id for prototype flexibility)
CREATE TABLE purchase_orders (
    po_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID REFERENCES orders(order_id),
    vendor_id       UUID NOT NULL,
    sku             VARCHAR(64) NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'created',
    eta             TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_po_status ON purchase_orders(status);

-- Production runs
CREATE TABLE production_runs (
    run_id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id             UUID REFERENCES orders(order_id),
    sku                  VARCHAR(64) NOT NULL,
    quantity             INTEGER NOT NULL,
    line_id              VARCHAR(32) NOT NULL,
    start_time           TIMESTAMPTZ NOT NULL,
    estimated_completion TIMESTAMPTZ,
    actual_completion    TIMESTAMPTZ,
    status               VARCHAR(32) NOT NULL DEFAULT 'scheduled'
);

-- Shipments
CREATE TABLE shipments (
    shipment_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID REFERENCES orders(order_id),
    carrier_id      UUID,
    waybill_no      VARCHAR(64) UNIQUE,
    origin          VARCHAR(128),
    destination     VARCHAR(128),
    eta             TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    pod_url         VARCHAR(256),
    status          VARCHAR(32) NOT NULL DEFAULT 'planned'
);

-- Alerts
CREATE TABLE alerts (
    alert_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    severity        VARCHAR(16) NOT NULL,
    category        VARCHAR(64) NOT NULL,
    message         TEXT NOT NULL,
    context         JSONB NOT NULL DEFAULT '{}',
    correlation_id  UUID,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);
CREATE INDEX idx_alerts_unresolved ON alerts(resolved) WHERE resolved = FALSE;

-- Approvals
CREATE TABLE approvals (
    approval_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID REFERENCES orders(order_id),
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at      TIMESTAMPTZ,
    decision        VARCHAR(16),
    reason          TEXT,
    notes           TEXT
);

-- pgvector for RAG
CREATE TABLE policy_embeddings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_type   VARCHAR(32) NOT NULL,
    chunk_text      TEXT NOT NULL,
    embedding       VECTOR(1536),
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_policy_emb_hnsw ON policy_embeddings USING hnsw (embedding vector_cosine_ops);

-- Seed data inline (so docker-entrypoint-initdb.d gets a complete demo on first boot)
INSERT INTO inventory (sku, on_hand_qty, reorder_point, safety_stock, warehouse_id) VALUES
    ('SKU-001', 100, 20, 10, 'WH-DEL'),
    ('SKU-002',   5, 15,  8, 'WH-DEL'),
    ('SKU-003',  50, 25, 12, 'WH-MUM'),
    ('MFG-001',   0, 10,  5, 'WH-DEL')
ON CONFLICT (sku) DO NOTHING;

INSERT INTO vendors (vendor_id, name, rating, avg_lead_time_h, contact_email) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Acme Components', 4.7, 48, 'sales@acme.example'),
    ('22222222-2222-2222-2222-222222222222', 'Bharat Supplies',  4.3, 24, 'orders@bharat.example')
ON CONFLICT DO NOTHING;

INSERT INTO vendor_contracts (vendor_id, sku, unit_price, valid_from, valid_to) VALUES
    ('11111111-1111-1111-1111-111111111111', 'SKU-001', 12.50, '2025-01-01', '2027-12-31'),
    ('22222222-2222-2222-2222-222222222222', 'SKU-002',  8.75, '2025-01-01', '2027-12-31'),
    ('11111111-1111-1111-1111-111111111111', 'SKU-003', 15.00, '2025-01-01', '2027-12-31')
ON CONFLICT DO NOTHING;
