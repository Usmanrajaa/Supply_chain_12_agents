-- ─── Incidents table for AI-driven issue resolution ──────────────
-- Stores every alert that the issue_resolution agent processes,
-- with its reasoning trace and actions taken. This is what the
-- dashboard renders to show "AI decided X because Y".

CREATE TABLE IF NOT EXISTS incidents (
    incident_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_id        UUID,
    correlation_id  UUID,
    severity        VARCHAR(16) NOT NULL,
    category        VARCHAR(64) NOT NULL,
    summary         TEXT NOT NULL,
    reasoning       TEXT,
    action_taken    VARCHAR(64) NOT NULL,
    action_payload  JSONB NOT NULL DEFAULT '{}',
    confidence      NUMERIC(3,2),
    requires_human  BOOLEAN NOT NULL DEFAULT FALSE,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_incidents_unresolved
    ON incidents(resolved) WHERE resolved = FALSE;
CREATE INDEX IF NOT EXISTS idx_incidents_correlation
    ON incidents(correlation_id);

-- Add a few example past-resolution embeddings to give the RAG
-- step something to retrieve from on day one. The actual embedding
-- vectors are written by a one-time script after first boot
-- (see scripts/seed_resolutions.py).
CREATE TABLE IF NOT EXISTS resolution_history (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_type   VARCHAR(64) NOT NULL,
    situation       TEXT NOT NULL,
    resolution      TEXT NOT NULL,
    outcome         VARCHAR(32),
    embedding       VECTOR(1536),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_resolution_emb_hnsw
    ON resolution_history USING hnsw (embedding vector_cosine_ops);
