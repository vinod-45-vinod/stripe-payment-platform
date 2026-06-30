-- Level 5: Webhook delivery tables for notification-service
-- merchants: stores registered webhook URLs per merchant
-- webhook_events: tracks every webhook attempt, status, and retry schedule

-- ============================================================
-- merchants table
-- ============================================================
CREATE TABLE merchants (
    id          VARCHAR(255) PRIMARY KEY,     -- merchant_id (e.g. "merch_001")
    name        VARCHAR(255) NOT NULL,
    webhook_url VARCHAR(1024),               -- registered HTTPS endpoint for webhooks
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed test merchants (used by dev/test environments)
INSERT INTO merchants (id, name, webhook_url, active) VALUES
    ('merch_001', 'Test Merchant One',   'http://localhost:8082/mock/webhook', true),
    ('merch_002', 'Test Merchant Two',   'http://localhost:8082/mock/webhook', true),
    ('merch_no_webhook', 'No-Webhook Merchant', NULL, true);

-- ============================================================
-- webhook_events table
-- ============================================================
CREATE TABLE webhook_events (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id    UUID        NOT NULL,
    merchant_id   VARCHAR(255) NOT NULL,
    merchant_url  VARCHAR(1024) NOT NULL,
    event_type    VARCHAR(100) NOT NULL,   -- e.g. 'payment.captured'
    payload       TEXT        NOT NULL,    -- JSON payload sent to merchant
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING / DELIVERED / FAILED
    retry_count   INT         NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP,              -- NULL means ready to deliver now
    last_error    TEXT,                   -- last HTTP error message for debugging
    created_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Index: poll pending/failed webhooks due for retry efficiently
CREATE INDEX idx_webhook_events_status_retry
    ON webhook_events (status, next_retry_at)
    WHERE status IN ('PENDING', 'FAILED');

-- Index: look up webhooks by payment_id
CREATE INDEX idx_webhook_events_payment_id ON webhook_events (payment_id);
