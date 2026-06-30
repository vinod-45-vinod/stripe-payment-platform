-- Level 3: Outbox Pattern
-- Outbox events are written in the SAME transaction as payment state changes.
-- A separate scheduler job reads unpublished rows and publishes them to Kafka,
-- then marks them published=true. This guarantees no event is ever lost even if
-- Kafka is temporarily unavailable at the moment of the state change.

CREATE TABLE outbox_events (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id   UUID        NOT NULL REFERENCES payments(id),
    event_type   VARCHAR(64) NOT NULL,                        -- e.g. PAYMENT_CREATED
    payload      TEXT        NOT NULL,                        -- JSON
    published    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_unpublished ON outbox_events (published, created_at)
    WHERE published = FALSE;
