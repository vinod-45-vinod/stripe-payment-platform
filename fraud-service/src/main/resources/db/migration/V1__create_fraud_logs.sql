-- Fraud service schema — Level 4
-- Logs fraud check results for every payment event processed.

CREATE TABLE IF NOT EXISTS fraud_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id      UUID        NOT NULL,
    customer_id     VARCHAR(255),
    amount          NUMERIC(19,4),
    rule_triggered  VARCHAR(64) NOT NULL,    -- HIGH_VALUE | VELOCITY | MULTIPLE_RULES | NONE
    result          VARCHAR(20) NOT NULL,    -- APPROVE | REVIEW_REQUIRED | BLOCK
    details         TEXT,                    -- human-readable explanation
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_logs_payment    ON fraud_logs(payment_id);
CREATE INDEX IF NOT EXISTS idx_fraud_logs_customer   ON fraud_logs(customer_id);
CREATE INDEX IF NOT EXISTS idx_fraud_logs_result     ON fraud_logs(result);
