-- Ledger service schema — Level 4
-- Implements double-entry bookkeeping:
--   accounts  : one row per participant (customer, merchant, platform)
--   transactions : one row per captured payment / refund event
--   ledger_entries : ≥2 rows per transaction, always summing to zero

CREATE TABLE IF NOT EXISTS accounts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_type  VARCHAR(20)  NOT NULL,  -- CUSTOMER | MERCHANT | PLATFORM
    owner_id    VARCHAR(255) NOT NULL,
    balance     NUMERIC(19,4) NOT NULL DEFAULT 0,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    UNIQUE (owner_type, owner_id)
);

-- Seed platform fee account (always exists)
INSERT INTO accounts (owner_type, owner_id) VALUES ('PLATFORM', 'platform-fee-account')
ON CONFLICT (owner_type, owner_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS ledger_transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reference_id    UUID        NOT NULL,  -- payment_id from Kafka event
    reference_type  VARCHAR(32) NOT NULL,  -- PAYMENT_CAPTURED | REFUND_CREATED
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID         NOT NULL REFERENCES ledger_transactions(id),
    account_id      UUID         NOT NULL REFERENCES accounts(id),
    entry_type      VARCHAR(10)  NOT NULL,   -- DEBIT | CREDIT
    amount          NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_transaction ON ledger_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_account     ON ledger_entries(account_id);
CREATE INDEX IF NOT EXISTS idx_ledger_txn_reference       ON ledger_transactions(reference_id);
