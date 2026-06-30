CREATE TABLE payments (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    amount             DECIMAL(19, 4) NOT NULL,
    currency           VARCHAR(3)   NOT NULL,
    status             VARCHAR(20)  NOT NULL DEFAULT 'CREATED',
    customer_id        VARCHAR(255) NOT NULL,
    merchant_id        VARCHAR(255) NOT NULL,
    card_token         VARCHAR(255),
    authorization_code VARCHAR(255),
    created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version            BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX idx_payments_customer_id ON payments(customer_id);
CREATE INDEX idx_payments_merchant_id ON payments(merchant_id);
CREATE INDEX idx_payments_status ON payments(status);
