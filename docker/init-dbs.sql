-- Docker init script: creates all databases needed by each microservice.
-- This runs automatically on first Postgres container startup.

-- payment-service DB already created as POSTGRES_DB in docker-compose

CREATE DATABASE ledger_db;
CREATE DATABASE fraud_db;
CREATE DATABASE notification_db;

-- Grant permissions to postgres user (already superuser, but explicit is cleaner)
GRANT ALL PRIVILEGES ON DATABASE payment_platform TO postgres;
GRANT ALL PRIVILEGES ON DATABASE ledger_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE fraud_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE notification_db TO postgres;

