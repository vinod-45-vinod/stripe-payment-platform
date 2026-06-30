# Payment Platform

> A Stripe-inspired payment platform built in Java 21 + Spring Boot 4
> — progressing from monolith to microservices across 7 production-grade levels.

## Quick Start (3 commands)

```bash
# 1. Clone
git clone https://github.com/vinod-45-vinod/stripe-payment-platform.git && cd stripe-payment-platform

# 2. Start everything (Postgres, Redis, Kafka + 4 microservices)
docker compose up -d

# 3. Run smoke test — full payment flow + health checks
./smoke-test.sh --skip-compose      # Linux/macOS
# OR on Windows:
.\smoke-test.ps1 -SkipCompose
```

> **Note:** First `docker compose up` takes ~3-4 min (Gradle + Docker image builds).
> Subsequent starts are ~30s thanks to layer caching.
> Services are ready when all health endpoints return `"status":"UP"`.

**Health endpoints:**
```bash
curl http://localhost:8080/actuator/health  # payment-service
curl http://localhost:8081/actuator/health  # ledger-service
curl http://localhost:8082/actuator/health  # notification-service
curl http://localhost:8083/actuator/health  # fraud-service
```

---

## Architecture Overview

The platform is split into **four independent microservices** connected by Kafka events.
All events carry an `X-Trace-Id` header for end-to-end log correlation across services.


```
                   ┌──────────────────────────────────────────────────────┐
REST Client ──────▶│           payment-service (:8080)                    │
                   │  state machine · idempotency · outbox · rate-limiting│
                   │  Swagger UI: http://localhost:8080/swagger-ui.html   │
                   └─────────────┬────────────────────────────────────────┘
                                 │  Kafka (Outbox Pattern)
                                 │  [X-Trace-Id header injected per event]
                    ┌────────────┴────────────────────┐
                    ▼             ▼                   ▼
        ┌──────────────┐  ┌────────────────┐  ┌──────────────┐
        │ledger-service│  │notification-   │  │fraud-service │
        │   (:8081)    │  │service (:8082) │  │   (:8083)    │
        │double-entry  │  │webhooks + retry│  │rule checks + │
        │bookkeeping   │  │+ DLQ + analytics│  │Redis velocity│
        │Swagger :8081 │  │Swagger :8082   │  │Swagger :8083 │
        └──────────────┘  └──────┬─────────┘  └──────────────┘
        ledger_db                │             fraud_db
                      ┌──────────┴──────────┐
                      │                     │
                      ▼                     ▼
             Merchant webhook       GET /analytics
             HTTP POST + retry    (rolling metrics:
             1m→5m→15m→1h→FAIL    volume/merchant,
                                  success rate,
                                  avg auth→capture)

DLQ: All 3 consumers → payment-dlq (after 3 retries + 1s back-off)
```

**Rate Limiting:** POST /payments is limited to **10 req/min per customerId** (Redis fixed window).
Exceeds limit → HTTP 429 with `Retry-After` header.

**Cross-Service Trace IDs:** Every Kafka message carries `X-Trace-Id` header.
All consumer log lines include `[traceId=xxx paymentId=yyy]` via SLF4J MDC.

**Kafka topics:**

| Topic                 | Published by         | Consumed by                              |
|-----------------------|----------------------|------------------------------------------|
| `payment-created`     | payment-service      | notification-service, fraud-service      |
| `payment-authorized`  | payment-service      | fraud-service                            |
| `payment-captured`    | payment-service      | ledger-service, notification-service     |
| `refund-created`      | payment-service      | ledger-service, notification-service     |
| `payment-dlq`         | Spring Kafka DLQ     | (admin inspection only)                  |

---

## Diagrams

| Diagram | Description |
|---------|-------------|
| ![](docs/images/1.%20architecture-overview.png) | System Architecture |
| ![](docs/images/2.%20payment-state-machine.png) | Payment State Machine |
| ![](docs/images/3.%20transactional-outbox-pattern.png) | Transactional Outbox Pattern |
| ![](docs/images/4.%20kafka-consumer-flow.png) | Kafka Consumer + DLQ + Trace ID Flow |
| ![](docs/images/5.%20payment-service-api-overview.png) | payment-service API (Swagger) |
| ![](docs/images/6.%20ledger-service-workflow.png) | ledger-service Workflow |
| ![](docs/images/7.%20notification-service-workflow.png) | notification-service Workflow |
| ![](docs/images/8.%20fraud-service-workflow.png) | fraud-service Workflow |
| ![](docs/images/9.%20redis-rate-limiting-flow.png) | Redis Rate Limiting Flow |
| ![](docs/images/10.%20webhook-retry-schedule.png) | Webhook Retry Schedule |

---

## Payment Lifecycle


```
CREATED → AUTHORIZED → CAPTURED → SUCCEEDED → REFUNDED
  │            │
  └──> CANCELLED <──┘
  │            │
  └──> FAILED  <──┘
```

| State        | Description                                               |
|--------------|-----------------------------------------------------------|
| `CREATED`    | Initial state after payment creation                      |
| `AUTHORIZED` | Payment has been authorized (auth code generated)         |
| `CAPTURED`   | Funds captured — auto-transitions to `SUCCEEDED`          |
| `SUCCEEDED`  | Payment completed successfully                            |
| `REFUNDED`   | Payment has been refunded (only from `SUCCEEDED`)         |
| `CANCELLED`  | Payment cancelled (only from `CREATED` or `AUTHORIZED`)   |
| `FAILED`     | Payment failed during processing                          |

---

## Module Structure (Gradle Multi-Module)

```
payment-platform/
├── shared-events/           <- Kafka event DTOs (shared by all services)
│   └── com.paymentplatform.events.PaymentEvents
│   └── com.paymentplatform.events.OutboxEventType
├── common-lib/              <- Shared utilities (KafkaTopics, TraceContext)
│   └── com.paymentplatform.common.KafkaTopics
│   └── com.paymentplatform.common.TraceContext
├── payment-service/         <- Core payment processing (port 8080)
├── ledger-service/          <- Double-entry bookkeeping (port 8081)
├── notification-service/    <- Webhooks + retry + analytics (port 8082)
├── fraud-service/           <- Rule-based fraud checks (port 8083)
├── docs/
│   └── images/              <- Architecture and flow diagrams (PNG)
├── docker/
│   └── init-dbs.sql         <- Postgres multi-DB init (payment, ledger, fraud, notification)
└── docker-compose.yml       <- Full stack (all services + infra)
```

---

## Quick Start — Full Stack (Docker)

```bash
# Start everything: Postgres, Redis, Kafka + all 4 microservices
docker compose up -d

# Check payment-service health
curl http://localhost:8080/actuator/health
```

## Quick Start — Local Development (infrastructure only)

```bash
# Start only infrastructure (no services)
docker compose up -d postgres redis zookeeper kafka

# Run payment-service (connects to local infra)
./gradlew :payment-service:bootRun

# Run ledger-service (separate terminal)
./gradlew :ledger-service:bootRun

# Run notification-service (dev profile = H2, mock webhook enabled)
./gradlew :notification-service:bootRun --args="--spring.profiles.active=dev"

# Run fraud-service
./gradlew :fraud-service:bootRun
```

---

## API Reference — payment-service (:8080)

Swagger UI: http://localhost:8080/swagger-ui.html

### Create Payment
```bash
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"amount": 100.00, "currency": "USD", "customerId": "cust_001", "merchantId": "merch_001"}'
```

### Payment Lifecycle Operations
```bash
# Replace {id} with the UUID returned from the create call

# Authorize
curl -s -X POST http://localhost:8080/payments/{id}/authorize

# Capture (auto-transitions to SUCCEEDED)
curl -s -X POST http://localhost:8080/payments/{id}/capture

# Refund (only from SUCCEEDED)
curl -s -X POST http://localhost:8080/payments/{id}/refund

# Cancel (only from CREATED or AUTHORIZED)
curl -s -X POST http://localhost:8080/payments/{id}/cancel

# Get payment (cached in Redis for 10 minutes)
curl -s http://localhost:8080/payments/{id}

# Payment history
curl -s http://localhost:8080/payments/history
```

### Idempotency
```bash
# POST with idempotency key — safe to retry on network timeout
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: unique-request-id-abc123" \
  -d '{"amount": 100.00, "currency": "USD", "customerId": "cust_001", "merchantId": "merch_001"}'
```

---

## API Reference — ledger-service (:8081)

Swagger UI: http://localhost:8081/swagger-ui.html

```bash
# Check balance for a customer account
curl http://localhost:8081/accounts/CUSTOMER/cust_001/balance

# Check balance for a merchant account
curl http://localhost:8081/accounts/MERCHANT/merch_001/balance

# Check platform fee account balance
curl http://localhost:8081/accounts/PLATFORM/platform-fee-account/balance
```

**Double-entry model:** For every captured payment of amount A:
- **DEBIT** customer account: -A
- **CREDIT** merchant account: +(A x 0.95)  [95% of payment]
- **CREDIT** platform account: +(A x 0.05)  [5% fee]
- Net sum = 0 (zero-sum invariant enforced and unit-tested)

---

## API Reference — notification-service (:8082)

Swagger UI: http://localhost:8082/swagger-ui.html

### Webhook Delivery

When a `payment.captured` or `refund.created` event is consumed, the notification-service:
1. Looks up the merchant's registered webhook URL from the `merchants` table
2. Persists a `webhook_events` row with `status=PENDING`
3. Immediately attempts HTTP POST delivery to the merchant's endpoint
4. On failure, schedules retries using the exponential backoff schedule below

**Retry Schedule:**


| Attempt | Delay After Previous Failure |
|---------|------------------------------|
| 1       | Immediate                    |
| 2       | +1 minute                    |
| 3       | +5 minutes                   |
| 4       | +15 minutes                  |
| 5       | +60 minutes → permanent FAILED |

**Webhook Payload Shape** (HTTP POST body, JSON):
```json
{
  "eventType": "payment.captured",
  "paymentId": "550e8400-e29b-41d4-a716-446655440000",
  "amount": "100.00",
  "currency": "USD",
  "merchantId": "merch_001"
}
```

**Merchant Response Contract:**
- Return HTTP `2xx` → delivery marked **DELIVERED**
- Return HTTP `4xx`/`5xx` or timeout → delivery marked **FAILED**, retry scheduled

### Admin / Inspection Endpoints

```bash
# Count webhooks by status
curl http://localhost:8082/admin/webhooks/stats

# Inspect all webhook events for a specific payment
curl http://localhost:8082/admin/webhooks/payment/{paymentId}
```

### Mock Merchant Endpoint (dev/test profile only)

Used for end-to-end retry testing — not active in production:

```bash
# Configure mock to fail next N requests then succeed
curl -X POST "http://localhost:8082/mock/webhook/configure?failCount=2"

# Receive a webhook (automatically called by notification-service)
curl -X POST http://localhost:8082/mock/webhook -H "Content-Type: application/json" -d '{...}'

# Check how many calls were received / failed / delivered
curl http://localhost:8082/mock/webhook/stats
```

### Analytics

```bash
# Rolling payment metrics — sourced purely from Kafka event consumption
curl http://localhost:8082/analytics
```

### DLQ (Dead Letter Queue)

All three consumer services (ledger, notification, fraud) use Spring Kafka's
`DefaultErrorHandler` with `DeadLetterPublishingRecoverer`:

- **3 retries** with 1 second fixed back-off on processing failures
- After exhausting retries → message forwarded to `payment-dlq` topic (partition 0)
- JSON deserialization errors → skip retries, go straight to DLQ
- Inspect DLQ contents:

```bash
docker exec -it payment-platform-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-dlq \
  --from-beginning
```

---

## API Reference — fraud-service (:8083)

Swagger UI: http://localhost:8083/swagger-ui.html

---

## Running Tests

```bash
# All tests (all modules)
./gradlew test

# payment-service only (Testcontainers — Docker required)
./gradlew :payment-service:test

# ledger-service zero-sum invariant tests (no Docker required)
./gradlew :ledger-service:test

# notification-service webhook delivery tests (no Docker required — uses H2 + mock endpoint)
./gradlew :notification-service:test

# State machine unit tests only (no Docker required)
./gradlew :payment-service:test --tests "com.paymentplatform.statemachine.*"
```

**Test coverage:**
- 14 state machine unit tests — all valid/invalid transitions
- Idempotency integration test — duplicate creation, body-hash conflict
- Optimistic locking integration test — concurrent capture race condition
- **4 ledger zero-sum invariant tests:**
  - `captureEntries_alwaysSumToZero`, `platformFee_isExactlyFivePercent`
  - `assertZeroSum_throwsOnImbalance`, `recordCapture_isIdempotent`
- **3 webhook delivery integration tests** (no Docker required):
  - `webhook_deliveredOnFirstAttempt` → status=DELIVERED
  - `webhook_failsOnceThenSucceeds` → retries once, succeeds
  - `webhook_exhaustsAllRetries` → permanent FAILED + "MAX RETRIES EXCEEDED" in lastError

---

## Kafka Topics

Verify events flowing:
```bash
# payment-created events
docker exec -it payment-platform-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment-created --from-beginning

# payment-captured events (triggers ledger bookkeeping + webhook delivery)
docker exec -it payment-platform-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment-captured --from-beginning

# DLQ — inspect failed consumer messages
docker exec -it payment-platform-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment-dlq --from-beginning
```

---

## Key Design Decisions

### Outbox Pattern

Events are written to `outbox_events` table in the **same DB transaction** as the payment state change.
A `@Scheduled` job publishes them to Kafka every 5s. This guarantees:
- No event lost if Kafka is down (retried on next run)
- No phantom event if the DB transaction rolls back
- At-least-once delivery (consumers must be idempotent)

### Event-Driven Decoupling

`ledger-service`, `notification-service`, and `fraud-service` are **pure consumers** — they never
call `payment-service` directly. This means:
- A notification failure does NOT affect payment processing
- Ledger updates happen asynchronously after capture
- Fraud results are logged but not yet wired into payment blocking (documented design decision)

### Redis Rate Limiting

POST /payments is protected by a **fixed-window per-customerId** rate limiter implemented with
Redis `INCR` + `EXPIRE`. The window and limit are configurable (`rate-limit.max-requests=10`,
`rate-limit.window-seconds=60`). Exceeds limit → HTTP 429 + `Retry-After` header. Fails open
if Redis is unavailable (never blocks legitimate traffic due to infra issues).

### Distributed Trace IDs

Each Kafka message produced by `OutboxPublisherJob` carries an `X-Trace-Id` header (UUID).
Consumer services extract this header and place it in SLF4J MDC so every log line for a
particular payment flow — across payment-service, ledger-service, notification-service, and
fraud-service — carries the same `[traceId=xxx paymentId=yyy]` context. No external tracing
infrastructure required.

### Webhook Delivery
Webhook delivery uses Spring Boot 4's `RestClient` (not deprecated `RestTemplate`).
Each delivery runs in its own `@Transactional` scope — the HTTP call does not hold a DB lock.
The retry scheduler polls every 30 seconds (configurable) for due retries.

### Dead Letter Queue
Spring Kafka's `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` is used in all
three consumer services. After 3 retries (1s back-off), the raw Kafka record (key + value + headers)
is forwarded to `payment-dlq`. This ensures no message is silently dropped.

### Fraud Service Design Note
The fraud-service logs BLOCK decisions but does not currently prevent payment processing.
This is an explicit design decision: wiring fraud decisions back into payment-service
synchronously would create coupling. A future `fraud-decision` Kafka event could be consumed
by payment-service to update status asynchronously.

---

## Tech Stack

| Technology             | Purpose                                                     |
|------------------------|-------------------------------------------------------------|
| Java 21 (LTS)          | Language (all modules)                                      |
| Spring Boot 4.0.7      | Application framework                                       |
| Spring State Machine   | Payment lifecycle enforcement                               |
| Spring Data JPA        | Data persistence (payment, ledger, fraud, notification)     |
| PostgreSQL             | Primary database (4 separate DBs)                           |
| Flyway                 | Schema migrations (per service)                             |
| Redis (Lettuce)        | Idempotency store + cache + rate limiting (payment & fraud) |
| Apache Kafka           | Event streaming between services                            |
| Spring Kafka DLQ       | Dead Letter Queue (payment-dlq topic)                       |
| RestClient (Spring 6)  | Webhook HTTP delivery                                       |
| SpringDoc / Swagger UI | OpenAPI 3.1 docs on all 4 services                          |
| SLF4J MDC + TraceId   | Cross-service distributed trace context                     |
| Lombok 1.18.46         | Boilerplate reduction                                       |
| Gradle 9.6 multi-module| Build tool                                                  |
| JUnit 5 + AssertJ      | Unit & integration testing                                  |
| Testcontainers         | Integration tests (Redis, Kafka, Postgres)                  |
| H2 (test scope)        | Fast in-memory tests (notification-service)                 |
| Docker Compose         | Full-stack local environment                                |

---

## Level Progress

| Level | Description                                                  | Status   |
|-------|--------------------------------------------------------------|----------|
| 1     | REST + State Machine + PostgreSQL                            | Complete |
| 2     | Idempotency + Optimistic Locking + Redis                     | Complete |
| 3     | Kafka + Outbox Pattern                                       | Complete |
| 4     | Microservices Split (4 services)                             | Complete |
| 5     | Webhook Delivery + Retry + DLQ                               | Complete |
| 6     | Full Dockerization + Testcontainers E2E + CI                 | Complete |
| 7     | Rate Limiting + Analytics + OpenAPI + Trace IDs + Polish     | Complete |
