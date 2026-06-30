# RESUME_BULLETS.md — Payment Platform

Use these as bullet points on a resume under a "Projects" or "Side Projects" section.
They are written in the results-first format (impact → action → technology).

---

## Stripe-Inspired Payment Platform

> **GitHub:** `github.com/<your-username>/payment-platform` · Java 21 · Spring Boot 4 · Kafka · Redis · PostgreSQL

---

- **Architected and shipped a 4-microservice payment platform** (payment, ledger, notification, fraud) processing the full payment lifecycle — CREATED → AUTHORIZED → CAPTURED → SUCCEEDED → REFUNDED — enforced by a Spring State Machine, with 100% invalid-transition rejection verified by 14 unit tests.

- **Implemented the Transactional Outbox Pattern** to guarantee zero event loss between PostgreSQL and Apache Kafka: payment state changes and their Kafka events are written atomically in a single DB transaction; a `@Scheduled` publisher polls unpublished rows every 5s, delivering at-least-once to 4 Kafka topics across 3 downstream services.

- **Delivered idempotent payment creation with Redis** using a body-hash comparison strategy: duplicate requests with the same `Idempotency-Key` return the cached response without a DB write; a different body on the same key returns HTTP 409 — proven correct by an integration test using Testcontainers (Postgres + Redis + Kafka).

- **Built distributed double-entry bookkeeping** in ledger-service: every captured payment triggers 3 balanced ledger entries (debit customer, credit merchant at 95%, credit platform at 5%) with a zero-sum invariant enforced by unit tests; concurrent write safety via JPA `@Version` optimistic locking returning HTTP 409 on contention.

- **Engineered Redis-based per-customer rate limiting** on payment endpoints using a fixed-window counter (`INCR`/`EXPIRE`); exceeding 10 req/min returns HTTP 429 with a `Retry-After` header; fails open if Redis is unavailable to avoid blocking legitimate traffic.

- **Implemented end-to-end distributed trace correlation** across all 4 services: `OutboxPublisherJob` injects a UUID `X-Trace-Id` into each Kafka `ProducerRecord` header; every consumer extracts it into SLF4J MDC so `[traceId=xxx paymentId=yyy]` appears on every log line for a payment's full journey — zero external infrastructure required.

- **Delivered real webhook delivery** in notification-service using Spring Boot 4's `RestClient`: merchants receive HTTP POST payloads on payment events with a 5-tier exponential retry schedule (immediate → 1m → 5m → 15m → 1h → permanent FAILED); 3 webhook integration tests pass without Docker.

- **Wired DLQ for all 3 Kafka consumers** using Spring Kafka's `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`: after 3 retries at 1s back-off, failed messages land on `payment-dlq`; JSON deserialization errors skip retries and route directly to DLQ.

- **Built an event-driven analytics API** (`GET /analytics` on notification-service) that tracks rolling payment metrics — volume per merchant, global success rate, average authorization-to-capture time — purely from Kafka event consumption, with zero direct DB queries to payment-service.

- **Deployed a fully containerised stack** with multi-stage Dockerfiles (Gradle build → JRE runtime, non-root user, container-aware JVM flags), hardened `docker-compose.yml` with `service_healthy` dependency chains, and a GitHub Actions CI pipeline running parallel module tests with Testcontainers + Docker-in-Docker on every push.
