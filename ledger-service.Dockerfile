# =============================================================================
# ledger-service — Multi-stage Dockerfile
# =============================================================================

FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /workspace

COPY gradlew gradlew.bat ./
COPY gradle/ gradle/
COPY settings.gradle ./
COPY gradle.properties ./

COPY payment-service/build.gradle payment-service/build.gradle
COPY ledger-service/build.gradle ledger-service/build.gradle
COPY notification-service/build.gradle notification-service/build.gradle
COPY fraud-service/build.gradle fraud-service/build.gradle
COPY common-lib/build.gradle common-lib/build.gradle
COPY shared-events/build.gradle shared-events/build.gradle

RUN chmod +x gradlew && ./gradlew :ledger-service:dependencies --no-daemon -q 2>/dev/null || true

COPY common-lib/ common-lib/
COPY shared-events/ shared-events/
COPY ledger-service/ ledger-service/

RUN ./gradlew :ledger-service:bootJar --no-daemon -x test

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /workspace/ledger-service/build/libs/*.jar app.jar
RUN chown appuser:appgroup app.jar
USER appuser

EXPOSE 8081

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
  CMD wget -qO- http://localhost:8081/actuator/health || exit 1
