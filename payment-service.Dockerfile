# =============================================================================
# payment-service — Multi-stage Dockerfile
# Stage 1: Build with Gradle (Gradle wrapper + JDK 21)
# Stage 2: Run on slim JRE image (eclipse-temurin JRE alpine)
# =============================================================================

# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /workspace

# Copy Gradle wrapper and root build files first (cache Gradle downloads)
COPY gradlew gradlew.bat ./
COPY gradle/ gradle/
COPY settings.gradle ./
COPY gradle.properties ./

# Copy all sub-project build files (for dependency resolution cache layer)
COPY payment-service/build.gradle payment-service/build.gradle
COPY ledger-service/build.gradle ledger-service/build.gradle
COPY notification-service/build.gradle notification-service/build.gradle
COPY fraud-service/build.gradle fraud-service/build.gradle
COPY common-lib/build.gradle common-lib/build.gradle
COPY shared-events/build.gradle shared-events/build.gradle

# Resolve dependencies before copying source (better layer caching)
RUN chmod +x gradlew && ./gradlew :payment-service:dependencies --no-daemon -q 2>/dev/null || true

# Copy all source code
COPY common-lib/ common-lib/
COPY shared-events/ shared-events/
COPY payment-service/ payment-service/

# Build the fat JAR (skip tests — tested in CI separately)
RUN ./gradlew :payment-service:bootJar --no-daemon -x test

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

# Security: run as non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy only the built JAR from builder stage
COPY --from=builder /workspace/payment-service/build/libs/*.jar app.jar

# Set ownership
RUN chown appuser:appgroup app.jar
USER appuser

EXPOSE 8080

# JVM tuning for containers: container-aware heap + GC
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1
