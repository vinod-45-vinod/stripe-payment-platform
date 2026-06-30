package com.paymentplatform.notification.analytics;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * In-memory rolling analytics store for payment events.
 *
 * Thread-safe via ConcurrentHashMap and atomic types.
 * Data is not persisted across restarts — this is intentional for demo
 * simplicity.
 * In production, metrics would be written to a time-series DB or emitted as
 * Prometheus gauges.
 *
 * Metrics tracked:
 * - Total payments created, authorized, captured, refunded (global)
 * - Volume (count + total amount) per merchant
 * - Authorization-to-capture elapsed time (avg in seconds)
 * - Global success rate = succeeded / (created)
 */
@Component
public class PaymentAnalyticsStore {

    // ── Global counters ───────────────────────────────────────────────────────
    private final AtomicLong totalCreated = new AtomicLong(0);
    private final AtomicLong totalAuthorized = new AtomicLong(0);
    private final AtomicLong totalCaptured = new AtomicLong(0);
    private final AtomicLong totalRefunded = new AtomicLong(0);

    // ── Per-merchant payment volume ───────────────────────────────────────────
    // merchantId → {count, totalAmount}
    private final ConcurrentHashMap<String, MerchantStats> merchantStats = new ConcurrentHashMap<>();

    // ── Authorization time tracking ───────────────────────────────────────────
    // paymentId → epoch-ms when payment was created
    private final ConcurrentHashMap<String, Long> createdAtMs = new ConcurrentHashMap<>();
    private final AtomicLong authTimeTotalMs = new AtomicLong(0);
    private final AtomicLong authTimeCount = new AtomicLong(0);

    // ── Event recording ───────────────────────────────────────────────────────

    public void recordCreated(String paymentId, String merchantId, BigDecimal amount) {
        totalCreated.incrementAndGet();
        createdAtMs.put(paymentId, System.currentTimeMillis());
        merchantStats.computeIfAbsent(merchantId, id -> new MerchantStats())
                .record(amount);
    }

    public void recordAuthorized(String paymentId) {
        totalAuthorized.incrementAndGet();
        Long createdMs = createdAtMs.get(paymentId);
        if (createdMs != null) {
            long elapsed = System.currentTimeMillis() - createdMs;
            authTimeTotalMs.addAndGet(elapsed);
            authTimeCount.incrementAndGet();
        }
    }

    public void recordCaptured() {
        totalCaptured.incrementAndGet();
    }

    public void recordRefunded() {
        totalRefunded.incrementAndGet();
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public AnalyticsSnapshot snapshot() {
        long created = totalCreated.get();
        long authorized = totalAuthorized.get();
        long captured = totalCaptured.get();
        long refunded = totalRefunded.get();

        double successRate = created == 0 ? 0.0
                : Math.round((double) captured / created * 10000.0) / 100.0;

        long authCount = authTimeCount.get();
        double avgAuthToCaptureSec = authCount == 0 ? 0.0
                : Math.round((double) authTimeTotalMs.get() / authCount / 10.0) / 100.0;

        Map<String, MerchantSummary> merchantSummaries = merchantStats.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new MerchantSummary(
                                e.getValue().count.get(),
                                e.getValue().totalAmount.get().setScale(2, RoundingMode.HALF_UP))));

        return new AnalyticsSnapshot(
                created, authorized, captured, refunded,
                successRate, avgAuthToCaptureSec, merchantSummaries);
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    static class MerchantStats {
        final AtomicLong count = new AtomicLong(0);
        final AtomicReference<BigDecimal> totalAmount = new AtomicReference<>(BigDecimal.ZERO);

        void record(BigDecimal amount) {
            count.incrementAndGet();
            totalAmount.updateAndGet(prev -> prev.add(amount == null ? BigDecimal.ZERO : amount));
        }
    }

    public record MerchantSummary(long paymentCount, BigDecimal totalAmount) {
    }

    public record AnalyticsSnapshot(
            long totalCreated,
            long totalAuthorized,
            long totalCaptured,
            long totalRefunded,
            double successRatePct,
            double avgAuthToCaptureSec,
            Map<String, MerchantSummary> volumeByMerchant) {
    }
}
