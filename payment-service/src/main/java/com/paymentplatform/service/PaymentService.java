package com.paymentplatform.service;

import com.paymentplatform.dto.CreatePaymentRequest;
import com.paymentplatform.dto.PaymentResponse;
import com.paymentplatform.entity.Payment;
import com.paymentplatform.exception.InvalidStateTransitionException;
import com.paymentplatform.exception.PaymentNotFoundException;
import com.paymentplatform.repository.PaymentRepository;
import com.paymentplatform.statemachine.PaymentEvent;
import com.paymentplatform.statemachine.PaymentState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.StateMachineEventResult;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Core payment business logic.
 *
 * Idempotency: createPayment() checks Redis before writing; stores response after.
 * Optimistic locking: @Version on Payment entity ensures concurrent writes race safely.
 * Redis caching: getPayment() is cached via @Cacheable; all writes evict the cache.
 *
 * Uses Spring State Machine as a validation layer to enforce the payment lifecycle —
 * the DB Payment.status field is the source of truth, not the state machine instance
 * (which is transient per-request).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    static final String PAYMENTS_CACHE = "payments";

    private final PaymentRepository paymentRepository;
    private final StateMachineFactory<PaymentState, PaymentEvent> stateMachineFactory;
    private final IdempotencyService idempotencyService;
    private final OutboxService outboxService;

    /**
     * Create a new payment.
     *
     * If an Idempotency-Key header is supplied:
     *  - Cache hit (same body): returns stored response, no DB write.
     *  - Cache hit (different body): throws IdempotencyConflictException → 409.
     *  - Cache miss: creates payment, stores response in Redis.
     *
     * @param idempotencyKey  nullable — client-supplied header value
     * @param requestBodyJson serialised request body, used for body-hash comparison
     * @param request         validated request body
     */
    @Transactional
    public PaymentResponse createPayment(
            String idempotencyKey,
            String requestBodyJson,
            CreatePaymentRequest request) {

        // Idempotency check — only when header is present
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            PaymentResponse cached = idempotencyService.checkExisting(idempotencyKey, requestBodyJson);
            if (cached != null) {
                return cached; // Duplicate request — return original response without DB write
            }
        }

        Payment payment = Payment.builder()
                .amount(request.getAmount())
                .currency(request.getCurrency().toUpperCase())
                .status(PaymentState.CREATED)
                .customerId(request.getCustomerId())
                .merchantId(request.getMerchantId())
                .build();

        payment = paymentRepository.save(payment);
        log.info("Payment created: id={}, amount={} {}, customer={}, merchant={}",
                payment.getId(), payment.getAmount(), payment.getCurrency(),
                payment.getCustomerId(), payment.getMerchantId());

        // Outbox: record event in same transaction — publisher job will send to Kafka
        outboxService.recordPaymentCreated(payment);

        PaymentResponse response = mapToResponse(payment);

        // Store in Redis for future duplicate detection
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            idempotencyService.store(idempotencyKey, requestBodyJson, response);
        }

        return response;
    }

    /**
     * CREATED → AUTHORIZED.
     * Evicts payment from cache so the next GET fetches the updated state.
     */
    @Transactional
    @CacheEvict(value = PAYMENTS_CACHE, key = "#id")
    public PaymentResponse authorizePayment(UUID id) {
        Payment payment = findPaymentOrThrow(id);

        validateAndTransition(payment.getStatus(), PaymentEvent.AUTHORIZE);

        payment.setStatus(PaymentState.AUTHORIZED);
        payment.setAuthorizationCode(generateAuthorizationCode());
        payment = paymentRepository.save(payment);

        outboxService.recordPaymentAuthorized(payment);
        log.info("Payment authorized: id={}, authCode={}", payment.getId(), payment.getAuthorizationCode());
        return mapToResponse(payment);
    }

    /**
     * Capture transitions: AUTHORIZED → CAPTURED → SUCCEEDED (auto).
     * Per the state diagram, CAPTURED → SUCCEEDED is an automatic transition
     * that fires immediately after capture, so the caller sees SUCCEEDED.
     *
     * Optimistic locking is active here via @Version — if two concurrent capture
     * requests race on the same payment, one will win and the other gets
     * ObjectOptimisticLockingFailureException → 409 (handled in GlobalExceptionHandler).
     */
    @Transactional
    @CacheEvict(value = PAYMENTS_CACHE, key = "#id")
    public PaymentResponse capturePayment(UUID id) {
        Payment payment = findPaymentOrThrow(id);

        // First: AUTHORIZED → CAPTURED
        validateAndTransition(payment.getStatus(), PaymentEvent.CAPTURE);

        // Auto-transition: CAPTURED → SUCCEEDED
        validateAndTransition(PaymentState.CAPTURED, PaymentEvent.SUCCEED);

        payment.setStatus(PaymentState.SUCCEEDED);
        payment = paymentRepository.save(payment); // @Version check happens here

        outboxService.recordPaymentCaptured(payment);
        log.info("Payment captured and succeeded: id={}", payment.getId());
        return mapToResponse(payment);
    }

    /**
     * CREATED or AUTHORIZED → CANCELLED.
     */
    @Transactional
    @CacheEvict(value = PAYMENTS_CACHE, key = "#id")
    public PaymentResponse cancelPayment(UUID id) {
        Payment payment = findPaymentOrThrow(id);

        validateAndTransition(payment.getStatus(), PaymentEvent.CANCEL);

        payment.setStatus(PaymentState.CANCELLED);
        payment = paymentRepository.save(payment);

        log.info("Payment cancelled: id={}", payment.getId());
        return mapToResponse(payment);
    }

    /**
     * SUCCEEDED → REFUNDED.
     */
    @Transactional
    @CacheEvict(value = PAYMENTS_CACHE, key = "#id")
    public PaymentResponse refundPayment(UUID id) {
        Payment payment = findPaymentOrThrow(id);

        validateAndTransition(payment.getStatus(), PaymentEvent.REFUND);

        payment.setStatus(PaymentState.REFUNDED);
        payment = paymentRepository.save(payment);

        outboxService.recordRefundCreated(payment);
        log.info("Payment refunded: id={}", payment.getId());
        return mapToResponse(payment);
    }

    /**
     * GET single payment — cached in Redis under key "payments::{id}".
     * Cache is populated on first read and evicted on any write (see above methods).
     */
    @Transactional(readOnly = true)
    @Cacheable(value = PAYMENTS_CACHE, key = "#id")
    public PaymentResponse getPayment(UUID id) {
        log.debug("Cache miss for payment id={}; loading from DB", id);
        return mapToResponse(findPaymentOrThrow(id));
    }

    @Transactional(readOnly = true)
    public List<PaymentResponse> getPaymentHistory() {
        return paymentRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private Payment findPaymentOrThrow(UUID id) {
        return paymentRepository.findById(id)
                .orElseThrow(() -> new PaymentNotFoundException(id));
    }

    /**
     * Builds a transient state machine seeded with {@code currentState},
     * sends {@code event}, and throws if the transition is rejected.
     */
    private void validateAndTransition(PaymentState currentState, PaymentEvent event) {
        StateMachine<PaymentState, PaymentEvent> sm = buildStateMachine(currentState);
        try {
            StateMachineEventResult<PaymentState, PaymentEvent> result =
                    sm.sendEvent(Mono.just(MessageBuilder.withPayload(event).build()))
                      .blockLast();

            if (result == null
                    || result.getResultType() != StateMachineEventResult.ResultType.ACCEPTED) {
                throw new InvalidStateTransitionException(currentState, event);
            }
        } finally {
            sm.stopReactively().block();
        }
    }

    /**
     * Creates a fresh state machine instance, resets it to the given state,
     * and starts it — ready to receive one event.
     */
    private StateMachine<PaymentState, PaymentEvent> buildStateMachine(PaymentState currentState) {
        StateMachine<PaymentState, PaymentEvent> sm =
                stateMachineFactory.getStateMachine(UUID.randomUUID().toString());

        sm.stopReactively().block();

        sm.getStateMachineAccessor()
                .doWithAllRegions(accessor ->
                        accessor.resetStateMachineReactively(
                                new DefaultStateMachineContext<>(currentState, null, null, null)
                        ).block()
                );

        sm.startReactively().block();
        return sm;
    }

    private String generateAuthorizationCode() {
        return "AUTH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private PaymentResponse mapToResponse(Payment payment) {
        return PaymentResponse.builder()
                .id(payment.getId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .status(payment.getStatus())
                .customerId(payment.getCustomerId())
                .merchantId(payment.getMerchantId())
                .cardToken(payment.getCardToken())
                .authorizationCode(payment.getAuthorizationCode())
                .createdAt(payment.getCreatedAt())
                .updatedAt(payment.getUpdatedAt())
                .version(payment.getVersion())
                .build();
    }
}
