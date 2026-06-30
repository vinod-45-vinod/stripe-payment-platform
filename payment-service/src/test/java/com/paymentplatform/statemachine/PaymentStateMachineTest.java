package com.paymentplatform.statemachine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.StateMachineEventResult;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for payment state machine transition rules.
 * Tests both valid transitions (happy path + cancellation) and
 * invalid transitions that must be rejected.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = PaymentStateMachineConfig.class)
class PaymentStateMachineTest {

    @Autowired
    private StateMachineFactory<PaymentState, PaymentEvent> stateMachineFactory;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private StateMachine<PaymentState, PaymentEvent> machineInState(PaymentState state) {
        StateMachine<PaymentState, PaymentEvent> sm =
                stateMachineFactory.getStateMachine(UUID.randomUUID().toString());

        sm.stopReactively().block();
        sm.getStateMachineAccessor()
                .doWithAllRegions(accessor ->
                        accessor.resetStateMachineReactively(
                                new DefaultStateMachineContext<>(state, null, null, null)
                        ).block()
                );
        sm.startReactively().block();
        return sm;
    }

    private boolean sendEvent(StateMachine<PaymentState, PaymentEvent> sm, PaymentEvent event) {
        StateMachineEventResult<PaymentState, PaymentEvent> result =
                sm.sendEvent(Mono.just(MessageBuilder.withPayload(event).build()))
                  .blockLast();
        return result != null
                && result.getResultType() == StateMachineEventResult.ResultType.ACCEPTED;
    }

    // -------------------------------------------------------------------------
    // Valid transitions
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Valid transitions")
    class ValidTransitions {

        @Test
        @DisplayName("CREATED → AUTHORIZED via AUTHORIZE")
        void createdToAuthorized() {
            var sm = machineInState(PaymentState.CREATED);
            assertTrue(sendEvent(sm, PaymentEvent.AUTHORIZE));
            assertEquals(PaymentState.AUTHORIZED, sm.getState().getId());
        }

        @Test
        @DisplayName("AUTHORIZED → CAPTURED via CAPTURE")
        void authorizedToCaptured() {
            var sm = machineInState(PaymentState.AUTHORIZED);
            assertTrue(sendEvent(sm, PaymentEvent.CAPTURE));
            assertEquals(PaymentState.CAPTURED, sm.getState().getId());
        }

        @Test
        @DisplayName("CAPTURED → SUCCEEDED via SUCCEED")
        void capturedToSucceeded() {
            var sm = machineInState(PaymentState.CAPTURED);
            assertTrue(sendEvent(sm, PaymentEvent.SUCCEED));
            assertEquals(PaymentState.SUCCEEDED, sm.getState().getId());
        }

        @Test
        @DisplayName("SUCCEEDED → REFUNDED via REFUND")
        void succeededToRefunded() {
            var sm = machineInState(PaymentState.SUCCEEDED);
            assertTrue(sendEvent(sm, PaymentEvent.REFUND));
            assertEquals(PaymentState.REFUNDED, sm.getState().getId());
        }

        @Test
        @DisplayName("CREATED → CANCELLED via CANCEL")
        void createdToCancelled() {
            var sm = machineInState(PaymentState.CREATED);
            assertTrue(sendEvent(sm, PaymentEvent.CANCEL));
            assertEquals(PaymentState.CANCELLED, sm.getState().getId());
        }

        @Test
        @DisplayName("AUTHORIZED → CANCELLED via CANCEL")
        void authorizedToCancelled() {
            var sm = machineInState(PaymentState.AUTHORIZED);
            assertTrue(sendEvent(sm, PaymentEvent.CANCEL));
            assertEquals(PaymentState.CANCELLED, sm.getState().getId());
        }

        @Test
        @DisplayName("Full happy path: CREATED → AUTHORIZED → CAPTURED → SUCCEEDED → REFUNDED")
        void fullHappyPath() {
            var sm = machineInState(PaymentState.CREATED);

            assertTrue(sendEvent(sm, PaymentEvent.AUTHORIZE));
            assertEquals(PaymentState.AUTHORIZED, sm.getState().getId());

            assertTrue(sendEvent(sm, PaymentEvent.CAPTURE));
            assertEquals(PaymentState.CAPTURED, sm.getState().getId());

            assertTrue(sendEvent(sm, PaymentEvent.SUCCEED));
            assertEquals(PaymentState.SUCCEEDED, sm.getState().getId());

            assertTrue(sendEvent(sm, PaymentEvent.REFUND));
            assertEquals(PaymentState.REFUNDED, sm.getState().getId());
        }
    }

    // -------------------------------------------------------------------------
    // Invalid transitions
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Invalid transitions — must be rejected")
    class InvalidTransitions {

        @Test
        @DisplayName("Cannot capture before authorization (CREATED → CAPTURED)")
        void cannotCaptureBeforeAuthorize() {
            var sm = machineInState(PaymentState.CREATED);
            assertFalse(sendEvent(sm, PaymentEvent.CAPTURE));
            assertEquals(PaymentState.CREATED, sm.getState().getId());
        }

        @Test
        @DisplayName("Cannot cancel after capture (CAPTURED → CANCELLED)")
        void cannotCancelAfterCapture() {
            var sm = machineInState(PaymentState.CAPTURED);
            assertFalse(sendEvent(sm, PaymentEvent.CANCEL));
            assertEquals(PaymentState.CAPTURED, sm.getState().getId());
        }

        @Test
        @DisplayName("Cannot refund before success (CREATED → REFUNDED)")
        void cannotRefundFromCreated() {
            var sm = machineInState(PaymentState.CREATED);
            assertFalse(sendEvent(sm, PaymentEvent.REFUND));
            assertEquals(PaymentState.CREATED, sm.getState().getId());
        }

        @Test
        @DisplayName("Cannot refund before success (AUTHORIZED → REFUNDED)")
        void cannotRefundFromAuthorized() {
            var sm = machineInState(PaymentState.AUTHORIZED);
            assertFalse(sendEvent(sm, PaymentEvent.REFUND));
            assertEquals(PaymentState.AUTHORIZED, sm.getState().getId());
        }

        @Test
        @DisplayName("Cannot cancel after success (SUCCEEDED → CANCELLED)")
        void cannotCancelAfterSuccess() {
            var sm = machineInState(PaymentState.SUCCEEDED);
            assertFalse(sendEvent(sm, PaymentEvent.CANCEL));
            assertEquals(PaymentState.SUCCEEDED, sm.getState().getId());
        }

        @Test
        @DisplayName("Cannot authorize twice (AUTHORIZED → AUTHORIZED)")
        void cannotDoubleAuthorize() {
            var sm = machineInState(PaymentState.AUTHORIZED);
            assertFalse(sendEvent(sm, PaymentEvent.AUTHORIZE));
            assertEquals(PaymentState.AUTHORIZED, sm.getState().getId());
        }
    }
}
