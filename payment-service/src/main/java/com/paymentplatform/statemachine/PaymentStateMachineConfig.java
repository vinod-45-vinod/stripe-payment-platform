package com.paymentplatform.statemachine;

import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachineFactory;
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineConfigurationConfigurer;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

import java.util.EnumSet;

/**
 * Configures the Spring State Machine for the payment lifecycle.
 *
 * Valid transitions (from Payment_Platform_Diagrams.txt - Diagram 3):
 *   CREATED     → AUTHORIZED   (AUTHORIZE)
 *   AUTHORIZED  → CAPTURED     (CAPTURE)
 *   CAPTURED    → SUCCEEDED    (SUCCEED — auto-fired after capture)
 *   SUCCEEDED   → REFUNDED     (REFUND)
 *   CREATED     → CANCELLED    (CANCEL)
 *   AUTHORIZED  → CANCELLED    (CANCEL)
 *   CREATED     → FAILED       (FAIL)
 *   AUTHORIZED  → FAILED       (FAIL)
 *
 * Invalid transitions rejected by the state machine:
 *   CREATED     → CAPTURED     (cannot skip authorize)
 *   CAPTURED    → CANCELLED    (cannot cancel after capture)
 *   CREATED     → REFUNDED     (cannot refund before success)
 *   AUTHORIZED  → REFUNDED     (cannot refund before success)
 */
@Configuration
@EnableStateMachineFactory
public class PaymentStateMachineConfig
        extends EnumStateMachineConfigurerAdapter<PaymentState, PaymentEvent> {

    @Override
    public void configure(StateMachineConfigurationConfigurer<PaymentState, PaymentEvent> config)
            throws Exception {
        config.withConfiguration()
                .autoStartup(false);
    }

    @Override
    public void configure(StateMachineStateConfigurer<PaymentState, PaymentEvent> states)
            throws Exception {
        states.withStates()
                .initial(PaymentState.CREATED)
                .states(EnumSet.allOf(PaymentState.class));
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<PaymentState, PaymentEvent> transitions)
            throws Exception {
        transitions
                // Happy path
                .withExternal()
                    .source(PaymentState.CREATED).target(PaymentState.AUTHORIZED)
                    .event(PaymentEvent.AUTHORIZE)
                .and()
                .withExternal()
                    .source(PaymentState.AUTHORIZED).target(PaymentState.CAPTURED)
                    .event(PaymentEvent.CAPTURE)
                .and()
                .withExternal()
                    .source(PaymentState.CAPTURED).target(PaymentState.SUCCEEDED)
                    .event(PaymentEvent.SUCCEED)
                .and()
                .withExternal()
                    .source(PaymentState.SUCCEEDED).target(PaymentState.REFUNDED)
                    .event(PaymentEvent.REFUND)
                .and()
                // Cancellation (only before capture)
                .withExternal()
                    .source(PaymentState.CREATED).target(PaymentState.CANCELLED)
                    .event(PaymentEvent.CANCEL)
                .and()
                .withExternal()
                    .source(PaymentState.AUTHORIZED).target(PaymentState.CANCELLED)
                    .event(PaymentEvent.CANCEL)
                .and()
                // Failure
                .withExternal()
                    .source(PaymentState.CREATED).target(PaymentState.FAILED)
                    .event(PaymentEvent.FAIL)
                .and()
                .withExternal()
                    .source(PaymentState.AUTHORIZED).target(PaymentState.FAILED)
                    .event(PaymentEvent.FAIL);
    }
}
