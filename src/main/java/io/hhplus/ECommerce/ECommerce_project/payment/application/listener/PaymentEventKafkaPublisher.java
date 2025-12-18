package io.hhplus.ECommerce.ECommerce_project.payment.application.listener;

import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentCompletedEvent;
import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.payment.infrastructure.kafka.PaymentKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Spring 이벤트를 Kafka로 전환하는 Publisher
 * - 트랜잭션 커밋 후 Kafka로 이벤트 발행
 */
@Slf4j
//@Component // 비활성화
@RequiredArgsConstructor
@Deprecated
public class PaymentEventKafkaPublisher {

    private final PaymentKafkaProducer paymentKafkaProducer;

    /**
     * 결제 완료 이벤트를 Kafka로 발행
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("결제 완료 이벤트 수신 -> Kafka 발행: orderId={}", event.orderId());
        paymentKafkaProducer.sendPaymentCompleted(event);
    }

    /**
     * 결제 실패 이벤트를 Kafka로 발행
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePaymentFailed(PaymentFailedEvent event) {
        log.info("결제 실패 이벤트 수신 -> Kafka 발행: orderId={}", event.orderId());
        paymentKafkaProducer.sendPaymentFailed(event);
    }
}
