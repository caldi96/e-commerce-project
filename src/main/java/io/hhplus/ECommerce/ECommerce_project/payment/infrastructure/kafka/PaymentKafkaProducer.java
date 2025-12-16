package io.hhplus.ECommerce.ECommerce_project.payment.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentCompletedEvent;
import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 결제 이벤트를 Kafka로 발행하는 Producer
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment-completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment-failed";

    /**
     * 결제 완료 이벤트 발행
     */
    public void sendPaymentCompleted(PaymentCompletedEvent event) {
        String key = String.valueOf(event.orderId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("결제 완료 이벤트 발행 성공: topic={}, partition={}, offset={}, orderId={}",
                        PAYMENT_COMPLETED_TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.orderId());
            } else {
                log.error("결제 완료 이벤트 발행 실패: orderId={}, error={}",
                        event.orderId(), ex.getMessage(), ex);
                // TODO: 실패 처리 로직 (재시도, DLQ 등)
            }
        });
    }

    /**
     * 결제 실패 이벤트 발행
     */
    public void sendPaymentFailed(PaymentFailedEvent event) {
        String key = String.valueOf(event.orderId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(PAYMENT_FAILED_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("결제 실패 이벤트 발행 성공: topic={}, partition={}, offset={}, orderId={}",
                        PAYMENT_FAILED_TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.orderId());
            } else {
                log.error("결제 실패 이벤트 발행 실패: orderId={}, error={}",
                        event.orderId(), ex.getMessage(), ex);
                // TODO: 실패 처리 로직 (재시도, DLQ 등)
            }
        });
    }
}
