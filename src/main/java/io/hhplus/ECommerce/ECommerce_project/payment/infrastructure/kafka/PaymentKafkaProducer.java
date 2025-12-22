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
 * - 실패 시 자동으로 재시도 (ProducerConfig.RETRIES_CONFIG)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment-completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment-failed";

    /**
     * 결제 완료 이벤트 발행 (비동기)
     * - Producer 레벨에서 자동 재시도 (최대 3회)
     */
    public void sendPaymentCompleted(PaymentCompletedEvent event) {
        String key = String.valueOf(event.orderId());
        send(PAYMENT_COMPLETED_TOPIC, key, event, "결제 완료");
    }

    /**
     * 결제 실패 이벤트 발행
     * - Producer 레벨에서 자동 재시도 (최대 3회)
     */
    public void sendPaymentFailed(PaymentFailedEvent event) {
        String key = String.valueOf(event.orderId());
        send(PAYMENT_FAILED_TOPIC, key, event, "결제 실패");
    }

    // ------------ 공동 전송 로직 -------------
    /**
     * 공통 전송 로직
     */
    private void send(String topic, String key, Object event, String eventName) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("{} 이벤트 발행 성공: topic={}, partition={}, offset={}",
                        eventName,
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("{} 이벤트 발행 실패: topic={}, error={}",
                        eventName, topic, ex.getMessage(), ex);
            }
        });
    }
}
