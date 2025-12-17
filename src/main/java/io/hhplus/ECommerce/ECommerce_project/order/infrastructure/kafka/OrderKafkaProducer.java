package io.hhplus.ECommerce.ECommerce_project.order.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 주문 이벤트를 Kafka로 발행하는 Producer
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // 토픽 이름 정의
    private static final String ORDER_VALIDATION_REQUESTED_TOPIC = "order-validation-requested";
    private static final String STOCK_DEDUCTION_REQUESTED_TOPIC = "stock-deduction-requested";
    private static final String ORDER_CREATION_REQUESTED_TOPIC = "order-creation-requested";
    private static final String VALIDATION_FAILED_TOPIC = "validation-failed";
    private static final String STOCK_DEDUCTION_FAILED_TOPIC = "stock-deduction-failed";
    private static final String ORDER_COMPLETED_TOPIC = "order-completed";
    private static final String ORDER_CREATION_FAILED_TOPIC = "order-creation-failed";

    /**
     * 주문 검증 요청 이벤트 발행
     */
    public void sendOrderValidationRequested(OrderFromProductValidationRequestedEvent event) {
        String key = String.valueOf(event.command().productId());
        send(ORDER_VALIDATION_REQUESTED_TOPIC, key, event, "주문 검증 요청");
    }

    /**
     * 재고 차감 요청 이벤트 발행
     */
    public void sendStockDeductionRequested(StockDeductionFromProductRequestedEvent event) {
        String key = String.valueOf(event.command().productId());
        send(STOCK_DEDUCTION_REQUESTED_TOPIC, key, event, "재고 차감 요청");
    }

    /**
     * 주문 생성 요청 이벤트 발행
     */
    public void sendOrderCreationRequested(OrderCreationFromProductRequestedEvent event) {
        String key = String.valueOf(event.command().userId());
        send(ORDER_CREATION_REQUESTED_TOPIC, key, event, "주문 생성 요청");
    }

    /**
     * 검증 실패 이벤트 발행 (보상 트랜잭션)
     */
    public void sendValidationFailed(ValidationFromProductFailedEvent event) {
        String key = String.valueOf(event.productId());
        send(VALIDATION_FAILED_TOPIC, key, event, "검증 실패");
    }

    /**
     * 재고 차감 실패 이벤트 발행 (보상 트랜잭션)
     */
    public void sendStockDeductionFailed(StockDeductionFromProductFailedEvent event) {
        String key = String.valueOf(event.productId());
        send(STOCK_DEDUCTION_FAILED_TOPIC, key, event, "재고 차감 실패");
    }

    /**
     * 주문 완료 이벤트 발행
     */
    public void sendOrderCompleted(OrderFromProductCompletedEvent event) {
        String key = String.valueOf(event.userId());
        send(ORDER_COMPLETED_TOPIC, key, event, "주문 완료");
    }

    /**
     * 주문 생성 실패 이벤트 발행
     */
    public void sendOrderCreationFailed(OrderCreationFromProductFailedEvent event) {
        String key = String.valueOf(event.reservations().get(0).productId());
        send(ORDER_CREATION_FAILED_TOPIC, key, event, "주문 생성 실패");
    }

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
