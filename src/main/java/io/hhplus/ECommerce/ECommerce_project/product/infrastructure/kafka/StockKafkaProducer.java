package io.hhplus.ECommerce.ECommerce_project.product.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockDecreasedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockIncreasedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 재고 이벤트를 Kafka로 발행하는 Producer
 * - Redis 재고 변경 후 DB 동기화를 위한 이벤트 발행
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String STOCK_DECREASED_TOPIC = "stock-decreased";
    private static final String STOCK_INCREASED_TOPIC = "stock-increased";

    /**
     * 재고 차감 이벤트 발행
     */
    public void sendStockDecreased(StockDecreasedEvent event) {
        String key = String.valueOf(event.productId());
        send(STOCK_DECREASED_TOPIC, key, event, "재고 차감");
    }

    /**
     * 재고 증가 이벤트 발행
     */
    public void sendStockIncreased(StockIncreasedEvent event) {
        String key = String.valueOf(event.productId());
        send(STOCK_INCREASED_TOPIC, key, event, "재고 증가");
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
