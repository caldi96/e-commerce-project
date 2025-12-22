package io.hhplus.ECommerce.ECommerce_project.product.application.service;

import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockDecreasedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockIncreasedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.infrastructure.kafka.StockKafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class StockService {

    private final RedisStockService redisStockService;
    private final StockKafkaProducer stockKafkaProducer;

    /**
     * 재고 차감 (Redis) - 초고속 처리
     * CreateOrderFromProductUseCase.java 에서 사용
     */
    public void reserveStock(Long productId, Integer quantity) {
        // 1. Redis에서 즉시 재고 차감 (5-10ms)
        Long remaining = redisStockService.decreaseStock(productId, quantity);

        // 2. Kafka로 DB 동기화 이벤트 발행 (비동기)
        stockKafkaProducer.sendStockDecreased(
                new StockDecreasedEvent(productId, quantity)
        );
    }

    /**
     * 재고 복구 (Redis)
     * CreateOrderFromProductUseCase.java 에서 사용
     */
    public void compensateStock(Long productId, Integer quantity) {
        // 1. Redis 재고 복구
        redisStockService.increaseStock(productId, quantity);

        // 2. Kafka로 DB 동기화 이벤트 발행
        stockKafkaProducer.sendStockIncreased(
                new StockIncreasedEvent(productId, quantity)
        );
    }

    /**
     * 여러 상품 재고 차감 (Redis) - 초고속 처리
     * CreateOrderFromCartUseCase.java 에서 사용
     */
    public void reserveStocks(List<Map.Entry<Long, Integer>> sortedEntries) {
        for (Map.Entry<Long, Integer> entry : sortedEntries) {
            Long productId = entry.getKey();
            Integer totalQuantity = entry.getValue();

            // Redis에서 즉시 재고 차감 (5-10ms)
            Long remaining = redisStockService.decreaseStock(productId, totalQuantity);

            // kafka로 DB 동기화 이벤트 발행 (비동기)
            stockKafkaProducer.sendStockDecreased(
                    new StockDecreasedEvent(productId, totalQuantity)
            );
        }
    }

    /**
     * 여러 상품 재고 복구 (Redis)
     * CreateOrderFromCartUseCase.java 에서 사용
     */
    public void compensateStocks(List<Map.Entry<Long, Integer>> sortedEntries) {
        for (Map.Entry<Long, Integer> entry : sortedEntries) {
            Long productId = entry.getKey();
            Integer totalQuantity = entry.getValue();

            // Redis 재고 복구
            redisStockService.increaseStock(productId, totalQuantity);

            // DB 동기화 이벤트 발행
            stockKafkaProducer.sendStockIncreased(
                    new StockIncreasedEvent(productId, totalQuantity)
            );
        }
    }
}
