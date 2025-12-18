package io.hhplus.ECommerce.ECommerce_project.product.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ProductException;
import io.hhplus.ECommerce.ECommerce_project.product.domain.entity.Product;
import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockDecreasedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.domain.event.StockIncreasedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.infrastructure.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 Kafka Consumer
 * - Redis 재고 변경 후 DB 동기화 처리
 * - Eventual Consistency 패턴
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockKafkaConsumer {

    private final ProductRepository productRepository;

    /**
     * 1. 재고 차감 이벤트 처리
     * - Redis에서 재고 차감 후 DB에 반영
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "stock-decreased",
            groupId = "stock-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeStockDecreased(StockDecreasedEvent event, Acknowledgment acknowledgment) {
        log.info("재고 차감 이벤트 수신 - productId: {}, quantity: {}",
                event.productId(), event.quantity());

        try {
            // 비관적 락으로 상품 조회 (DB 동시성 제어)
            Product product = productRepository.findByIdWithLock(event.productId())
                    .orElseThrow(() -> new ProductException(ErrorCode.PRODUCT_NOT_FOUND));

            // DB 재고 차감 및 판매량 증가
            product.decreaseStock(event.quantity());
            product.increaseSoldCount(event.quantity());

            log.info("DB 재고 차감 완료 - productId: {}, quantity: {}, remainingStock: {}",
                    event.productId(), event.quantity(), product.getStock());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("DB 재고 차감 실패 - productId: {}, quantity: {}, error: {}",
                    event.productId(), event.quantity(), e.getMessage(), e);

            // TODO: 실패 시 보상 트랜잭션 또는 알림 발송
            // - Redis 재고 복구
            // - 관리자 알림
            // - Dead Letter Queue 저장
            throw e;  // 재시도 후 DLQ
        }
    }

    /**
     * 2. 재고 증가 이벤트 처리
     * - 보상 트랜잭션으로 재고 복구 시 DB 반영
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "stock-increased",
            groupId = "stock-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeStockIncreased(StockIncreasedEvent event, Acknowledgment acknowledgment) {
        log.info("재고 증가 이벤트 수신 - productId: {}, quantity: {}",
                event.productId(), event.quantity());

        try {
            // 비관적 락으로 상품 조회
            Product product = productRepository.findByIdWithLock(event.productId())
                    .orElseThrow(() -> new ProductException(ErrorCode.PRODUCT_NOT_FOUND));

            // DB 재고 증가 및 판매량 감소
            product.increaseStock(event.quantity());
            product.decreaseSoldCount(event.quantity());

            log.info("DB 재고 증가 완료 - productId: {}, quantity: {}, remainingStock: {}",
                    event.productId(), event.quantity(), product.getStock());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("DB 재고 증가 실패 - productId: {}, quantity: {}, error: {}",
                    event.productId(), event.quantity(), e.getMessage(), e);

            throw e;  // 재시도 후 DLQ
        }
    }
}
