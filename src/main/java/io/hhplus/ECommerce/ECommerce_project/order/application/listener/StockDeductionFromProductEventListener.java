package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.common.annotation.DistributedLock;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromProductRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromProductFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromProductRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.ProductFinderService;
import io.hhplus.ECommerce.ECommerce_project.product.domain.entity.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * DB 재고 차감 이벤트 리스너
 * - 검증 완료 후 DB 재고 차감 수행
 * - 차감 성공 시 주문 생성 이벤트 발행
 * - 차감 실패 시 Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트를 kafka 이벤트로 변경
@RequiredArgsConstructor
public class StockDeductionFromProductEventListener {

    private final ProductFinderService productFinderService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @DistributedLock(  // TODO: Kafka 도입 후 @DistributedLock을 Kafka Consumer에서 처리
            key = "'product:stock:' + #event.command().productId()",
            waitTime = 3L,
            leaseTime = 5L  // 재고 차감 + 판매량 증가
    )
    public void handleStockDeduction(StockDeductionFromProductRequestedEvent event) {
        log.info("DB 재고 차감 이벤트 처리 시작 - productId: {}, quantity: {}",
                event.command().productId(), event.command().quantity());

        try {
            // DB 재고 차감 (분산락으로 동시성 제어)
            Product product = productFinderService.getProduct(event.command().productId());

            // DB 재고 차감 및 판매량 증가
            product.decreaseStock(event.command().quantity());
            product.increaseSoldCount(event.command().quantity());

            log.info("DB 재고 차감 성공 - productId: {}, 남은재고: {}, 판매량: {}",
                    event.command().productId(), product.getStock(), product.getSoldCount());

            // DB 재고 차감 성공 -> 주문 생성 이벤트 발행
            applicationEventPublisher.publishEvent(
                    OrderCreationFromProductRequestedEvent.of(event.command(), event.validatedOrderFromProductData())
            );

        } catch (Exception e) {
            log.error("DB 재고 차감 실패 - productId: {}, quantity: {}, reason: {}",
                    event.command().productId(), event.command().quantity(), e.getMessage(), e);

            // DB 재고 차감 실패 -> Redis 재고 복구
            applicationEventPublisher.publishEvent(
                    StockDeductionFromProductFailedEvent.of(
                            event.command().productId(),
                            event.command().quantity(),
                            e.getMessage()
                    )
            );
        }
    }
}
