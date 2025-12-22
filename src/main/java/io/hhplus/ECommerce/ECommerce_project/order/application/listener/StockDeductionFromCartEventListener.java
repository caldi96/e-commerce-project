package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromCartRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromCartFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromCartRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.StockDeductionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 장바구니 DB 재고 차감 이벤트 리스너
 * - 검증 완료 후 DB 재고 차감 수행 (All or Nothing)
 * - 차감 성공 시 주문 생성 이벤트 발행
 * - 차감 실패 시 Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트를 카프카 이벤트로 변경
@RequiredArgsConstructor
public class StockDeductionFromCartEventListener {

    private final StockDeductionService stockDeductionService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleStockDeduction(StockDeductionFromCartRequestedEvent event) {
        log.info("장바구니 DB 재고 차감 이벤트 처리 시작 - userId: {}, 상품수: {}",
                event.command().userId(), event.validatedOrderFromCartData().sortedEntries().size());

        List<Map.Entry<Long, Integer>> successEntries = new ArrayList<>();

        try {
            // 모든 상품 재고 차감 (All or Nothing)
            for (Map.Entry<Long, Integer> entry : event.validatedOrderFromCartData().sortedEntries()) {
                Long productId = entry.getKey();
                Integer quantity = entry.getValue();

                // 외부 서비스 호출 → AOP 프록시 작동
                stockDeductionService.deductStockWithDistributedLock(productId, quantity);
                successEntries.add(entry);

                log.debug("DB 재고 차감 성공 - productId: {}, quantity: {}", productId, quantity);
            }

            log.info("전체 상품 DB 재고 차감 완료 - userId: {}, 성공수: {}",
                    event.command().userId(), successEntries.size());

            // DB 재고 차감 성공 -> 주문 생성 이벤트 발행
            applicationEventPublisher.publishEvent(
                    OrderCreationFromCartRequestedEvent.of(
                            event.command(),
                            event.validatedOrderFromCartData()
                    )
            );
        } catch (Exception e) {
            log.error("장바구니 DB 재고 차감 실패 - userId: {}, 실패 위치: {}/{}, reason: {}",
                    event.command().userId(), successEntries.size(),
                    event.validatedOrderFromCartData().sortedEntries().size(), e.getMessage(), e);

            // DB 재고 차감 실패 시 성공한 항목들 복구
            rollbackDbStock(successEntries);

            // Redis 재고 복구 이벤트 발행
            applicationEventPublisher.publishEvent(
                    StockDeductionFromCartFailedEvent.of(
                            event.command().userId(),
                            event.validatedOrderFromCartData().sortedEntries(),
                            e.getMessage()
                    )
            );
        }
    }

    /**
     * DB 재고 복구 (성공한 항목만)
     */
    private void rollbackDbStock(List<Map.Entry<Long, Integer>> successEntries) {
        log.warn("DB 재고 복구 시작 - 복구 대상 상품수: {}", successEntries.size());

        for (Map.Entry<Long, Integer> entry : successEntries) {
            try {
                // 외부 서비스 호출 → AOP 프록시 작동
                stockDeductionService.recoverStockWithDistributedLock(entry.getKey(), entry.getValue());
                log.debug("DB 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("DB 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                // TODO: 재고 복구 실패 시 처리 로직
                // - Dead Letter Queue에 저장
                // - 관리자 알림 발송
                // - 재시도 스케줄링
            }
        }

        log.warn("DB 재고 복구 완료 - 복구 상품수: {}", successEntries.size());
    }
}
