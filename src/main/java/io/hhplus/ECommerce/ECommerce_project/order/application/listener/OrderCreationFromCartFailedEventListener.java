package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromCartFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.StockDeductionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Map;

/**
 * 장바구니 주문 생성 실패 이벤트 리스너
 * - 주문 생성 실패 시 DB 재고 + Redis 재고 복구
 */
@Slf4j
//@Component // 스프링 이벤트를 비동기 이벤트로 변경
@RequiredArgsConstructor
public class OrderCreationFromCartFailedEventListener {

    private final RedisStockService redisStockService;
    private final StockDeductionService stockDeductionService;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleOrderCreationFailed(OrderCreationFromCartFailedEvent event) {
        log.info("장바구니 주문 생성 실패 재고 복구 시작 - userId: {}, 상품수: {}, reason: {}",
                event.userId(), event.sortedEntries().size(), event.failureReason());

        int dbSuccessCount = 0;
        int dbFailCount = 0;
        int redisSuccessCount = 0;
        int redisFailCount = 0;

        try {
            // 1. DB 재고 복구 (필요한 경우)
            if (event.needsDbStockRecovery()) {
                for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
                    try {
                        // 외부 서비스 호출 → AOP 프록시 작동
                        stockDeductionService.recoverStockWithDistributedLock(entry.getKey(), entry.getValue());
                        dbSuccessCount++;

                        log.debug("DB 재고 복구 성공 - productId: {}, quantity: {}",
                                entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        dbFailCount++;
                        log.error("DB 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                                entry.getKey(), entry.getValue(), e.getMessage(), e);
                    }
                }

                log.info("DB 재고 복구 완료 - 성공: {}, 실패: {}", dbSuccessCount, dbFailCount);
            }

            // 2. Redis 재고 복구 (모든 상품)
            for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
                try {
                    redisStockService.increaseStock(entry.getKey(), entry.getValue());
                    redisSuccessCount++;

                    log.debug("Redis 재고 복구 성공 - productId: {}, quantity: {}",
                            entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    redisFailCount++;
                    log.error("Redis 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                            entry.getKey(), entry.getValue(), e.getMessage(), e);
                }
            }

            log.info("장바구니 주문 생성 실패 재고 복구 완료 - userId: {}, Redis 성공: {}, Redis 실패: {}",
                    event.userId(), redisSuccessCount, redisFailCount);
        } catch (Exception e) {
            log.error("장바구니 주문 생성 실패 재고 복구 중 오류 - userId: {}, error: {}",
                    event.userId(), e.getMessage(), e);

            // TODO: 재고 복구 실패 시 처리 로직
            // - Dead Letter Queue에 저장
            // - 관리자 알림 발송
            // - 재시도 스케줄링
            // - Kafka 도입 후 DLQ 토픽으로 전송
        }
    }
}
