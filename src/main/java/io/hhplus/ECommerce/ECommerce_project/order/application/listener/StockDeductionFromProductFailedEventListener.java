package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromProductFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * DB 재고 차감 실패 이벤트 리스너
 * - DB 재고 차감 실패 시 Redis 재고 복구
 */
@Slf4j
//@Component // 스프링 이벤트를 kafka 비동기 이벤트로 변경
@RequiredArgsConstructor
public class StockDeductionFromProductFailedEventListener {

    private final RedisStockService redisStockService;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleStockDeductionFailure(StockDeductionFromProductFailedEvent event) {
        log.info("DB 재고 차감 실패 재고 복구 시작 - productId: {}, quantity: {}, reason: {}",
                event.productId(), event.quantity(), event.failureReason());

        try {
            // Redis 재고 복구
            redisStockService.increaseStock(event.productId(), event.quantity());

            log.info("DB 재고 차감 실패 재고 복구 완료 - productId: {}, quantity: {}",
                    event.productId(), event.quantity());

        } catch (Exception e) {
            log.error("DB 재고 차감 실패 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                    event.productId(), event.quantity(), e.getMessage(), e);

            // TODO: 재고 복구 실패 시 처리 로직
            // - Dead Letter Queue에 저장
            // - 관리자 알림 발송
            // - 재시도 스케줄링
            // - Kafka 도입 후 DLQ 토픽으로 전송
        }
    }
}
