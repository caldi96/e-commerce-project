package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.ValidationFromCartFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Map;

/**
 * 장바구니 검증 실패 이벤트 리스너
 * - 검증 실패 시 Redis 재고 복구
 */
@Slf4j
//@Component // 스프링 이벤트를 카프카 이벤트로 변경
@RequiredArgsConstructor
public class ValidationFromCartFailedEventListener {

    private final RedisStockService redisStockService;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleValidationFailed(ValidationFromCartFailedEvent event) {
        log.info("장바구니 검증 실패 재고 복구 시작 - userId: {}, 상품수: {}, reason: {}",
                event.userId(), event.sortedEntries().size(), event.failureReason());

        int successCount = 0;
        int failCount = 0;

        // Redis 재고 복구 (모든 상품)
        for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
            try {
                redisStockService.increaseStock(entry.getKey(), entry.getValue());
                successCount++;

                log.debug("Redis 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());
            } catch (Exception e) {
                failCount++;
                log.error("Redis 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                // TODO: 재고 복구 실패 시 처리 로직
                // - Dead Letter Queue에 저장
                // - 관리자 알림 발송
                // - 재시도 스케줄링
                // - Kafka 도입 후 DLQ 토픽으로 전송
            }
        }

        log.info("장바구니 검증 실패 재고 복구 완료 - userId: {}, 성공: {}, 실패: {}",
                event.userId(), successCount, failCount);
    }
}
