package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.application.service.StockRecoveryService;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromProductFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 주문 생성 실패 이벤트 리스너
 * - 주문 완료 트랜잭션 실패 시 재고 복구를 비동기로 처리
 */
@Slf4j
//@Component // 스프링 이벤트에서 kafka 이벤트로 변경
@RequiredArgsConstructor
public class OrderCreationFromProductFailedEventListener {

    private final StockRecoveryService stockRecoveryService;

    /**
     * 주문 생성 실패 이벤트 처리
     * - DB 재고 및 Redis 재고를 비동기로 복구
     * - 복수 상품 지원
     */
    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderCreationFailed(OrderCreationFromProductFailedEvent event) {
        log.info("주문 생성 실패 재고 복구 시작 - userId: {}, products: {}, reason: {}",
                event.userId(), event.reservations().size(), event.failureReason());

        try {
            // 각 상품별로 재고 복구 처리 (별도 서비스로 분산락 적용)
            for (OrderCreationFromProductFailedEvent.StockReservation reservation : event.reservations()) {
                stockRecoveryService.recoverStockForProduct(event.needsDbStockRecovery(), reservation);
            }

            log.info("주문 생성 실패 재고 복구 완료 - userId: {}, products: {}",
                    event.userId(), event.reservations().size());

        } catch (Exception e) {
            log.error("주문 생성 실패 재고 복구 실패 - userId: {}, products: {}, error: {}",
                    event.userId(), event.reservations().size(), e.getMessage(), e);

            // TODO: 재고 복구 실패 시 처리 로직
            // - Dead Letter Queue에 저장
            // - 관리자 알림 발송
            // - 재시도 스케줄링
            // - Kafka 도입 후 DLQ 토픽으로 전송
        }
    }
}
