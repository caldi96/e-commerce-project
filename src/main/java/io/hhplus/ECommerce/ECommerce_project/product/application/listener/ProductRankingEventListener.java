package io.hhplus.ECommerce.ECommerce_project.product.application.listener;

import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentCompletedEvent;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisRankingService;
import io.hhplus.ECommerce.ECommerce_project.product.domain.event.ProductViewedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 상품 랭킹 이벤트 리스너
 * - Kafka로부터 결제 완료 이벤트를 수신하여 Redis 랭킹 업데이트
 * - Eventual Consistency 패턴 적용
 * - 실패 시 재시도 로직 적용 (최대 3회)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProductRankingEventListener {

    private final RedisRankingService redisRankingService;

    /**
     * Kafka로부터 결제 완료 이벤트 수신
     * - Redis 랭킹 업데이트
     * - 실패 시 최대 3회 재시도 (1초 간격)
     */
    @KafkaListener(
            topics = "payment-completed",
            groupId = "product-ranking-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"  // 자동 커밋
    )
    @Retryable(
            retryFor = Exception.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("결제 완료 이벤트 수신 - orderId: {}", event.orderId());

        try {
            event.orderItems().forEach(orderItemInfo -> {
                redisRankingService.incrementSoldCount(
                        orderItemInfo.productId(),
                        orderItemInfo.quantity()
                );
                log.debug("Redis 랭킹 업데이트 완료 - productId: {}, quantity: {}",
                        orderItemInfo.productId(), orderItemInfo.quantity());
            });

            log.info("결제 완료 이벤트 처리 완료 - orderId: {}, itemCount: {}",
                    event.orderId(), event.orderItems().size());
        } catch (Exception e) {
            log.error("결제 완료 이벤트 처리 실패 - orderId: {}", event.orderId(), e);
            // 자동 커밋이므로 예외 발생 시 재시도 로직에 의존
            throw e;
        }
    }

    /**
     * 결제 완료 이벤트 처리 재시도 실패 시 호출
     * - 3회 재시도 후에도 실패하면 이 메서드가 호출됨
     */
    @Recover
    public void handlePaymentCompletedRecover(Exception e, PaymentCompletedEvent event) {
        log.error("결제 완료 이벤트 처리 최종 실패 - orderId: {}, 재시도 3회 모두 실패", event.orderId(), e);

        // TODO: 실패 시 처리 로직
        // - Dead Letter Queue에 저장
        // - 관리자 알림 발송
        // - 모니터링 시스템에 알림
    }

    /**
     * 상품 조회 이벤트 처리
     * - 트랜잭션 커밋 후 Redis 조회수 증가
     * - 비동기로 처리되어 상품 조회 응답 속도에 영향 없음
     */
    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleProductViewed(ProductViewedEvent event) {
        try {
            log.debug("상품 조회 이벤트 수신 - productId: {}", event.productId());

            redisRankingService.incrementViewCount(event.productId());

            log.debug("Redis 조회수 증가 완료 - productId: {}", event.productId());
        } catch (Exception e) {
            // Redis 업데이트 실패는 로깅만 하고 트랜잭션에 영향 없음
            log.error("Redis 조회수 증가 실패 - productId: {}", event.productId(), e);

            // TODO: 실패 시 처리 로직
            // - 재시도 큐에 추가
            // - 모니터링 알림
        }
    }
}
