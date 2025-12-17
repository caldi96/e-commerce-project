package io.hhplus.ECommerce.ECommerce_project.coupon.application.listener;

import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.RedisCouponService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssueFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 쿠폰 발급 실패 이벤트 리스너
 * - DB 저장 실패 시 Redis 롤백 처리
 */
@Slf4j
//@Component 스프링 이벤트가 아닌 kafka 이벤트에서 수행함
@RequiredArgsConstructor
public class CouponIssueFailedEventListener {

    private final RedisCouponService redisCouponService;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleCouponIssueFailed(CouponIssueFailedEvent event) {
        log.warn("쿠폰 발급 실패 - Redis 롤백 시작 - userId: {}, couponId: {}, reason: {}",
                event.userId(), event.couponId(), event.failureReason());

        try {
            redisCouponService.cancelIssueCoupon(event.couponId(), event.userId());
            log.info("Redis 롤백 성공 - userId: {}, couponId: {}", event.userId(), event.couponId());

        } catch (Exception e) {
            // Redis 롤백 실패 시 로그 기록 (추후 DLQ 전송)
            log.error("Redis 롤백 실패 - userId: {}, couponId: {} - 수동 처리 필요",
                    event.userId(), event.couponId(), e);
            // TODO: DLQ(Dead Letter Queue)로 전송하여 재처리 필요
        }
    }
}
