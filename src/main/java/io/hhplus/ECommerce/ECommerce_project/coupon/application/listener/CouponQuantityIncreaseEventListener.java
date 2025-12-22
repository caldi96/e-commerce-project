package io.hhplus.ECommerce.ECommerce_project.coupon.application.listener;

import io.hhplus.ECommerce.ECommerce_project.common.annotation.DistributedLock;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponQuantityIncreaseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 쿠폰 수량 증가 이벤트 리스너
 * - UserCoupon 저장 성공 후 발행되는 이벤트 처리
 * - DB의 issuedQuantity를 증가 (집계 데이터 동기화)
 */
@Slf4j
//@Component // 현재 스프링 이벤트가 아닌 kafka 이벤트에서 처리
@RequiredArgsConstructor
public class CouponQuantityIncreaseEventListener {

    private final CouponFinderService couponFinderService;

    @Async  // Kafka 도입 시 제거
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @DistributedLock(   // kafka 도입 후 @DistributedLock을 Kafka Consumer에서 처리
            key = "'coupon:increase:' + #event.couponId()",
            waitTime = 2L,
            leaseTime = 3L  // 수량만 증가하므로 락 보유 시간을 5L -> 3L로 수정
    )
    public void handleQuantityIncrease(CouponQuantityIncreaseEvent event) {
        log.info("쿠폰 수량 증가 시작 - couponId: {}", event.couponId());

        try {
            Coupon coupon = couponFinderService.getCoupon(event.couponId());
            coupon.increaseIssuedQuantity();
            log.info("쿠폰 수량 증가 완료 - couponId: {}", event.couponId());

        } catch (Exception e) {
            // 수량 증가 실패 시 로그 기록
            // UserCoupon은 이미 저장되었으므로 재시도 필요
            log.error("쿠폰 수량 증가 실패 - couponId: {} - 재시도 필요", event.couponId(), e);
            // TODO: DLQ로 전송하여 재처리
        }
    }
}
