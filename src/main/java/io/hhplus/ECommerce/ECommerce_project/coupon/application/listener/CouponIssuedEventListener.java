package io.hhplus.ECommerce.ECommerce_project.coupon.application.listener;

import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.UserCoupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssueFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssuedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponQuantityIncreaseEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.infrastructure.UserCouponRepository;
import io.hhplus.ECommerce.ECommerce_project.user.application.service.UserFinderService;
import io.hhplus.ECommerce.ECommerce_project.user.domain.entity.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 쿠폰 발급 이벤트 리스너
 * - Redis 발급 성공 후 DB에 저장
 * - 실패 시 보상 이벤트 발행 (Redis 롤백)
 */
@Slf4j
//@Component 스프링 이벤트를 카프카에서 처리
@RequiredArgsConstructor
public class CouponIssuedEventListener {

    private final UserCouponRepository userCouponRepository;
    private final UserFinderService userFinderService;
    private final CouponFinderService couponFinderService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // 현재는 @DistributedLock이 스레드 A에 락을 걸었는데 @Async가 새로운 스레드 B를 실행해서 비즈니스 로직이 스레드 B에서 돌아가는 바람에 분산락이 안걸릴 수 있음 -> kafka 도입으로 해결
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleCouponIssued(CouponIssuedEvent event) {
        log.info("쿠폰 발급 이벤트 처리 시작 - userId: {}, couponId: {}", event.userId(), event.couponId());

        try {
            // 1. User 및 Coupon 조회
            User user = userFinderService.getUser(event.userId());
            Coupon coupon = couponFinderService.getCoupon(event.couponId());

            // 2. UserCoupon 생성 및 저장
            UserCoupon userCoupon = UserCoupon.issueCoupon(user, coupon);
            userCouponRepository.save(userCoupon);

            log.info("쿠폰 발급 DB 저장 성공 - userId: {}, couponId: {}", event.userId(), event.couponId());

            // 3. 수량 증가 이벤트 발행 (집계 데이터는 별도 처리)
            applicationEventPublisher.publishEvent(
                    CouponQuantityIncreaseEvent.of(event.couponId())
            );

        } catch (DataIntegrityViolationException e) {
            // DB 유니크 제약 위반 = 이미 발급받음
            log.error("쿠폰 중복 발급 감지 - userId: {}, couponId: {}", event.userId(), event.couponId(), e);
            applicationEventPublisher.publishEvent(
                    CouponIssueFailedEvent.of(event.userId(), event.couponId(), "DUPLICATE_ISSUE")
            );

        } catch (Exception e) {
            // 기타 예외 발생 시 Redis 롤백
            log.error("쿠폰 발급 DB 저장 실패 - userId: {}, couponId: {}", event.userId(), event.couponId(), e);
            applicationEventPublisher.publishEvent(
                    CouponIssueFailedEvent.of(event.userId(), event.couponId(), "DB_SAVE_FAILED")
            );
        }
    }
}
