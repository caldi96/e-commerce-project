package io.hhplus.ECommerce.ECommerce_project.coupon.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.common.annotation.DistributedLock;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.RedisCouponService;
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final UserCouponRepository userCouponRepository;
    private final UserFinderService userFinderService;
    private final CouponFinderService couponFinderService;
    private final RedisCouponService redisCouponService;
    private final CouponKafkaProducer couponKafkaProducer;

    /**
     * 쿠폰 발급 이벤트 처리
     * - Redis 발급 성공 후 DB에 저장
     * - 수동 커밋: 중요한 트랜잭션이므로 명시적 커밋 필요
     */
    @Transactional
    @KafkaListener(
            topics = "coupon-issued",
            groupId = "coupon-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeCouponIssued(CouponIssuedEvent event, Acknowledgment acknowledgment) {
        log.info("쿠폰 발급 이벤트 수신 - userId: {}, couponId: {}", event.userId(), event.couponId());

        try {
            // 1. User 및 Coupon 조회
            User user = userFinderService.getUser(event.userId());
            Coupon coupon = couponFinderService.getCoupon(event.couponId());

            // 2. UserCoupon 생성 및 저장
            UserCoupon userCoupon = UserCoupon.issueCoupon(user, coupon);
            userCouponRepository.save(userCoupon);

            log.info("쿠폰 발급 DB 저장 성공 - userId: {}, couponId: {}", event.userId(), event.couponId());

            // 3. 수량 증가 이벤트 발행 (집계 데이터는 별도 처리)
            couponKafkaProducer.sendCouponQuantityIncrease(
                    CouponQuantityIncreaseEvent.of(event.couponId())
            );

            // 4. 수동 커밋
            acknowledgment.acknowledge();

        } catch (DataIntegrityViolationException e) {
            // DB 유니크 제약 위반 = 이미 발급받음
            log.error("쿠폰 중복 발급 감지 - userId: {}, couponId: {}", event.userId(), event.couponId(), e);

            couponKafkaProducer.sendCouponIssueFailed(
                    CouponIssueFailedEvent.of(event.userId(), event.couponId(), "DUPLICATE_ISSUE")
            );

            acknowledgment.acknowledge(); // 중복은 재처리 불필요

        } catch (Exception e) {
            // 기타 예외 발생 시 재시도 또는 DLQ 전송
            log.error("쿠폰 발급 DB 저장 실패 - userId: {}, couponId: {}", event.userId(), event.couponId(), e);

            // Kafka로 발급 실패 이벤트 발행
            couponKafkaProducer.sendCouponIssueFailed(
                    CouponIssueFailedEvent.of(event.userId(), event.couponId(), "DB_SAVE_FAILED")
            );

            // ack 하지 않으면 자동으로 재시도 후 DLQ로 전송됨
            throw e;
        }
    }

    /**
     * 쿠폰 수량 증가 이벤트 처리
     * - DB의 issuedQuantity를 증가 (집계 데이터 동기화)
     * - 수동 커밋: 집계 데이터 정합성이 중요
     */
    @Transactional
    @KafkaListener(
            topics = "coupon-quantity-increase",
            groupId = "coupon-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"  // 수동 커밋
    )
    @DistributedLock(
            key = "'coupon:increase:' + #event.couponId()",
            waitTime = 2L,
            leaseTime = 3L
    )
    public void consumeCouponQuantityIncrease(CouponQuantityIncreaseEvent event, Acknowledgment acknowledgment) {
        log.info("쿠폰 수량 증가 이벤트 수신 - couponId: {}", event.couponId());

        try {
            Coupon coupon = couponFinderService.getCoupon(event.couponId());
            coupon.increaseIssuedQuantity();

            log.info("쿠폰 수량 증가 완료 - couponId: {}", event.couponId());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("쿠폰 수량 증가 실패 - couponId: {}", event.couponId(), e);
            // ack 하지 않으면 재시도 후 DLQ로 전송
            throw e;
        }
    }

    /**
     * 쿠폰 발급 실패 이벤트 처리 (보상 트랜잭션)
     * - Redis 롤백 처리
     * - 수동 커밋: Redis 롤백은 중요한 보상 트랜잭션
     */
    @KafkaListener(
            topics = "coupon-issue-failed",
            groupId = "coupon-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"  // 수동 커밋
    )
    public void consumeCouponIssueFailed(CouponIssueFailedEvent event, Acknowledgment acknowledgment) {
        log.warn("쿠폰 발급 실패 이벤트 수신 - Redis 롤백 시작 - userId: {}, couponId: {}, reason: {}",
                event.userId(), event.couponId(), event.failureReason());

        try {
            redisCouponService.cancelIssueCoupon(event.couponId(), event.userId());
            log.info("Redis 롤백 성공 - userId: {}, couponId: {}", event.userId(), event.couponId());

            acknowledgment.acknowledge();   // 수동 커밋

        } catch (Exception e) {
            log.error("Redis 롤백 실패 - userId: {}, couponId: {} - 재시도 필요",
                    event.userId(), event.couponId(), e);
            // ack 하지 않으면 재시도 후 DLQ로 전송
            throw e;
        }
    }
}
