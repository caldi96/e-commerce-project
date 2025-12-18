package io.hhplus.ECommerce.ECommerce_project.coupon.application;

import io.hhplus.ECommerce.ECommerce_project.common.exception.CouponException;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.command.IssueCouponCommand;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.RedisCouponMetadataService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.RedisCouponService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssuedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.service.CouponDomainService;
import io.hhplus.ECommerce.ECommerce_project.coupon.infrastructure.kafka.CouponKafkaProducer;
import io.hhplus.ECommerce.ECommerce_project.user.domain.service.UserDomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IssueCouponUseCase {

    private final UserDomainService userDomainService;
    private final CouponDomainService couponDomainService;
    private final CouponFinderService couponFinderService;
    private final RedisCouponService redisCouponService;
    private final RedisCouponMetadataService redisCouponMetadataService;
    private final CouponKafkaProducer couponKafkaProducer;

    /**
     * Redis Lua Script + Kafka 이벤트 발행
     * - Lua Script가 원자성 보장 (분산 락 불필요)
     * - Redis 발급 성공 후 즉시 반환
     * - DB 저장은 Kafka 이벤트로 처리
     */
    public void execute(IssueCouponCommand command) {

        userDomainService.validateId(command.userId());
        couponDomainService.validateId(command.couponId());

        // 1. Redis에서 totalQuantity 조회 (초고속)
        Integer totalQuantity = getTotalQuantityWithFallback(command.couponId());

        // 2. Redis Lua Script로 선착순 발급 시도
        boolean issued = redisCouponService.tryIssueCoupon(
                command.couponId(),
                command.userId(),
                totalQuantity
        );

        if (!issued) {
            throw new CouponException(ErrorCode.COUPON_ALL_ISSUED);
        }

        // 3. Kafka 이벤트 발행 (DB 저장은 Consumer에서 처리)
        couponKafkaProducer.sendCouponIssued(
                CouponIssuedEvent.of(command.userId(), command.couponId())
        );

        log.info("쿠폰 발급 이벤트 발행 완료 - userId: {}, couponId: {}", command.userId(), command.couponId());
    }

    /**
     * totalQuantity 조회 (Redis → DB Fallback)
     * - Redis에 있으면 Redis에서 조회 (초고속)
     * - 없으면 DB 조회 후 Redis 캐싱 (최초 1회)
     */
    private Integer getTotalQuantityWithFallback(Long couponId) {
        // 1. Redis 시도
        Integer totalQuantity = redisCouponMetadataService.getTotalQuantity(couponId);

        if (totalQuantity != null) {
            log.debug("Redis에서 쿠폰 수량 조회 - couponId: {}, totalQuantity: {}", couponId, totalQuantity);
            return totalQuantity;
        }

        // 2. Redis에 없으면 DB 조회
        log.info("Redis에 메타데이터 없음, DB 조회 - couponId: {}", couponId);
        Coupon coupon = couponFinderService.getCoupon(couponId);

        // 3. Redis 캐싱 (다음번엔 빠르게)
        redisCouponMetadataService.saveCouponMetadata(
                coupon.getId(),
                coupon.getTotalQuantity(),
                coupon.isActive(),
                coupon.getStartDate(),
                coupon.getEndDate()
        );

        return coupon.getTotalQuantity();
    }
}