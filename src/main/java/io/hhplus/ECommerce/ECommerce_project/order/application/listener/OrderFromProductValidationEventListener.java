package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.common.exception.CouponException;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ProductException;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.UserCouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.UserCoupon;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromProductCommand;
import io.hhplus.ECommerce.ECommerce_project.order.application.dto.ValidatedOrderFromProductData;
import io.hhplus.ECommerce.ECommerce_project.order.domain.constants.ShippingPolicy;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromProductValidationRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromProductRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.ValidationFromProductFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.point.application.service.PointFinderService;
import io.hhplus.ECommerce.ECommerce_project.point.domain.entity.Point;
import io.hhplus.ECommerce.ECommerce_project.point.domain.service.PointDomainService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.ProductFinderService;
import io.hhplus.ECommerce.ECommerce_project.product.domain.entity.Product;
import io.hhplus.ECommerce.ECommerce_project.product.domain.service.ProductDomainService;
import io.hhplus.ECommerce.ECommerce_project.user.application.service.UserFinderService;
import io.hhplus.ECommerce.ECommerce_project.user.domain.entity.User;
import io.hhplus.ECommerce.ECommerce_project.user.domain.service.UserDomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.math.BigDecimal;
import java.util.List;

/**
 * 주문 검증 이벤트 리스너
 * - Redis 재고 차감 후 주문 데이터 검증
 * - 검증 성공 시 DB 재고 차감 이벤트 발행
 * - 검증 실패 시 Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트를 kafka 이벤트로 변경
@RequiredArgsConstructor
public class OrderFromProductValidationEventListener {

    private final UserDomainService userDomainService;
    private final UserFinderService userFinderService;
    private final ProductDomainService productDomainService;
    private final ProductFinderService productFinderService;
    private final CouponFinderService couponFinderService;
    private final UserCouponFinderService userCouponFinderService;
    private final PointDomainService pointDomainService;
    private final PointFinderService pointFinderService;

    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleValidation(OrderFromProductValidationRequestedEvent event) {
        log.info("주문 검증 이벤트 처리 시작 - userId: {}, productId: {}, quantity: {}",
                event.command().userId(), event.command().productId(), event.command().quantity());

        try {
            // 검증 및 계산 수행
            ValidatedOrderFromProductData validatedOrderFromProductData = validateAndCalculate(event.command());

            log.info("주문 검증 성공 - userId: {}, productId: {}, totalAmount: {}",
                    event.command().userId(), event.command().productId(), validatedOrderFromProductData.totalAmount());

            // 검증 성공 -> DB 재고 차감 이벤트 발행
            applicationEventPublisher.publishEvent(
                    StockDeductionFromProductRequestedEvent.of(event.command(), validatedOrderFromProductData)
            );

        } catch (Exception e) {
            log.error("주문 검증 실패 - userId: {}, productId: {}, reason: {}",
                    event.command().userId(), event.command().productId(), e.getMessage(), e);

            // 검증 실패 -> Redis 재고 복구
            applicationEventPublisher.publishEvent(
                    ValidationFromProductFailedEvent.of(
                            event.command().productId(),
                            event.command().quantity(),
                            e.getMessage()
                    )
            );
        }
    }

    private ValidatedOrderFromProductData validateAndCalculate(CreateOrderFromProductCommand command) {

        // 1. ID 검증
        userDomainService.validateId(command.userId());

        // 1. 사용자 확인 (락 없이)
        User user = userFinderService.getUser(command.userId());

        // 2. 상품 도메인 검증
        productDomainService.validateId(command.productId());
        productDomainService.validateQuantity(command.quantity());

        // 3. 상품 조회 (가격, 활성화 상태, 최소/최대 주문량 정보용)
        Product product = productFinderService.getProduct(command.productId());

        // 4. 주문 가능 여부 검증 (활성화 상태 체크)
        if (!product.isActive()) {
            throw new ProductException(
                    ErrorCode.PRODUCT_NOT_ACTIVE,
                    " 비활성 상태의 상품은 주문할 수 없습니다."
            );
        }

        // 5. 최소/최대 주문량 검증
        if (product.getMinOrderQuantity() != null && command.quantity() < product.getMinOrderQuantity()) {
            throw new ProductException(
                    ErrorCode.PRODUCT_MIN_ORDER_QUANTITY_NOT_MET,
                    " 최소 주문 수량: " + product.getMinOrderQuantity() + ", 요청 수량: " + command.quantity()
            );
        }

        if (product.getMaxOrderQuantity() != null && command.quantity() > product.getMaxOrderQuantity()) {
            throw new ProductException(
                    ErrorCode.PRODUCT_MAX_ORDER_QUANTITY_EXCEEDED,
                    " 최대 주문 수량: " + product.getMaxOrderQuantity() + ", 요청 수량: " + command.quantity()
            );
        }

        // 6. 주문 금액 계산
        BigDecimal totalAmount = product.getPrice()
                .multiply(BigDecimal.valueOf(command.quantity()));

        // 7. 배송비 계산
        BigDecimal shippingFee = ShippingPolicy.calculateShippingFee(totalAmount);

        // 8. 쿠폰 사전 검증 (락 없이)
        BigDecimal discountAmount = BigDecimal.ZERO;

        Coupon coupon = null;

        if (command.couponId() != null) {
            // 8-1. 사용자 쿠폰 조회 (락 없음)
            UserCoupon userCoupon = userCouponFinderService
                    .getUserCouponByUserIdAndCouponId(command.userId(), command.couponId())
                    .orElseThrow(() -> new CouponException(ErrorCode.USER_COUPON_NOT_FOUND));

            // 8-2. 쿠폰 조회 및 검증
            coupon = couponFinderService.getCoupon(command.couponId());

            // 8-3. 쿠폰 유효성 검증 (활성화, 기간 등)
            coupon.validateAvailability();

            // 8-4. 사용자 쿠폰 사용 가능 여부 확인
            userCoupon.validateCanUse(coupon.getPerUserLimit());

            // 8-5. 할인 금액 계산 (최소 주문 금액 검증 포함)
            discountAmount = coupon.calculateDiscountAmount(totalAmount);
        }

        // 9. 포인트 사전 검증
        if (command.pointAmount() != null
                && command.pointAmount().compareTo(BigDecimal.ZERO) > 0) {

            // 사용 가능한 포인트 조회
            List<Point> availablePoints = pointFinderService.getAvailablePoints(command.userId());

            // 사용 가능한 포인트 합계 계산 (남은 금액 기준)
            BigDecimal totalAvailablePoint = availablePoints.stream()
                    .map(Point::getRemainingAmount)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            // 포인트 잔액 검증
            pointDomainService.validateAvailablePoint(totalAvailablePoint, command.pointAmount());
        }

        // 검증된 데이터 반환
        return new ValidatedOrderFromProductData(totalAmount, shippingFee, discountAmount);
    }
}
