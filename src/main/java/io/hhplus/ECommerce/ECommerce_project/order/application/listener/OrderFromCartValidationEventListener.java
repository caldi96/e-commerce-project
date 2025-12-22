package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.cart.application.service.CartFinderService;
import io.hhplus.ECommerce.ECommerce_project.cart.domain.entity.Cart;
import io.hhplus.ECommerce.ECommerce_project.cart.domain.service.CartDomainService;
import io.hhplus.ECommerce.ECommerce_project.common.exception.CouponException;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ProductException;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.UserCouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.UserCoupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.service.CouponDomainService;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromCartCommand;
import io.hhplus.ECommerce.ECommerce_project.order.application.dto.ValidatedOrderFromCartData;
import io.hhplus.ECommerce.ECommerce_project.order.domain.constants.ShippingPolicy;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromCartValidationRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromCartRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.ValidationFromCartFailedEvent;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 장바구니 주문 검증 이벤트 리스너
 * - Redis 재고 차감 후 검증 수행
 * - 검증 성공 시 DB 재고 차감 이벤트 발행
 * - 검증 실패 시 Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트에서 카프카 이벤트로 변경
@RequiredArgsConstructor
public class OrderFromCartValidationEventListener {
    private final UserDomainService userDomainService;
    private final UserFinderService userFinderService;
    private final CartDomainService cartDomainService;
    private final CartFinderService cartFinderService;
    private final ProductDomainService productDomainService;
    private final ProductFinderService productFinderService;
    private final CouponDomainService couponDomainService;
    private final CouponFinderService couponFinderService;
    private final UserCouponFinderService userCouponFinderService;
    private final PointDomainService pointDomainService;
    private final PointFinderService pointFinderService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleValidation(OrderFromCartValidationRequestedEvent event) {
        CreateOrderFromCartCommand command = event.command();
        List<Map.Entry<Long, Integer>> sortedEntries = event.sortedEntries();

        log.info("장바구니 주문 검증 이벤트 처리 시작 - userId: {}, 장바구니 아이템수: {}, 상품수: {}",
                command.userId(), command.cartItemIds().size(), sortedEntries.size());

        try {
            // 검증 및 계산
            ValidatedOrderFromCartData validatedOrderFromCartData = validateAndCalculate(command);

            log.info("장바구니 주문 검증 성공 - userId: {}, 총 상품수: {}, 최종금액: {}",
                    command.userId(), sortedEntries.size(), validatedOrderFromCartData.finalAmount());

            // DB 재고 차감 이벤트 발행
            applicationEventPublisher.publishEvent(
                    StockDeductionFromCartRequestedEvent.of(command, validatedOrderFromCartData)
            );

        } catch (Exception e) {
            log.error("장바구니 주문 검증 실패 - userId: {}, reason: {}",
                    command.userId(), e.getMessage(), e);

            // 검증 실패 -> Redis 재고 복구 이벤트 발행
            applicationEventPublisher.publishEvent(
                    ValidationFromCartFailedEvent.of(
                            command.userId(),
                            sortedEntries,
                            e.getMessage()
                    )
            );
        }
    }

    /**
     * 검증 및 계산 로직
     */
    private ValidatedOrderFromCartData validateAndCalculate(CreateOrderFromCartCommand command) {

        // 1. ID 검증
        userDomainService.validateId(command.userId());

        // 1. 사용자 확인
        User user = userFinderService.getUser(command.userId());

        // 2. 장바구니 아이템 조회 (cartItemIds)
        List<Cart> cartList = command.cartItemIds().stream()
                .map(cartId -> {

                    // cartId 검증
                    cartDomainService.validateId(cartId);

                    Cart cart = cartFinderService.getCart(cartId);

                    // 유저의 카트인지 확인
                    cartDomainService.validateSameUser(cart, user);

                    return cart;
                })
                .toList();

        // 3. 각 장바구니 아이템에 대해 상품 검증 및 재고 처리
        // 3-1. 상품별 주문 수량 집계 (같은 상품이 여러 장바구니 항목에 있을 수 있음)
        Map<Long, Integer> productOrderQuantityMap = new HashMap<>();
        for (Cart cart : cartList) {
            productOrderQuantityMap.merge(
                    cart.getProduct().getId(),
                    cart.getQuantity(),
                    Integer::sum
            );
        }

        // 3-2. 장바구니 아이템 오름차순 정렬 (데드락 방지: productId 오름차순 정렬, 원자적 처리)
        // 발생 가능한 데드락 예시 : A가 1번 상품 -> 2번 상품, B가 2번 상품 -> 1번 상품  이런 경우 서로 상품을 점유한 상태에서 안 놔줘서 안 넘어감
        List<Map.Entry<Long, Integer>> sortedEntries = productOrderQuantityMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();

        Map<Long, Product> productMap = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : sortedEntries) {
            Long productId = entry.getKey();
            Integer totalQuantity = entry.getValue();

            // 상품 도메인 검증
            productDomainService.validateId(productId);
            productDomainService.validateQuantity(totalQuantity);

            // 상품 조회 (가격, 활성화 상태, 최소/최대 주문량 정보용)
            Product product = productFinderService.getProduct(productId);

            // 주문 가능 여부 검증 (활성화 상태 체크)
            if (!product.isActive()) {
                throw new ProductException(
                        ErrorCode.PRODUCT_NOT_ACTIVE,
                        " 비활성 상태의 상품은 주문할 수 없습니다. productId=" + productId
                );
            }

            // 최소/최대 주문량 검증
            if (product.getMinOrderQuantity() != null && totalQuantity < product.getMinOrderQuantity()) {
                throw new ProductException(
                        ErrorCode.PRODUCT_MIN_ORDER_QUANTITY_NOT_MET,
                        " 최소 주문 수량: " + product.getMinOrderQuantity() + ", 요청 수량: " + totalQuantity + ", productId=" + productId
                );
            }

            if (product.getMaxOrderQuantity() != null && totalQuantity > product.getMaxOrderQuantity()) {
                throw new ProductException(
                        ErrorCode.PRODUCT_MAX_ORDER_QUANTITY_EXCEEDED,
                        " 최대 주문 수량: " + product.getMaxOrderQuantity() + ", 요청 수량: " + totalQuantity + ", productId=" + productId
                );
            }

            // 주문 금액 계산을 위해 productMap에 저장
            productMap.put(productId, product);
        }

        // 4. 주문 금액 계산
        BigDecimal totalAmount = calculateTotalAmount(cartList, productMap);

        // 5. 배송비 계산 (상수 클래스 사용)
        BigDecimal shippingFee = ShippingPolicy.calculateShippingFee(totalAmount);

        // 6. 쿠폰 처리
        BigDecimal discountAmount = BigDecimal.ZERO;

        Coupon coupon = null;

        if (command.couponId() != null) {

            // 도메인 검증
            couponDomainService.validateId(command.couponId());

            // 6-1. 사용자 쿠폰 조회
            UserCoupon userCoupon = userCouponFinderService
                    .getUserCouponByUserIdAndCouponId(command.userId(), command.couponId())
                    .orElseThrow(() -> new CouponException(ErrorCode.USER_COUPON_NOT_FOUND));

            // 6-2. 쿠폰 조회 및 검증
            coupon = couponFinderService.getCoupon(command.couponId());

            // 6-3. 쿠폰 유효성 검증 (활성화, 기간 등)
            coupon.validateAvailability();

            // 6-4. 사용자 쿠폰 사용 가능 여부 확인
            userCoupon.validateCanUse(coupon.getPerUserLimit());

            // 6-5. 할인 금액 계산 (최소 주문 금액 검증 포함)
            discountAmount = coupon.calculateDiscountAmount(totalAmount);
        }

        // 10. 포인트 차감 금액
        BigDecimal pointDeduction = (command.pointAmount() != null) ? command.pointAmount() : BigDecimal.ZERO;

        // 7. 포인트 잔액 검증 (사전 검증 - 재고 차감 전에 포인트 부족 감지)
        if (pointDeduction.compareTo(BigDecimal.ZERO) > 0) {

            // 사용 가능한 포인트 조회
            List<Point> availablePoints = pointFinderService.getAvailablePoints(command.userId());

            // 사용 가능한 포인트 합계 계산 (남은 금액 기준)
            BigDecimal totalAvailablePoint = availablePoints.stream()
                    .map(Point::getRemainingAmount)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            // 포인트 잔액 검증
            pointDomainService.validateAvailablePoint(totalAvailablePoint, command.pointAmount());
        }

        // 12. 최종 금액 계산
        BigDecimal finalAmount = totalAmount
                .add(shippingFee)
                .subtract(discountAmount)
                .subtract(pointDeduction);

        if (finalAmount.compareTo(BigDecimal.ZERO) < 0) {
            finalAmount = BigDecimal.ZERO;
        }

        return new ValidatedOrderFromCartData(
                user,
                cartList,
                sortedEntries,
                productMap,
                coupon,
                totalAmount,
                shippingFee,
                discountAmount,
                pointDeduction,
                finalAmount
        );
    }

    // 주문 금액 계산 헬퍼 메서드
    private BigDecimal calculateTotalAmount(List<Cart> cartList, Map<Long, Product> productMap) {
        BigDecimal totalAmount = BigDecimal.ZERO;

        for (Cart cart : cartList) {
            Product product = productMap.get(cart.getProduct().getId());

            BigDecimal itemTotalAmount = product.getPrice()
                    .multiply(BigDecimal.valueOf(cart.getQuantity()));
            totalAmount = totalAmount.add(itemTotalAmount);
        }

        return totalAmount;
    }
}
