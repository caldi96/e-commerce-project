package io.hhplus.ECommerce.ECommerce_project.order.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.common.annotation.DistributedLock;
import io.hhplus.ECommerce.ECommerce_project.common.exception.CouponException;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ProductException;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.UserCouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.UserCoupon;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromProductCommand;
import io.hhplus.ECommerce.ECommerce_project.order.application.dto.ValidatedOrderFromProductData;
import io.hhplus.ECommerce.ECommerce_project.order.application.service.OrderCompletionService;
import io.hhplus.ECommerce.ECommerce_project.order.domain.constants.ShippingPolicy;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.*;
import io.hhplus.ECommerce.ECommerce_project.order.presentation.response.CreateOrderResponse;
import io.hhplus.ECommerce.ECommerce_project.point.application.service.PointFinderService;
import io.hhplus.ECommerce.ECommerce_project.point.domain.entity.Point;
import io.hhplus.ECommerce.ECommerce_project.point.domain.service.PointDomainService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.ProductFinderService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import io.hhplus.ECommerce.ECommerce_project.product.domain.entity.Product;
import io.hhplus.ECommerce.ECommerce_project.product.domain.service.ProductDomainService;
import io.hhplus.ECommerce.ECommerce_project.user.application.service.UserFinderService;
import io.hhplus.ECommerce.ECommerce_project.user.domain.entity.User;
import io.hhplus.ECommerce.ECommerce_project.user.domain.service.UserDomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaConsumer {

    private final OrderKafkaProducer orderKafkaProducer;
    private final ProductFinderService productFinderService;
    private final UserFinderService userFinderService;
    private final UserCouponFinderService userCouponFinderService;
    private final CouponFinderService couponFinderService;
    private final PointFinderService pointFinderService;
    private final OrderCompletionService orderCompletionService;
    private final RedisStockService redisStockService;

    // 검증 로직에 필요한 Service들
    private final UserDomainService userDomainService;
    private final ProductDomainService productDomainService;
    private final PointDomainService pointDomainService;

    /**
     * 1. 주문 검증 이벤트 처리
     */
    @Transactional
    @KafkaListener(
            topics = "order-validation-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeOrderValidation(OrderFromProductValidationRequestedEvent event, Acknowledgment acknowledgment) {
        log.info("주문 검증 이벤트 수신 - userId: {}, productId: {}",
                event.command().userId(), event.command().productId());

        try {
            // 검증 로직 (기존 OrderFromProductValidationEventListener의 로직)
            ValidatedOrderFromProductData validatedData = validateAndCalculate(event.command());

            log.info("주문 검증 성공");

            // 재고 차감 이벤트 발행
            orderKafkaProducer.sendStockDeductionRequested(
                    StockDeductionFromProductRequestedEvent.of(event.command(), validatedData)
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("주문 검증 실패: {}", e.getMessage(), e);

            // 검증 실패 이벤트 발행
            orderKafkaProducer.sendValidationFailed(
                    ValidationFromProductFailedEvent.of(
                            event.command().productId(),
                            event.command().quantity(),
                            e.getMessage()
                    )
            );

            acknowledgment.acknowledge();  // 검증 실패는 재처리 불필요
        }
    }

    /**
     * 2. 재고 차감 이벤트 처리
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "stock-deduction-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    @DistributedLock(
            key = "'product:stock:' + #event.command().productId()",
            waitTime = 3L,
            leaseTime = 5L  // 재고 차감 + 판매량 증가
    )
    public void consumeStockDeduction(StockDeductionFromProductRequestedEvent event, Acknowledgment acknowledgment) {
        log.info("재고 차감 이벤트 수신 - productId: {}", event.command().productId());

        try {
            Product product = productFinderService.getProduct(event.command().productId());
            product.decreaseStock(event.command().quantity());
            product.increaseSoldCount(event.command().quantity());

            log.info("DB 재고 차감 성공");

            // 주문 생성 이벤트 발행
            orderKafkaProducer.sendOrderCreationRequested(
                    OrderCreationFromProductRequestedEvent.of(event.command(), event.validatedOrderFromProductData())
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("재고 차감 실패: {}", e.getMessage(), e);

            // 재고 차감 실패 이벤트 발행
            orderKafkaProducer.sendStockDeductionFailed(
                    StockDeductionFromProductFailedEvent.of(
                            event.command().productId(),
                            event.command().quantity(),
                            e.getMessage()
                    )
            );

            acknowledgment.acknowledge();
        }
    }

    /**
     * 3. 검증 실패 처리 (Redis 재고 복구)
     */
    @KafkaListener(
            topics = "validation-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeValidationFailed(ValidationFromProductFailedEvent event, Acknowledgment acknowledgment) {
        log.warn("검증 실패 재고 복구 시작 - productId: {}", event.productId());

        try {
            redisStockService.increaseStock(event.productId(), event.quantity());
            log.info("Redis 재고 복구 완료");

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Redis 재고 복구 실패", e);
            throw e;  // 재시도 후 DLQ
        }
    }

    /**
     * 4. 재고 차감 실패 처리 (Redis 재고 복구)
     */
    @KafkaListener(
            topics = "stock-deduction-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeStockDeductionFailed(StockDeductionFromProductFailedEvent event, Acknowledgment acknowledgment) {
        log.warn("재고 차감 실패 Redis 복구 시작 - productId: {}", event.productId());

        try {
            redisStockService.increaseStock(event.productId(), event.quantity());
            log.info("Redis 재고 복구 완료");

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Redis 재고 복구 실패", e);
            throw e;  // 재시도 후 DLQ
        }
    }

    /**
     * 5. 주문 생성 이벤트 처리
     */
    @Transactional
    @KafkaListener(
            topics = "order-creation-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeOrderCreation(OrderCreationFromProductRequestedEvent event, Acknowledgment acknowledgment) {
        log.info("주문 생성 이벤트 수신 - userId: {}, productId: {}",
                event.command().userId(), event.command().productId());

        try {
            // 주문 생성 (DB 저장, 포인트 차감, 쿠폰 사용)
            CreateOrderResponse response = orderCompletionService.completeOrderFromProduct(
                    event.command(),
                    event.validatedOrderFromProductData()
            );

            log.info("주문 생성 완료");

            // 주문 완료 이벤트 발행
            orderKafkaProducer.sendOrderCompleted(
                    OrderFromProductCompletedEvent.of(response.userId(), response)
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("주문 생성 실패", e);
            // 주문 생성 실패 시 보상 트랜잭션 (DB 재고 복구, Redis 재고 복구)
            throw e;  // 재시도 후 DLQ
        }
    }

    /**
     * 6. 주문 완료 알림 이벤트 처리
     */
    @KafkaListener(
            topics = "order-completed",
            groupId = "order-service-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory" // 알림은 덜 중요
    )
    public void consumeOrderCompleted(OrderFromProductCompletedEvent event, Acknowledgment acknowledgment) {
        log.info("=== 주문 완료 알림 ===");
        log.info("사용자 ID: {}", event.userId());
        log.info("주문 ID: {}", event.orderResponse().orderId());
        // TODO: WebSocket/푸시 알림 전송

        acknowledgment.acknowledge();
    }

    /**
     * 7. 주문 생성 실패 처리 (DB 재고 복구)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "order-creation-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    @DistributedLock(
            key = "'product:stock:' + #event.reservations().get(0).productId()",
            waitTime = 3L,
            leaseTime = 5L
    )
    public void consumeOrderCreationFailed(OrderCreationFromProductFailedEvent event, Acknowledgment acknowledgment) {
        log.warn("주문 생성 실패 DB 재고 복구 시작 - productId: {}", event.reservations().get(0).productId());

        try {
            // DB 재고 복구 (차감했던 재고 원복)
            Product product = productFinderService.getProduct(event.reservations().get(0).productId());
            product.increaseStock(event.reservations().get(0).quantity());
            product.decreaseSoldCount(event.reservations().get(0).quantity());

            log.info("DB 재고 복구 완료");

            // Redis 재고 복구 이벤트 발행
            orderKafkaProducer.sendStockDeductionFailed(
                    StockDeductionFromProductFailedEvent.of(
                            event.reservations().get(0).productId(),
                            event.reservations().get(0).quantity(),
                            event.failureReason()
                    )
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("DB 재고 복구 실패", e);
            throw e;  // 재시도 후 DLQ
        }
    }

    /**
     * 주문 검증 및 가격 계산 로직
     */
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
