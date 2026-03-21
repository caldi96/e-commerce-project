package io.hhplus.ECommerce.ECommerce_project.order.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.cart.application.service.CartFinderService;
import io.hhplus.ECommerce.ECommerce_project.cart.domain.entity.Cart;
import io.hhplus.ECommerce.ECommerce_project.cart.domain.service.CartDomainService;
import io.hhplus.ECommerce.ECommerce_project.common.annotation.DistributedLock;
import io.hhplus.ECommerce.ECommerce_project.common.exception.CouponException;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ErrorCode;
import io.hhplus.ECommerce.ECommerce_project.common.exception.ProductException;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.UserCouponFinderService;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.Coupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.entity.UserCoupon;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.service.CouponDomainService;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromCartCommand;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromProductCommand;
import io.hhplus.ECommerce.ECommerce_project.order.application.dto.ValidatedOrderFromCartData;
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
import io.hhplus.ECommerce.ECommerce_project.product.application.service.StockDeductionService;
import io.hhplus.ECommerce.ECommerce_project.product.domain.entity.Product;
import io.hhplus.ECommerce.ECommerce_project.product.domain.service.ProductDomainService;
import io.hhplus.ECommerce.ECommerce_project.user.application.service.UserFinderService;
import io.hhplus.ECommerce.ECommerce_project.user.domain.entity.User;
import io.hhplus.ECommerce.ECommerce_project.user.domain.service.UserDomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private final CartFinderService cartFinderService;
    private final RedisStockService redisStockService;

    // 검증 로직에 필요한 Service들
    private final UserDomainService userDomainService;
    private final ProductDomainService productDomainService;
    private final PointDomainService pointDomainService;
    private final CartDomainService cartDomainService;
    private final CouponDomainService couponDomainService;

    // 상품 재고 감소에 쓰이는 Service
    private final StockDeductionService stockDeductionService;

    // 멱등성 보장을 위한 RedisTemplate
    private final RedisTemplate<String, String> redisTemplate;

    // ------------ 개별 상품 주문 -------------
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
            ValidatedOrderFromProductData validatedData = validateAndCalculateForProduct(event.command());

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
            // 멱등성 키로 중복 처리 방지
            String idempotencyKey = String.format(
                    "validation-failed-recovery:%d:%d:%s",
                    event.productId(),
                    event.quantity(),
                    event.failureReason().hashCode()
            );

            Boolean isFirstAttempt = redisTemplate.opsForValue()
                    .setIfAbsent(idempotencyKey, "1", 10, TimeUnit.MINUTES);

            if (Boolean.TRUE.equals(isFirstAttempt)) {
                redisStockService.increaseStock(event.productId(), event.quantity());
                log.info("Redis 재고 복구 완료");
            } else {
                log.warn("이미 처리된 재고 복구 요청 - productId: {}", event.productId());
            }

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
            // 멱등성 키로 중복 처리 방지
            String idempotencyKey = String.format(
                    "stock-deduction-failed-recovery:%d:%d:%s",
                    event.productId(),
                    event.quantity(),
                    event.failureReason().hashCode()
            );

            Boolean isFirstAttempt = redisTemplate.opsForValue()
                    .setIfAbsent(idempotencyKey, "1", 10, TimeUnit.MINUTES);

            if (Boolean.TRUE.equals(isFirstAttempt)) {
                redisStockService.increaseStock(event.productId(), event.quantity());
                log.info("Redis 재고 복구 완료");
            } else {
                log.warn("이미 처리된 재고 복구 요청 - productId: {}", event.productId());
            }

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

    // ------------ 장바구니 상품 주문 -------------
    /**
     * 8. 장바구니 주문 검증 이벤트 처리
     */
    @Transactional
    @KafkaListener(
            topics = "order-from-cart-validation-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeOrderFromCartValidation(OrderFromCartValidationRequestedEvent event, Acknowledgment acknowledgment) {
        CreateOrderFromCartCommand command = event.command();
        List<Map.Entry<Long, Integer>> sortedEntries = event.sortedEntries();

        log.info("장바구니 주문 검증 이벤트 처리 시작 - userId: {}, 장바구니 아이템수: {}, 상품수: {}",
                command.userId(), command.cartItemIds().size(), sortedEntries.size());

        try {
            // 검증 로직 (OrderFromCartValidationEventListener의 로직)
            ValidatedOrderFromCartData validatedOrderFromCartData =
                    validateAndCalculateForCart(command, sortedEntries);

            log.info("장바구니 주문 검증 성공 - userId: {}, 총 상품수: {}, 최종금액: {}",
                    command.userId(), sortedEntries.size(), validatedOrderFromCartData.finalAmount());

            // 재고 차감 이벤트 발행
            orderKafkaProducer.sendStockDeductionFromCartRequested(
                    StockDeductionFromCartRequestedEvent.of(command, validatedOrderFromCartData)
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("장바구니 주문 검증 실패 - userId: {}, reason: {}",
                    command.userId(), e.getMessage(), e);

            // 검증 실패 이벤트 발행
            orderKafkaProducer.sendValidationFromCartFailed(
                    ValidationFromCartFailedEvent.of(
                            command.userId(),
                            sortedEntries,
                            e.getMessage()
                    )
            );

            acknowledgment.acknowledge(); // 검증 실패는 재처리 불필요
        }
    }

    /**
     * 9. 장바구니 재고 차감 이벤트 처리 (복수 상품)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "stock-deduction-from-cart-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeStockDeductionFromCart(StockDeductionFromCartRequestedEvent event, Acknowledgment acknowledgment) {
        log.info("장바구니 DB 재고 차감 이벤트 처리 시작 - userId: {}, 상품수: {}",
                event.command().userId(), event.validatedOrderFromCartData().sortedEntries().size());

        List<Map.Entry<Long, Integer>> successEntries = new ArrayList<>();

        try {
            // 모든 상품 재고 차감 (All or Nothing)
            for (Map.Entry<Long, Integer> entry : event.validatedOrderFromCartData().sortedEntries()) {
                Long productId = entry.getKey();
                Integer quantity = entry.getValue();

                // 외부 서비스 호출 → AOP 프록시 작동
                stockDeductionService.deductStockWithDistributedLock(productId, quantity);
                successEntries.add(entry);

                log.debug("DB 재고 차감 성공 - productId: {}, quantity: {}", productId, quantity);
            }

            log.info("전체 상품 DB 재고 차감 완료 - userId: {}, 성공수: {}",
                    event.command().userId(), successEntries.size());

            // DB 재고 차감 성공 -> 주문 생성 이벤트 발행
            orderKafkaProducer.sendOrderCreationFromCartRequested(
                    OrderCreationFromCartRequestedEvent.of(
                            event.command(),
                            event.validatedOrderFromCartData()
                    )
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("장바구니 DB 재고 차감 실패 - userId: {}, 실패 위치: {}/{}, reason: {}",
                    event.command().userId(), successEntries.size(),
                    event.validatedOrderFromCartData().sortedEntries().size(), e.getMessage(), e);

            // DB 재고 차감 실패 시 성공한 항목들 복구
            rollbackDbStock(successEntries);

            // Redis 재고 복구 이벤트 발행
            orderKafkaProducer.sendStockDeductionFromCartFailed(
                    StockDeductionFromCartFailedEvent.of(
                            event.command().userId(),
                            event.validatedOrderFromCartData().sortedEntries(),
                            e.getMessage()
                    )
            );

            acknowledgment.acknowledge();
        }
    }

    /**
     * 10. 장바구니 검증 실패 처리 (Redis 재고 복구 - 복수 상품)
     */
    @KafkaListener(
            topics = "validation-from-cart-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeValidationFromCartFailed(ValidationFromCartFailedEvent event, Acknowledgment acknowledgment) {
        log.info("장바구니 검증 실패 재고 복구 시작 - userId: {}, 상품수: {}, reason: {}",
                event.userId(), event.sortedEntries().size(), event.failureReason());

        int successCount = 0;
        int failCount = 0;

        // Redis 재고 복구 (모든 상품)
        for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
            try {
                redisStockService.increaseStock(entry.getKey(), entry.getValue());
                successCount++;

                log.debug("Redis 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());

            } catch (Exception e) {
                failCount++;
                log.error("Redis 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                throw e;  // 재시도 후 DLQ
            }
        }

        log.info("장바구니 검증 실패 재고 복구 완료 - userId: {}, 성공: {}, 실패: {}",
                event.userId(), successCount, failCount);

        acknowledgment.acknowledge();
    }

    /**
     * 11. 장바구니 재고 차감 실패 처리 (Redis 재고 복구 - 복수 상품)
     */
    @KafkaListener(
            topics = "stock-deduction-from-cart-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeStockDeductionFromCartFailed(StockDeductionFromCartFailedEvent event, Acknowledgment acknowledgment) {
        log.info("장바구니 DB 재고 차감 실패 재고 복구 시작 - userId: {}, 상품수: {}, reason: {}",
                event.userId(), event.sortedEntries().size(), event.failureReason());

        int successCount = 0;
        int failCount = 0;

        // Redis 재고 복구 (모든 상품)
        for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
            try {
                redisStockService.increaseStock(entry.getKey(), entry.getValue());
                successCount++;

                log.debug("Redis 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());

            } catch (Exception e) {
                failCount++;
                log.error("Redis 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                throw e; // 재시도 3회 실패 시 DLQ에 저장
            }
        }

        log.info("장바구니 DB 재고 차감 실패 재고 복구 완료 - userId: {}, 성공: {}, 실패: {}",
                event.userId(), successCount, failCount);

        acknowledgment.acknowledge();
    }

    /**
     * 12. 장바구니 주문 생성 이벤트 처리
     */
    @Transactional
    @KafkaListener(
            topics = "order-creation-from-cart-requested",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeOrderCreationFromCart(OrderCreationFromCartRequestedEvent event, Acknowledgment acknowledgment) {
        log.info("장바구니 주문 생성 이벤트 처리 시작 - userId: {}, 상품수: {}",
                event.command().userId(), event.validatedOrderFromCartData().sortedEntries().size());

        try {
            // 주문 생성 (DB 저장, 포인트 차감, 쿠폰 사용)
            CreateOrderResponse response = orderCompletionService.completeOrderFromCart(
                    event.command(),
                    event.validatedOrderFromCartData()
            );

            log.info("장바구니 주문 생성 완료 - userId: {}, orderId: {}, finalAmount: {}",
                    event.command().userId(), response.orderId(), response.finalAmount());

            // 주문 완료 이벤트 발행 (알림용)
            orderKafkaProducer.sendOrderFromCartCompleted(
                    OrderFromCartCompletedEvent.of(event.command().userId(), response)
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("장바구니 주문 생성 실패 - userId: {}, reason: {}",
                    event.command().userId(), e.getMessage(), e);

            // 주문 생성 실패 -> DB 재고 + Redis 재고 복구
            orderKafkaProducer.sendOrderCreationFromCartFailed(
                    OrderCreationFromCartFailedEvent.of(
                            event.command().userId(),
                            event.validatedOrderFromCartData().sortedEntries(),
                            e.getMessage(),
                            true  // DB 재고 복구 필요
                    )
            );

            acknowledgment.acknowledge();

        }
    }

    /**
     * 13. 장바구니 주문 완료 알림 이벤트 처리
     */
    @KafkaListener(
            topics = "order-from-cart-completed",
            groupId = "order-service-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory" // 알림은 덜 중요하므로 자동 커밋
    )
    public void consumeOrderFromCartCompleted(OrderFromCartCompletedEvent event, Acknowledgment acknowledgment) {
        log.info("=== 장바구니 주문 완료 알림 ===");
        log.info("사용자 ID: {}", event.userId());
        log.info("주문 ID: {}", event.createOrderResponse().orderId());
        log.info("총 상품 금액: {}", event.createOrderResponse().totalAmount());
        log.info("배송비: {}", event.createOrderResponse().shippingFee());
        log.info("할인 금액: {}", event.createOrderResponse().discountAmount());
        log.info("포인트 사용: {}", event.createOrderResponse().pointAmount());
        log.info("최종 결제 금액: {}", event.createOrderResponse().finalAmount());
        log.info("주문 상태: {}", event.createOrderResponse().orderStatus());
        log.info("주문 메시지: {}", event.createOrderResponse().message());
        log.info("주문 아이템 수: {}", event.createOrderResponse().orderItems().size());
        log.info("==========================");

        // TODO: WebSocket으로 실시간 알림 전송
        // messagingTemplate.convertAndSendToUser(
        //         event.userId().toString(),
        //         "/queue/orders",
        //         event.orderResponse()
        // );

        // TODO: 푸시 알림 전송
        // notificationService.sendOrderCompletedNotification(
        //         event.userId(),
        //         event.orderResponse()
        // );

        // TODO: 이메일 알림 전송
        // emailService.sendOrderConfirmationEmail(
        //         event.userId(),
        //         event.orderResponse()
        // );
    }

    /**
     * 14. 장바구니 주문 생성 실패 처리 (DB 재고 복구 - 복수 상품)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @KafkaListener(
            topics = "order-creation-from-cart-failed",
            groupId = "order-service-group",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeOrderCreationFromCartFailed(OrderCreationFromCartFailedEvent event, Acknowledgment acknowledgment) {
        log.info("장바구니 주문 생성 실패 재고 복구 시작 - userId: {}, 상품수: {}, reason: {}",
                event.userId(), event.sortedEntries().size(), event.failureReason());

        int dbSuccessCount = 0;
        int dbFailCount = 0;
        int redisSuccessCount = 0;
        int redisFailCount = 0;

        try {
            // 1. DB 재고 복구 (필요한 경우)
            if (event.needsDbStockRecovery()) {
                for (Map.Entry<Long, Integer> entry : event.sortedEntries()) {
                    try {
                        // 외부 서비스 호출 → AOP 프록시 작동
                        stockDeductionService.recoverStockWithDistributedLock(entry.getKey(), entry.getValue());
                        dbSuccessCount++;

                        log.debug("DB 재고 복구 성공 - productId: {}, quantity: {}",
                                entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        dbFailCount++;
                        log.error("DB 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                                entry.getKey(), entry.getValue(), e.getMessage(), e);
                    }
                }

                log.info("DB 재고 복구 완료 - 성공: {}, 실패: {}", dbSuccessCount, dbFailCount);
            }

            // 레디스 재고 복구 kafka 이벤트 발행
            orderKafkaProducer.sendStockDeductionFromCartFailed(
                    StockDeductionFromCartFailedEvent.of(
                            event.userId(),
                            event.sortedEntries(),
                            event.failureReason()
                    )
            );

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("장바구니 주문 생성 실패 재고 복구 중 오류 - userId: {}, error: {}",
                    event.userId(), e.getMessage(), e);

            throw e; // 재시도 후 DLQ
        }
    }

    /**
     * 개별 상품 주문 검증 및 가격 계산 로직
     */
    private ValidatedOrderFromProductData validateAndCalculateForProduct(CreateOrderFromProductCommand command) {

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

    /**
     * 장바구니 주문 검증 및 가격 계산 로직
     */
    private ValidatedOrderFromCartData validateAndCalculateForCart(
            CreateOrderFromCartCommand command,
            List<Map.Entry<Long, Integer>> sortedEntries
    ) {

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

    /**
     * DB 재고 복구 (성공한 항목만)
     */
    private void rollbackDbStock(List<Map.Entry<Long, Integer>> successEntries) {
        log.warn("DB 재고 복구 시작 - 복구 대상 상품수: {}", successEntries.size());

        for (Map.Entry<Long, Integer> entry : successEntries) {
            try {
                // 외부 서비스 호출 → AOP 프록시 작동
                stockDeductionService.recoverStockWithDistributedLock(entry.getKey(), entry.getValue());
                log.debug("DB 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("DB 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                // TODO: 재고 복구 실패 시 처리 로직
                // - Dead Letter Queue에 저장
                // - 관리자 알림 발송
                // - 재시도 스케줄링
            }
        }

        log.warn("DB 재고 복구 완료 - 복구 상품수: {}", successEntries.size());
    }
}
