package io.hhplus.ECommerce.ECommerce_project.payment.application.listener;

import io.hhplus.ECommerce.ECommerce_project.coupon.application.service.CouponCompensationService;
import io.hhplus.ECommerce.ECommerce_project.order.application.service.OrderFinderService;
import io.hhplus.ECommerce.ECommerce_project.order.application.service.OrderItemFinderService;
import io.hhplus.ECommerce.ECommerce_project.order.domain.entity.OrderItem;
import io.hhplus.ECommerce.ECommerce_project.order.domain.entity.Orders;
import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.point.application.service.PointCompensationService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;

/**
 * 결제 보상 Kafka Consumer
 * - Kafka에서 결제 실패 이벤트를 수신하여 보상 트랜잭션 처리
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCompensationEventListener {

    private final OrderFinderService orderFinderService;
    private final OrderItemFinderService orderItemFinderService;
    private final StockService stockService;
    private final CouponCompensationService couponCompensationService;
    private final PointCompensationService pointCompensationService;

    /**
     * Kafka에서 결제 실패 이벤트 수신
     * - 재고, 쿠폰, 포인트를 순차적으로 복구
     */
    @KafkaListener(
            topics = "payment-failed",
            groupId = "payment-compensation-group",
            containerFactory = "autoCommitKafkaListerContainerFactory" // 수동 커밋
    )
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handlePaymentFailed(PaymentFailedEvent event, Acknowledgment ack) {
        log.info("Kafka 결제 실패 이벤트 수신: orderId={}, userId={}, reason={}",
                event.orderId(), event.userId(), event.failureReason());

        try {
            // 주문 조회
            Orders order = orderFinderService.getOrder(event.orderId());

            // 1. 재고 복구
            compensateStock(order);

            // 2. 쿠폰 복구
            compensateCoupon(order);

            // 3. 포인트 복구
            compensatePoint(order);

            log.info("결제 실패 보상 트랜잭션 완료: orderId={}", event.orderId());

            // 성공 시 수동 커밋
            ack.acknowledge();

        } catch (Exception e) {
            log.error("결제 실패 보상 트랜잭션 실패: orderId={}, error={}",
                    event.orderId(), e.getMessage(), e);

            // 수동 커밋하지 않음 -> 재처리됨
            // TODO: 재시도 횟수 제한 후 DLQ로 이동
            throw e;
        }
    }

    /**
     * 재고 복구
     */
    private void compensateStock(Orders order) {
        try {
            log.debug("재고 복구 시작: orderId={}", order.getId());

            // 1. 주문 아이템 조회
            List<OrderItem> orderItems = orderItemFinderService.getOrderItems(order.getId());

            // 2. 상품 재고 복구 (동시성 제어 적용)
            for (OrderItem orderItem : orderItems) {
                stockService.compensateStock(
                        orderItem.getProduct().getId(),
                        orderItem.getQuantity()
                );
            }

            log.info("재고 복구 완료: orderId={}, itemCount={}", order.getId(), orderItems.size());

        } catch (Exception e) {
            log.error("재고 복구 실패: orderId={}, error={}", order.getId(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 쿠폰 복구
     */
    private void compensateCoupon(Orders order) {
        if (order.getCoupon() == null || order.getCoupon().getId() == null) {
            log.debug("쿠폰 사용 없음: orderId={}", order.getId());
            return;
        }

        try {
            log.debug("쿠폰 복구 시작: orderId={}, couponId={}",
                    order.getId(), order.getCoupon().getId());

            couponCompensationService.compensate(
                    order.getUser().getId(),
                    order.getCoupon().getId(),
                    order.getCoupon().getPerUserLimit()
            );

            log.info("쿠폰 복구 완료: orderId={}, couponId={}",
                    order.getId(), order.getCoupon().getId());

        } catch (Exception e) {
            log.error("쿠폰 복구 실패: orderId={}, couponId={}, error={}",
                    order.getId(), order.getCoupon().getId(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 포인트 복구
     */
    private void compensatePoint(Orders order) {
        try {
            log.debug("포인트 복구 시작: orderId={}, userId={}",
                    order.getId(), order.getUser().getId());

            pointCompensationService.compensate(
                    order.getId(),
                    order.getUser().getId()
            );

            log.info("포인트 복구 완료: orderId={}, userId={}",
                    order.getId(), order.getUser().getId());

        } catch (Exception e) {
            log.error("포인트 복구 실패: orderId={}, userId={}, error={}",
                    order.getId(), order.getUser().getId(), e.getMessage(), e);
            throw e;
        }
    }
}
