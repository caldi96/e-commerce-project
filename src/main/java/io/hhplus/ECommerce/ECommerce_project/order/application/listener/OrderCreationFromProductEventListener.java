package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.application.service.OrderCompletionService;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromProductCompletedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromProductFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromProductRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.presentation.response.CreateOrderResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 주문 생성 이벤트 리스너
 * - DB 재고 차감 완료 후 주문 생성 수행
 * - 주문 생성 실패 시 DB 재고 + Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트를 kafka 이벤트로 변경
@RequiredArgsConstructor
public class OrderCreationFromProductEventListener {

    private final OrderCompletionService orderCompletionService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleOrderCreation(OrderCreationFromProductRequestedEvent event) {
        log.info("주문 생성 이벤트 처리 시작 - userId: {}, productId: {}, quantity: {}",
                event.command().userId(), event.command().productId(), event.command().quantity());

        try {
            // 주문 생성 (DB 저장, 포인트 차감, 쿠폰 사용)
            CreateOrderResponse response = orderCompletionService.completeOrderFromProduct(
                    event.command(),
                    event.validatedOrderFromProductData()
            );

            log.info("주문 생성 완료 - userId: {}, orderId: {}, finalAmount: {}",
                    event.command().userId(), response.orderId(), response.finalAmount());

            // 주문 완료 이벤트 발행 (알림용)
            applicationEventPublisher.publishEvent(
                    OrderFromProductCompletedEvent.of(event.command().userId(), response)
            );

        } catch (Exception e) {
            log.error("주문 생성 실패 - userId: {}, productId: {}, reason: {}",
                    event.command().userId(), event.command().productId(), e.getMessage(), e);

            // 주문 생성 실패 -> DB 재고 + Redis 재고 복구
            applicationEventPublisher.publishEvent(
                    OrderCreationFromProductFailedEvent.of(
                            event.command().userId(),
                            event.command().productId(),
                            event.command().quantity(),
                            e.getMessage()
                    )
            );
        }
    }
}
