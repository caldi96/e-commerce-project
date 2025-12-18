package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.application.service.OrderCompletionService;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromCartFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderCreationFromCartRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromCartCompletedEvent;
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
 * 장바구니 주문 생성 이벤트 리스너
 * - DB 재고 차감 완료 후 주문 생성 수행
 * - 주문 생성 실패 시 DB 재고 + Redis 재고 복구 이벤트 발행
 */
@Slf4j
//@Component // 스프링 이벤트를 카프카 이벤트로 변경
@RequiredArgsConstructor
public class OrderCreationFromCartEventListener {

    private final OrderCompletionService orderCompletionService;
    private final ApplicationEventPublisher applicationEventPublisher;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleOrderCreation(OrderCreationFromCartRequestedEvent event) {
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
            applicationEventPublisher.publishEvent(
                    OrderFromCartCompletedEvent.of(event.command().userId(), response)
            );

        } catch (Exception e) {
            log.error("장바구니 주문 생성 실패 - userId: {}, reason: {}",
                    event.command().userId(), e.getMessage(), e);

            // 주문 생성 실패 -> DB 재고 + Redis 재고 복구
            applicationEventPublisher.publishEvent(
                    OrderCreationFromCartFailedEvent.of(
                            event.command().userId(),
                            event.validatedOrderFromCartData().sortedEntries(),
                            e.getMessage(),
                            true  // DB 재고 복구 필요
                    )
            );
        }
    }
}
