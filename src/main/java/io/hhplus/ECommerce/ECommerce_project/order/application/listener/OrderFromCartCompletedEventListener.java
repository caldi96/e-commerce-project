package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromCartCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 장바구니 주문 완료 이벤트 리스너
 * - 주문 생성 완료 후 사용자에게 알림 전송
 * - 현재는 로그만 출력, 추후 WebSocket/SSE/푸시 알림 추가 예정
 */
@Slf4j
//@Component // 스프링 이벤트를 카프카 이벤트로 처리
@RequiredArgsConstructor
public class OrderFromCartCompletedEventListener {
    // TODO: WebSocket/SSE 구현 시 추가
    // private final SimpMessagingTemplate messagingTemplate;
    // private final NotificationService notificationService;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderCompleted(OrderFromCartCompletedEvent event) {
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
}
