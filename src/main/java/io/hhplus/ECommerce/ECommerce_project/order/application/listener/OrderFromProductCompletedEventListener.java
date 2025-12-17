package io.hhplus.ECommerce.ECommerce_project.order.application.listener;

import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromProductCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 주문 완료 이벤트 리스너
 * - 주문 생성 완료 후 사용자에게 알림 전송
 * - 현재는 로그만 출력, 추후 WebSocket/SSE/푸시 알림 추가 예정
 */
@Slf4j
//@Component // 스프링 이벤트를 kafka 이벤트로 변경
@RequiredArgsConstructor
public class OrderFromProductCompletedEventListener {

    // TODO: WebSocket/SSE 구현 시 추가
    // private final SimpMessagingTemplate messagingTemplate;
    // private final NotificationService notificationService;

    @Async  // TODO: Kafka 도입 시 메시지 컨슈머로 변경 예정
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderCompleted(OrderFromProductCompletedEvent event) {
        log.info("=== 주문 완료 알림 ===");
        log.info("사용자 ID: {}", event.userId());
        log.info("주문 ID: {}", event.orderResponse().orderId());
        log.info("최종 금액: {}", event.orderResponse().finalAmount());
        log.info("주문 상태: {}", event.orderResponse().orderStatus());
        log.info("주문 메시지: {}", event.orderResponse().message());
        log.info("===================");

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