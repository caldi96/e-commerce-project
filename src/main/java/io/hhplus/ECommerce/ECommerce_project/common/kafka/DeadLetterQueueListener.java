package io.hhplus.ECommerce.ECommerce_project.common.kafka;

import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentCompletedEvent;
import io.hhplus.ECommerce.ECommerce_project.payment.domain.event.PaymentFailedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * DLQ(Dead Letter Queue) 리스너
 * - 재시도 실패한 메시지를 수신하여 로깅 및 알림 발송
 */
@Slf4j
@Component
public class DeadLetterQueueListener {

    /**
     * 결제 실패 보상 DLQ
     */
    @KafkaListener(
            topics = "payment-failed.DLT",
            groupId = "payment-failed-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handlePaymentFailedDlq(
            @Payload PaymentFailedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String stackTrace
    ) {
        log.error("⚠️⚠️⚠️ [치명적] 결제 실패 보상 트랜잭션 최종 실패 - DLQ 도착 ⚠️⚠️⚠️");
        log.error("DLQ Topic: {}", topic);
        log.error("Event - orderId: {}, userId: {}, reason: {}",
                event.orderId(), event.userId(), event.failureReason());
        log.error("Kafka Key: {}", key);
        log.error("Exception Message: {}", exceptionMessage);
        log.error("Stack Trace: {}", stackTrace);

        // TODO: 실패 처리
        // 1. DB에 실패 로그 저장
        // 2. 관리자에게 알림 발송 (이메일, Slack 등)
        // 3. 모니터링 시스템에 알림
    }

    /**
     * 결제 완료 랭킹 업데이트 DLQ
     * - Redis 랭킹 업데이트 실패는 비즈니스에 치명적이지 않음
     * - 로그만 남기고 나중에 배치로 재계산 가능
     */
    @KafkaListener(
            topics = "payment-completed.DLT",
            groupId = "payment-completed-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handlePaymentCompletedDlq(
            @Payload PaymentCompletedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        log.warn("⚠️ Redis 랭킹 업데이트 최종 실패 - DLQ 도착");
        log.warn("DLQ Topic: {}", topic);
        log.warn("Event - orderId: {}, itemCount: {}",
                event.orderId(), event.orderItems().size());
        log.warn("Kafka Key: {}", key);
        log.warn("Exception Message: {}", exceptionMessage);

        /// TODO: 비치명적 오류 처리
        // 1. 실패 로그 저장 (모니터링용)
        //    - saveRankingUpdateFailureLog(event);

        // 2. 일반 알림 발송 (선택적)
        //    - sendNotification("Redis 랭킹 업데이트 실패");

        // 3. 배치 작업으로 나중에 재계산
        //    - 매일 밤 전체 랭킹 재계산 배치에서 처리

        // 랭킹 업데이트 실패는 비즈니스에 영향 없으므로 낮은 우선순위로 처리
    }
}

