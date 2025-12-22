package io.hhplus.ECommerce.ECommerce_project.common.kafka;

import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssueFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssuedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponQuantityIncreaseEvent;
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
        log.error("[치명적] 결제 실패 보상 트랜잭션 최종 실패 - DLQ 도착");
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
        log.warn("Redis 랭킹 업데이트 최종 실패 - DLQ 도착");
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

    // ========== 쿠폰 관련 DLQ 핸들러 추가 ==========

    /**
     * 쿠폰 발급 DLQ
     * - 쿠폰 발급 실패는 치명적 (금전적 가치 있음)
     * - Redis는 성공했지만 DB 저장이 실패한 상태
     */
    @KafkaListener(
            topics = "coupon-issued.DLT",
            groupId = "coupon-issued-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handleCouponIssuedDlq(
            @Payload CouponIssuedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String stackTrace
    ) {
        log.error("[치명적] 쿠폰 발급 최종 실패 - DLQ 도착");
        log.error("DLQ Topic: {}", topic);
        log.error("Event - userId: {}, couponId: {}", event.userId(), event.couponId());
        log.error("Kafka Key: {}", key);
        log.error("Exception Message: {}", exceptionMessage);
        log.error("Stack Trace: {}", stackTrace);

        // TODO: 치명적 오류 처리
        // 1. DB에 실패 로그 저장 (중요!)
        //    - saveCouponIssueFailureLog(event);
        // 2. 관리자에게 긴급 알림 발송 (이메일, Slack, PagerDuty 등)
        //    - alertOpsTeam("쿠폰 발급 실패", event);
        // 3. Redis 롤백 이벤트 수동 발행 시도
        //    - couponKafkaProducer.sendCouponIssueFailed(...);
        // 4. 고객 보상 절차 시작
        //    - 수동으로 쿠폰 발급 또는 CS 처리
    }

    /**
     * 쿠폰 수량 증가 DLQ
     * - DB에 UserCoupon은 저장되었지만 집계 데이터(issuedQuantity) 증가 실패
     * - 데이터 정합성 문제 (실제 발급 수 != issuedQuantity)
     */
    @KafkaListener(
            topics = "coupon-quantity-increase.DLT",
            groupId = "coupon-quantity-increase-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handleCouponQuantityIncreaseDlq(
            @Payload CouponQuantityIncreaseEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String stackTrace
    ) {
        log.error("[중요] 쿠폰 수량 증가 최종 실패 - DLQ 도착");
        log.error("DLQ Topic: {}", topic);
        log.error("Event - couponId: {}", event.couponId());
        log.error("Kafka Key: {}", key);
        log.error("Exception Message: {}", exceptionMessage);
        log.error("Stack Trace: {}", stackTrace);

        // TODO: 집계 데이터 정합성 복구
        // 1. DB에 실패 로그 저장
        //    - saveCouponQuantityIncreaseFailureLog(event);
        // 2. 알림 발송 (정합성 문제 발생)
        //    - alertOpsTeam("쿠폰 집계 데이터 불일치", event);
        // 3. 수동 재시도 또는 배치 작업으로 정합성 복구
        //    - 실제 발급된 UserCoupon 개수와 issuedQuantity 비교 후 조정
        // 4. 모니터링 대시보드에 표시
    }

    /**
     * 쿠폰 발급 실패 (보상 트랜잭션) DLQ
     * - Redis 롤백까지 실패한 최악의 상황
     * - Redis에는 발급된 것으로 기록되었지만 DB에는 없는 불일치 상태
     */
    @KafkaListener(
            topics = "coupon-issue-failed.DLT",
            groupId = "coupon-issue-failed-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handleCouponIssueFailedDlq(
            @Payload CouponIssueFailedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String stackTrace
    ) {
        log.error("[최고 치명도] 쿠폰 Redis 롤백 최종 실패 - DLQ 도착");
        log.error("DLQ Topic: {}", topic);
        log.error("Event - userId: {}, couponId: {}, reason: {}",
                event.userId(), event.couponId(), event.failureReason());
        log.error("Kafka Key: {}", key);
        log.error("Exception Message: {}", exceptionMessage);
        log.error("Stack Trace: {}", stackTrace);
        log.error("Redis-DB 불일치 상태: Redis에는 발급됨, DB에는 없음");

        // TODO: 최우선 긴급 처리
        // 1. 즉시 DB에 실패 로그 저장 (최고 우선순위)
        //    - saveCriticalFailureLog(event, "REDIS_ROLLBACK_FAILED");
        // 2. 긴급 알림 발송 (PagerDuty, 전화 알림 등)
        //    - sendCriticalAlert("쿠폰 Redis 롤백 실패", event);
        // 3. 수동 개입 필요 플래그 설정
        //    - markForManualIntervention(event);
        // 4. 데이터 정합성 수동 복구
        //    - Redis에서 해당 userId의 쿠폰 발급 기록 수동 삭제
        //    - 또는 DB에 UserCoupon 강제 생성
        // 5. 고객 보상
        //    - 고객에게 쿠폰 재발급 또는 보상 제공
    }
}

