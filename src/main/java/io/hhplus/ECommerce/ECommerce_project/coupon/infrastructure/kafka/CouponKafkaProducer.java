package io.hhplus.ECommerce.ECommerce_project.coupon.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssueFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponIssuedEvent;
import io.hhplus.ECommerce.ECommerce_project.coupon.domain.event.CouponQuantityIncreaseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 쿠폰 이벤트를 Kafka로 발행하는 Producer
 * - 실패 시 자동으로 재시도 (ProducerConfig.RETRIES_CONFIG)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String COUPON_ISSUED_TOPIC = "coupon-issued";
    private static final String COUPON_QUANTITY_INCREASE_TOPIC = "coupon-quantity-increase";
    private static final String COUPON_ISSUE_FAILED_TOPIC = "coupon-issue-failed";

    /**
     * 쿠폰 발급 이벤트 발행 (비동기)
     */
    public void sendCouponIssued(CouponIssuedEvent event) {
        String key = String.valueOf(event.couponId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(COUPON_ISSUED_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("쿠폰 발급 이벤트 발행 성공: topic={}, partition={}, offset={}, userId={}, couponId={}",
                        COUPON_ISSUED_TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.userId(),
                        event.couponId());
            } else {
                log.error("쿠폰 발급 이벤트 발행 실패: userId={}, couponId={}, error={}",
                        event.userId(), event.couponId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * 쿠폰 수량 증가 이벤트 발행
     */
    public void sendCouponQuantityIncrease(CouponQuantityIncreaseEvent event) {
        String key = String.valueOf(event.couponId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(COUPON_QUANTITY_INCREASE_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("쿠폰 수량 증가 이벤트 발행 성공: topic={}, partition={}, offset={}, couponId={}",
                        COUPON_QUANTITY_INCREASE_TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.couponId());
            } else {
                log.error("쿠폰 수량 증가 이벤트 발행 실패: couponId={}, error={}",
                        event.couponId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * 쿠폰 발급 실패 이벤트 발행 (보상 트랜잭션)
     */
    public void sendCouponIssueFailed(CouponIssueFailedEvent event) {
        String key = String.valueOf(event.couponId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(COUPON_ISSUE_FAILED_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("쿠폰 발급 실패 이벤트 발행 성공: topic={}, partition={}, offset={}, userId={}, couponId={}",
                        COUPON_ISSUE_FAILED_TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.userId(),
                        event.couponId());
            } else {
                log.error("쿠폰 발급 실패 이벤트 발행 실패: userId={}, couponId={}, error={}",
                        event.userId(), event.couponId(), ex.getMessage(), ex);
            }
        });
    }
}
