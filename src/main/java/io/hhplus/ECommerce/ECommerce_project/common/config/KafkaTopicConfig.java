package io.hhplus.ECommerce.ECommerce_project.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 토픽 설정
 * 애플리케이션 시작 시 정의된 토픽들을 자동으로 생성합니다.
 */
@Configuration
public class KafkaTopicConfig {

    private static final int DEFAULT_PARTITIONS = 3;
    private static final int DEFAULT_REPLICAS = 3;
    private static final String MIN_IN_SYNC_REPLICAS = "2";

    /**
     * 주문 생성 이벤트 토픽
     * 주문이 생성되면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name("order-created")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 결제 완료 이벤트 토픽
     * 결제가 완료되면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic paymentCompletedTopic() {
        return TopicBuilder.name("payment-completed")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 결제 실패 이벤트 토픽
     * 결제가 실패하면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name("payment-failed")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    // ========== DLQ 토픽들 ==========

    /**
     * 결제 완료 이벤트 DLQ
     */
    @Bean
    public NewTopic paymentCompletedDlqTopic() {
        return TopicBuilder.name("payment-completed.DLT")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 결제 실패 이벤트 DLQ
     */
    @Bean
    public NewTopic paymentFailedDlqTopic() {
        return TopicBuilder.name("payment-failed.DLT")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 재고 증가 이벤트 토픽
     * 재고가 증가하면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic stockIncreasedTopic() {
        return TopicBuilder.name("stock-increased")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 재고 감소 이벤트 토픽
     * 재고가 감소하면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic stockDecreasedTopic() {
        return TopicBuilder.name("stock-decreased")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 재고 감소 이벤트 DLT
     */
    @Bean
    public NewTopic stockDecreasedDltTopic() {
        return TopicBuilder.name("stock-decreased.DLT")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    // stock-increased DLT
    @Bean
    public NewTopic stockIncreasedDltTopic() {
        return TopicBuilder.name("stock-increased.DLT")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 쿠폰 발급 이벤트 토픽
     * 쿠폰이 발급되면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic couponIssuedTopic() {
        return TopicBuilder.name("coupon-issued")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }

    /**
     * 포인트 적립/차감 이벤트 토픽
     * 포인트가 변동되면 이 토픽으로 이벤트가 발행됩니다.
     */
    @Bean
    public NewTopic pointChangedTopic() {
        return TopicBuilder.name("point-changed")
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .config("min.insync.replicas", MIN_IN_SYNC_REPLICAS)
                .build();
    }
}