package io.hhplus.ECommerce.ECommerce_project.common.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer 설정
 * - 수동 커밋: 중요한 트랜잭션 (결제 실패 보상)
 * - 자동 커밋: 덜 중요한 작업 (Redis 랭킹 업데이트)
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaConsumerConfig(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 공통 Consumer 설정
     */
    private Map<String, Object> baseConsumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return config;
    }

    /**
     * 수동 커밋 Consumer Factory (중요한 트랜잭션)
     * - 결제 실패 보상 처리용
     * - 메시지 처리 완료 후 명시적으로 커밋
     * - enable.auto.commit = false
     */
    @Bean
    public ConsumerFactory<String, Object> manualCommitConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * 수동 커밋 Listener Container Factory (DLQ 포함)
     * - 3회 재시도 후 DLQ로 이동
     * - AckMode.MANUAL: 리스너에서 Acknowledgment.acknowledge() 호출 시 커밋
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    manualCommitKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(manualCommitConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);

        // DLQ 설정 : 3회 재시도 후 실패하면 DLQ로 전송
        factory.setCommonErrorHandler(createErrorHandler(3, 2000L));

        return factory;
    }

    /**
     * 자동 커밋 Consumer Factory (덜 중요한 작업)
     * - Redis 랭킹 업데이트용
     * - 메시지 처리 후 자동으로 커밋
     * - enable.auto.commit = true
     * - auto.commit.interval.ms = 1000 (1초마다 자동 커밋)
     */
    @Bean
    public ConsumerFactory<String, Object> autoCommitConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * 자동 커밋 Listener Container Factory (DLQ 포함)
     * - 3회 재시도 후 DLQ 이동
     * - AckMode.BATCH: 배치 단위로 자동 커밋 (성능 최적화)
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> autoCommitKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(autoCommitConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(3);

        // DLQ 설정: 3회 재시도 후 실패하면 DLQ로 전송
        factory.setCommonErrorHandler(createErrorHandler(3, 1000L));

        return factory;
    }

    /**
     * DLQ 에러 핸들러 생성
     * @param maxAttempts 최대 재시도 횟수
     * @param backOffInterval 재시도 간격 (ms)
     */
    private DefaultErrorHandler createErrorHandler(int maxAttempts, long backOffInterval) {
        // DLQ 발행: 원본 토픽명 + ".DLT" (Dead Letter Topic)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    // DLQ 토픽명: 원본토픽.DLT
                    String dlqTopic = record.topic() + ".DLT";
                    return new TopicPartition(dlqTopic, record.partition());
                }
        );

        // 재시도 정책: maxAttempts회 재시도, backOffInterval 간격
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(backOffInterval, maxAttempts - 1)  // maxAttempts: 첫 시도 제외
        );

        // 재시도하지 않을 예외 타입 (즉시 DLQ로 전송)
        // errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        return defaultErrorHandler;
    }
}
