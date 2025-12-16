package io.hhplus.ECommerce.ECommerce_project.common.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

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
     * 수동 커밋 Listener Container Factory
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
     * 자동 커밋 Listener Container Factory
     * - AckMode.BATCH: 배치 단위로 자동 커밋 (성능 최적화)
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> autoCommitKafkaListerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(autoCommitConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(3);
        return factory;
    }
}
