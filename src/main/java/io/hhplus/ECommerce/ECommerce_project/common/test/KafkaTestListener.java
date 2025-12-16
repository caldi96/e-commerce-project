package io.hhplus.ECommerce.ECommerce_project.common.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 테스트용 리스너
 * - test-topic의 메시지를 수신하여 Kafka 동작 확인
 */
@Slf4j
@Component
public class KafkaTestListener {

    /**
     * test-topic 메시지 수신
     * - 자동 커밋 방식으로 처리
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "test-consumer-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void listenTestMessage(Object message) {
        log.info("🎯 테스트 메시지 수신됨: {}", message);

        try {
            // 메시지 처리 로직
            log.info("✅ 메시지 처리 완료: {}", message);
        } catch (Exception e) {
            log.error("❌ 메시지 처리 실패: {}", message, e);
            throw e; // 재시도를 위해 예외 던지기
        }
    }
}