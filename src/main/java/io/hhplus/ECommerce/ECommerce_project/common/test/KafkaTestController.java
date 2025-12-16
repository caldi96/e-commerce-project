package io.hhplus.ECommerce.ECommerce_project.common.test;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka 테스트용 컨트롤러
 * - 테스트 메시지를 발행하여 Kafka 동작 확인
 */
@Slf4j
@RestController
@RequestMapping("/api/test/kafka")
@RequiredArgsConstructor
public class KafkaTestController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 테스트 메시지 발행
     * GET /api/test/kafka/send?message=안녕하세요
     */
    @GetMapping("/send")
    public Map<String, Object> sendTestMessage(@RequestParam(defaultValue = "Hello Kafka!") String message) {
        Map<String, Object> response = new HashMap<>();
        String topic = "test-topic";
        String key = "test-key-" + System.currentTimeMillis();

        try {
            // 테스트 메시지 객체 생성
            TestMessage testMessage = new TestMessage(message, System.currentTimeMillis());

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, key, testMessage);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("✅ 테스트 메시지 발행 성공: topic={}, partition={}, offset={}, message={}",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            message);
                } else {
                    log.error("❌ 테스트 메시지 발행 실패: message={}, error={}",
                            message, ex.getMessage(), ex);
                }
            });

            response.put("success", true);
            response.put("topic", topic);
            response.put("key", key);
            response.put("message", message);
            response.put("timestamp", testMessage.timestamp);

        } catch (Exception e) {
            log.error("테스트 메시지 발행 중 예외 발생", e);
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return response;
    }

    /**
     * Kafka 상태 확인
     * GET /api/test/kafka/health
     */
    @GetMapping("/health")
    public Map<String, Object> checkHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("kafka", "ready");
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }

    /**
     * 테스트 메시지 DTO
     */
    public record TestMessage(
            String content,
            long timestamp
    ) {}
}