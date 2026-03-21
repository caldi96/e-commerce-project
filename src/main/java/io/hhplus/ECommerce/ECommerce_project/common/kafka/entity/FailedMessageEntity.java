package io.hhplus.ECommerce.ECommerce_project.common.kafka.entity;

import io.hhplus.ECommerce.ECommerce_project.common.entity.BaseEntity;
import io.hhplus.ECommerce.ECommerce_project.common.kafka.enums.FailedMessageStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "filed_messages")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FailedMessageEntity extends BaseEntity {

    @Column(name = "dlq_topic", nullable = false)
    private String dlqTopic;

    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
    private String payload;     // JSON 직렬화된 이벤트

    @Column(name = "event_type", nullable = false)
    private String eventType;   // "CouponIssuedEvent"

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private FailedMessageStatus status;

    @CreationTimestamp
    @Column(name = "failed_at", nullable = false, updatable = false)
    private LocalDateTime failedAt;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    public static FailedMessageEntity create(
            String dlqTopic, String messageKey,
            String payload, String eventType,
            String errorMessage, String stackTrace) {

        FailedMessageEntity entity = new FailedMessageEntity();
        entity.dlqTopic = dlqTopic;
        entity.originalTopic = dlqTopic.replace(".DLT", "");
        entity.messageKey = messageKey;
        entity.payload = payload;
        entity.eventType = eventType;
        entity.errorMessage = errorMessage;
        entity.stackTrace = stackTrace;
        entity.failedAt = LocalDateTime.now();
        return entity;
    }
}
