package io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.entity;

import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureStatus;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureType;
import io.hhplus.ECommerce.ECommerce_project.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "compensation_failures")
@EntityListeners(AuditingEntityListener.class)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CompensationFailure extends BaseEntity {
    @Enumerated(EnumType.STRING)
    @Column(name = "failure_type", nullable = false, length = 50)
    private CompensationFailureType failureType;

    @Column(name = "product_id")
    private Long productId;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "quantity")
    private Integer quantity;

    @Column(name = "original_event", columnDefinition = "TEXT")
    private String originalEvent;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private CompensationFailureStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @Column(name = "resolved_by")
    private String resolvedBy;

    @Column(name = "resolution_note", columnDefinition = "TEXT")
    private String resolutionNote;

    @Builder
    public CompensationFailure(
            CompensationFailureType failureType,
            Long productId,
            Long userId,
            Integer quantity,
            String originalEvent,
            String failureReason
    ) {
        this.failureType = failureType;
        this.productId = productId;
        this.userId = userId;
        this.quantity = quantity;
        this.originalEvent = originalEvent;
        this.failureReason = failureReason;
        this.retryCount = 0;
        this.status = CompensationFailureStatus.PENDING;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public void markAsResolved(String resolvedBy, String note) {
        this.status = CompensationFailureStatus.RESOLVED;
        this.resolvedAt = LocalDateTime.now();
        this.resolvedBy = resolvedBy;
        this.resolutionNote = note;
    }

    public void markAsManualRequired() {
        this.status = CompensationFailureStatus.MANUAL_REQUIRED;
    }

    public void markAsRetrying() {
        this.status = CompensationFailureStatus.RETRYING;
    }

    public boolean canRetry() {
        return this.status == CompensationFailureStatus.PENDING
                && this.retryCount < 3;
    }
}
