package io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums;

/**
 * 보상 트랜잭션 실패 상태
 */
public enum CompensationFailureStatus {
    PENDING("대기중"),
    RETRYING("재시도 중"),
    RESOLVED("해결됨"),
    MANUAL_REQUIRED("수동 처리 필요");

    private final String description;

    CompensationFailureStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}