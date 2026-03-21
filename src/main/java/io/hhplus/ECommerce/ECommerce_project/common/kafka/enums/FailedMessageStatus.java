package io.hhplus.ECommerce.ECommerce_project.common.kafka.enums;

public enum FailedMessageStatus {
    PENDING("재처리 대기"),
    REPROCESSING("재처리 중"),
    RESOLVED("정상 처리 완료"),
    IGNORED("의도적으로 무시");

    private final String description;

    FailedMessageStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
