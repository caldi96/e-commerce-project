package io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums;

/**
 * 보상 트랜잭션 실패 타입
 */
public enum CompensationFailureType {
    REDIS_STOCK_RECOVERY_FAILED("Redis 재고 복구 실패"),
    DB_STOCK_RECOVERY_FAILED("DB 재고 복구 실패"),
    POINT_RECOVERY_FAILED("포인트 복구 실패"),
    COUPON_RECOVERY_FAILED("쿠폰 복구 실패");

    private final String description;

    CompensationFailureType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}