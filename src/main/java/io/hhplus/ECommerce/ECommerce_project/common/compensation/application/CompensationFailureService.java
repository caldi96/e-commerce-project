package io.hhplus.ECommerce.ECommerce_project.common.compensation.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.entity.CompensationFailure;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureStatus;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureType;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.infrastructure.CompensationFailureRepository;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.StockDeductionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CompensationFailureService {
    private final CompensationFailureRepository compensationFailureRepository;
    private final RedisStockService redisStockService;
    private final StockDeductionService stockDeductionService;
    private final ObjectMapper objectMapper;

    /**
     * 보상 실패 기록 저장
     */
    @Transactional
    public CompensationFailure recordFailure(
            CompensationFailureType failureType,
            Long productId,
            Long userId,
            Integer quantity,
            Object originalEvent,
            String failureReason
    ) {
        try {
            String eventJson = objectMapper.writeValueAsString(originalEvent);

            CompensationFailure failure = CompensationFailure.builder()
                    .failureType(failureType)
                    .productId(productId)
                    .userId(userId)
                    .quantity(quantity)
                    .originalEvent(eventJson)
                    .failureReason(failureReason)
                    .build();

            return compensationFailureRepository.save(failure);

        } catch (Exception e) {
            log.error("보상 실패 기록 저장 중 오류 발생", e);
            throw new RuntimeException("보상 실패 기록 저장 실패", e);
        }
    }

    /**
     * 대기 중인 실패 목록 조회
     */
    @Transactional(readOnly = true)
    public List<CompensationFailure> findPendingFailures() {
        return compensationFailureRepository.findByStatusOrderByCreatedAtDesc(
                CompensationFailureStatus.PENDING
        );
    }

    /**
     * 수동 처리 필요한 실패 목록 조회
     */
    @Transactional(readOnly = true)
    public List<CompensationFailure> findManualRequiredFailures() {
        return compensationFailureRepository.findByStatusOrderByCreatedAtDesc(
                CompensationFailureStatus.MANUAL_REQUIRED
        );
    }

    /**
     * 수동 재시도
     */
    @Transactional
    public void manualRetry(Long failureId) {
        CompensationFailure failure = compensationFailureRepository.findById(failureId)
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 실패 기록"));

        if (!failure.canRetry()) {
            throw new IllegalStateException("재시도할 수 없는 상태입니다");
        }

        failure.markAsRetrying();
        failure.incrementRetryCount();
        compensationFailureRepository.save(failure);

        try {
            switch (failure.getFailureType()) {
                case REDIS_STOCK_RECOVERY_FAILED ->
                        redisStockService.increaseStock(failure.getProductId(), failure.getQuantity());
                case DB_STOCK_RECOVERY_FAILED ->
                        stockDeductionService.recoverStockWithDistributedLock(
                                failure.getProductId(),
                                failure.getQuantity()
                        );
                default ->
                        throw new IllegalArgumentException("지원하지 않는 실패 타입");
            }

            failure.markAsResolved("SYSTEM", "수동 재시도 성공");
            compensationFailureRepository.save(failure);

            log.info("보상 재시도 성공 - failureId: {}", failureId);

        } catch (Exception e) {
            log.error("보상 재시도 실패 - failureId: {}", failureId, e);

            if (failure.getRetryCount() >= 3) {
                failure.markAsManualRequired();
                compensationFailureRepository.save(failure);
            }

            throw e;
        }
    }

    /**
     * 수동 해결 처리
     */
    @Transactional
    public void markResolved(Long failureId, String resolvedBy, String note) {
        CompensationFailure failure = compensationFailureRepository.findById(failureId)
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 실패 기록"));

        failure.markAsResolved(resolvedBy, note);
        compensationFailureRepository.save(failure);

        log.info("보상 실패 수동 해결 - failureId: {}, resolvedBy: {}", failureId, resolvedBy);
    }
}
