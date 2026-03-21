package io.hhplus.ECommerce.ECommerce_project.order.infrastructure.kafka;

import io.hhplus.ECommerce.ECommerce_project.common.compensation.application.CompensationFailureService;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureType;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.StockDeductionFromProductFailedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.ValidationFromProductFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * DLQ(Dead Letter Queue) 메시지 처리 Consumer
 * - 보상 트랜잭션이 3회 재시도 후에도 실패한 경우 처리
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CompensationFailureDLQConsumer {

    private final CompensationFailureService compensationFailureService;

    /**
     * 개별 상품 검증 실패 DLQ 처리
     */
    @KafkaListener(
            topics = "validation-failed.DLT",
            groupId = "compensation-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handleValidationFailedDLQ(
            ValidationFromProductFailedEvent event,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage
    ) {
        log.error("검증 실패 보상 DLQ 수신 - productId: {}, exception: {}",
                event.productId(), exceptionMessage);

        compensationFailureService.recordFailure(
                CompensationFailureType.REDIS_STOCK_RECOVERY_FAILED,
                event.productId(),
                null,
                event.quantity(),
                event,
                exceptionMessage
        );
    }

    /**
     * 개별 상품 재고 차감 실패 DLQ 처리
     */
    @KafkaListener(
            topics = "stock-deduction-failed.DLT",
            groupId = "compensation-dlq-group",
            containerFactory = "autoCommitKafkaListenerContainerFactory"
    )
    public void handleStockDeductionFailedDLQ(
            StockDeductionFromProductFailedEvent event,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage
    ) {
        log.error("재고 차감 실패 보상 DLQ 수신 - productId: {}, exception: {}",
                event.productId(), exceptionMessage);

        compensationFailureService.recordFailure(
                CompensationFailureType.REDIS_STOCK_RECOVERY_FAILED,
                event.productId(),
                null,
                event.quantity(),
                event,
                exceptionMessage
        );
    }
}
