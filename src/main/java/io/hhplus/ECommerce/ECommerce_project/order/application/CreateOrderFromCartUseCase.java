package io.hhplus.ECommerce.ECommerce_project.order.application;

import io.hhplus.ECommerce.ECommerce_project.cart.application.service.CartFinderService;
import io.hhplus.ECommerce.ECommerce_project.cart.domain.entity.Cart;
import io.hhplus.ECommerce.ECommerce_project.order.application.command.CreateOrderFromCartCommand;
import io.hhplus.ECommerce.ECommerce_project.order.domain.event.OrderFromCartValidationRequestedEvent;
import io.hhplus.ECommerce.ECommerce_project.order.infrastructure.kafka.OrderKafkaProducer;
import io.hhplus.ECommerce.ECommerce_project.order.presentation.response.CreateOrderResponse;
import io.hhplus.ECommerce.ECommerce_project.product.application.service.RedisStockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 장바구니 주문 생성 UseCase
 * - 장바구니 조회 및 상품별 수량 집계
 * - Redis 재고 차감 (동기, All or Nothing)
 * - 이후 처리는 kafka 이벤트로 진행
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CreateOrderFromCartUseCase {

    private final RedisStockService redisStockService;
    private final CartFinderService cartFinderService;
    private final OrderKafkaProducer orderKafkaProducer;

    public CreateOrderResponse execute(CreateOrderFromCartCommand command) {

        List<Map.Entry<Long, Integer>> successEntries = new ArrayList<>();

        try {
            log.info("장바구니 주문 시작 - userId: {}, 장바구니 아이템수: {}",
                    command.userId(), command.cartItemIds().size());

            // 1. 장바구니 조회 및 상품별 수량 집계
            List<Cart> cartList = command.cartItemIds().stream()
                    .map(cartFinderService::getCart)
                    .toList();

            // 상품별 주문 수량 집계 (같은 상품이 여러 장바구니 항목에 있을 수 있음)
            Map<Long, Integer> productQuantityMap = new HashMap<>();
            for (Cart cart : cartList) {
                productQuantityMap.merge(
                        cart.getProduct().getId(),
                        cart.getQuantity(),
                        Integer::sum
                );
            }

            // 데드락 방지: productId 오름차순 정렬
            List<Map.Entry<Long, Integer>> sortedEntries = productQuantityMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .toList();

            log.info("상품별 수량 집계 완료 - 총 상품수: {}", sortedEntries.size());

            // 2. 모든 상품 Redis 재고 차감 (동기, All or Nothing)
            for (Map.Entry<Long, Integer> entry : sortedEntries) {
                Long productId = entry.getKey();
                Integer quantity = entry.getValue();

                redisStockService.decreaseStock(productId, quantity);
                successEntries.add(entry);

                log.debug("Redis 재고 차감 성공 - productId: {}, quantity: {}",
                        productId, quantity);
            }

            log.info("전체 상품 Redis 재고 차감 완료 - userId: {}, 성공수: {}",
                    command.userId(), successEntries.size());

            // 3. Kafka로 검증 이벤트 발행 (비동기)
            orderKafkaProducer.sendOrderFromCartValidationRequested(
                    OrderFromCartValidationRequestedEvent.of(command, sortedEntries)
            );

            // 4. 주문 접수 완료 응답 즉시 반환
            return CreateOrderResponse.acceptedForCart(
                    command.userId(),
                    cartList.size()
            );
        } catch (Exception e) {
            log.error("장바구니 주문 실패 - userId: {}, 실패 위치: {}, reason: {}",
                    command.userId(), successEntries.size(), e.getMessage(), e);

            // 실패 시 이미 차감된 Redis 재고 모두 복구
            rollbackRedisStock(successEntries);
            throw e;
        }
    }

    /**
     * Redis 재고 복구 (동기 처리)
     */
    private void rollbackRedisStock(List<Map.Entry<Long, Integer>> successEntries) {
        log.warn("Redis 재고 복구 시작 - 복구 대상 상품수: {}", successEntries.size());

        for (Map.Entry<Long, Integer> entry : successEntries) {
            try {
                redisStockService.increaseStock(entry.getKey(), entry.getValue());
                log.debug("Redis 재고 복구 성공 - productId: {}, quantity: {}",
                        entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("Redis 재고 복구 실패 - productId: {}, quantity: {}, error: {}",
                        entry.getKey(), entry.getValue(), e.getMessage(), e);

                // TODO: 재고 복구 실패 시 처리 로직
                // - Dead Letter Queue에 저장
                // - 관리자 알림 발송
                // - 재시도 스케줄링
            }
        }

        log.warn("Redis 재고 복구 완료 - 복구 상품수: {}", successEntries.size());
    }
}
