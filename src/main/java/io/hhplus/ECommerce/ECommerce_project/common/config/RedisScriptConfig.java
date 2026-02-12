package io.hhplus.ECommerce.ECommerce_project.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Redis Lua Script 설정
 *
 * Lua Script를 사용하면:
 * 1. 완벽한 원자성 보장 (모든 연산이 하나의 트랜잭션처럼 실행)
 * 2. 네트워크 왕복 최소화 (여러 Redis 명령을 한 번에 실행)
 * 3. Race Condition 완전 방지
 */
@Configuration
public class RedisScriptConfig {

    /**
     * 선착순 쿠폰 발급 Lua Script
     *
     * KEYS[1]: coupon:issue:{couponId}
     * ARGV[1]: userId (String)
     * ARGV[2]: timestamp (long)
     * ARGV[3]: maxQuantity (int)
     * ARGV[4]: TTL in seconds (long)
     *
     * 반환값:
     * - >= 0: 발급 성공, 순위 반환 (0부터 시작)
     * - -1: 이미 발급받음 (중복)
     * - -2: 수량 초과
     */
    @Bean(name = {"redisCouponIssueScript", "레디스 선착순 쿠폰 발급 스크립트"})
    public RedisScript<Long> couponIssueScript() {
        String script = """
                -- 1. 중복 체크: 이미 발급받았는지 확인
                local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
                if score then
                    return -1
                end

                -- 2. 수량 체크: ZADD 전에 먼저 확인 (동시성 문제 해결)
                local currentCount = redis.call('ZCARD', KEYS[1])
                if currentCount >= tonumber(ARGV[3]) then
                    return -2
                end

                -- 3. Sorted Set에 추가 (timestamp를 score로 사용)
                redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])

                -- 4. 본인의 순위 확인 (0부터 시작, 0 = 1등)
                local rank = redis.call('ZRANK', KEYS[1], ARGV[1])

                -- 5. 순위가 최대 수량 이내인지 검증 (이중 체크)
                if rank < tonumber(ARGV[3]) then
                    -- TTL 설정 (처음 설정되지 않은 경우에만)
                    local ttl = redis.call('TTL', KEYS[1])
                    if ttl == -1 then
                        redis.call('EXPIRE', KEYS[1], ARGV[4])
                    end
                    return rank
                else
                    -- 순위 초과 시 삭제하고 실패 반환
                    redis.call('ZREM', KEYS[1], ARGV[1])
                    return -2
                end
                """;

                return RedisScript.of(script, Long.class);
    }
}
