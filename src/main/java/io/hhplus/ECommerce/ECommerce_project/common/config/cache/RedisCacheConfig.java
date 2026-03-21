package io.hhplus.ECommerce.ECommerce_project.common.config.cache;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.hibernate6.Hibernate6Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * Redis 캐시 설정
 * - 분산 환경에서 여러 인스턴스 간 캐시 공유
 * - 영속성이 필요하거나 대용량 데이터 캐싱에 적합
 * - 다중 인스턴스 환경에서 효과적
 */
@Configuration
public class RedisCacheConfig {

    /**
     * Redis 캐시 매니저
     * - 동적으로 캐시 생성 가능 (@Cacheable의 cacheManager 속성으로 지정)
     * - 예: @Cacheable(value = "productList", cacheManager = "redisCacheManager")
     */
    @Bean
    public CacheManager redisCacheManager(RedisConnectionFactory connectionFactory) {
        // ObjectMapper 설정: Java 8 날짜/시간 타입 + Hibernate Lazy Loading 지원
        ObjectMapper objectMapper = new ObjectMapper();

        // Java 8 날짜/시간 타입 지원 (LocalDateTime 등)
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);  // ISO-8601 형식으로 직렬화

        // Hibernate Lazy Loading 프록시 객체 직렬화 지원
        Hibernate6Module hibernateModule = new Hibernate6Module();
        hibernateModule.configure(Hibernate6Module.Feature.FORCE_LAZY_LOADING, false);  // Lazy 로딩 강제 실행 안함
        objectMapper.registerModule(hibernateModule);

        // 역직렬화 설정: 알 수 없는 속성 무시 (Lombok boolean getter 문제 해결)
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 타입 정보 포함 (역직렬화 시 올바른 타입으로 변환하기 위함)
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfSubType(Object.class)  // 모든 타입 허용
                .build();
        objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);

        RedisCacheConfiguration config =
                RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(2))  // 2분 TTL
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJackson2JsonRedisSerializer(objectMapper)));

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(config)
                .build();
    }
}