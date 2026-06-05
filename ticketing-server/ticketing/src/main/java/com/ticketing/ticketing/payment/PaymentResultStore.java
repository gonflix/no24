package com.ticketing.ticketing.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResultStore {

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;

    private static final String KEY_PREFIX = "payment:result:";
    private static final Duration TTL = Duration.ofMinutes(10);

    public void save(PaymentResultEvent result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            RBucket<String> bucket = redissonClient.getBucket(KEY_PREFIX + result.reservationId());
            bucket.set(json, TTL);
            log.info("Saved payment result. reservationId={}, success={}", result.reservationId(), result.success());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payment result. reservationId=" + result.reservationId(), e);
        }
    }

    public Optional<PaymentResultEvent> get(String reservationId) {
        RBucket<String> bucket = redissonClient.getBucket(KEY_PREFIX + reservationId);
        String json = bucket.get();
        if (json == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(json, PaymentResultEvent.class));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment result. reservationId={}", reservationId, e);
            return Optional.empty();
        }
    }
}
