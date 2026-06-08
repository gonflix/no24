package com.ticketing.ticketing.payment;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResultStore {

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;

    private static final String MAP_KEY = "payment:results";
    private static final long TTL_MINUTES = 10;

    private RMapCache<String, String> resultMap;

    @PostConstruct
    void init() {
        this.resultMap = redissonClient.getMapCache(MAP_KEY);
    }

    public void save(PaymentResultEvent result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            resultMap.put(result.reservationId(), json, TTL_MINUTES, TimeUnit.MINUTES);
            log.info("Saved payment result. reservationId={}, success={}", result.reservationId(), result.success());
        } catch (JacksonException e) {
            throw new RuntimeException("Failed to serialize payment result. reservationId=" + result.reservationId(), e);
        }
    }

    public Optional<PaymentResultEvent> get(String reservationId) {
        String json = resultMap.get(reservationId);
        if (json == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(json, PaymentResultEvent.class));
        } catch (JacksonException e) {
            log.error("Failed to deserialize payment result. reservationId={}", reservationId, e);
            return Optional.empty();
        }
    }
}
