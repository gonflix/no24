package com.ticketing.ticketing.payment;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

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
            resultMap.put(result.reservationEid(), json, TTL_MINUTES, TimeUnit.MINUTES);
            log.info("Saved payment result. reservationEid={}, success={}", result.reservationEid(),
                    result.success());
        } catch (JacksonException e) {
            throw new RuntimeException(
                    "Failed to serialize payment result. reservationEid=" + result.reservationEid(),
                    e);
        }
    }

    public Optional<PaymentResultEvent> get(String reservationEid) {
        String json = resultMap.get(reservationEid);
        if (json == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(json, PaymentResultEvent.class));
        } catch (JacksonException e) {
            log.error("Failed to deserialize payment result. reservationEid={}", reservationEid, e);
            return Optional.empty();
        }
    }
}
