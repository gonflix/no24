package com.ticketing.ticketing.notification;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ticketing.ticketing.payment.PaymentResultEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class PushNotificationService {
    private static final long DEFAULT_TIMEOUT = 60 * 60 * 1000L; // 1시간
    private final Map<String, SseEmitter> mapEmitter = new ConcurrentHashMap<>();

    public SseEmitter subscribe(String userId) {
        SseEmitter emitter = new SseEmitter(DEFAULT_TIMEOUT);
        mapEmitter.put(userId, emitter);

        emitter.onCompletion(() -> mapEmitter.remove(userId));
        emitter.onTimeout(() -> mapEmitter.remove(userId));
        emitter.onError(ex -> mapEmitter.remove(userId));

        try {
            emitter.send(SseEmitter.event().name("connected").data("push subscription ready"));
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
        return emitter;
    }

    public void pushPaymentResult(PaymentResultEvent result) {
        SseEmitter emitter = mapEmitter.get(result.userId());
        if (emitter == null) {
            log.info("No active push channel for userId={}, event={}", result.userId(), result);
            return;
        }

        try {
            emitter.send(SseEmitter.event().name("payment_result").data(result));
        } catch (IOException e) {
            mapEmitter.remove(result.userId());
            emitter.completeWithError(e);
        }
    }
}
