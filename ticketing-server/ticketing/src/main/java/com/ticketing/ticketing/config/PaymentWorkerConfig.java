package com.ticketing.ticketing.config;

import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@EnableAsync
@Configuration
public class PaymentWorkerConfig {

    @Bean("paymentWorkerExecutor")
    ThreadPoolTaskExecutor paymentWorkerExecutor(
            @Value("${app.kafka.payment-worker-threads}") int threads) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(threads * 2);
        executor.setThreadNamePrefix("payment-worker-");
        // 큐가 꽉 차면 Consumer 스레드(=Caller)가 직접 실행 => Backpressure 효과
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
