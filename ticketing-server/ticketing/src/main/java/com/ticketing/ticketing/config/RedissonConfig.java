package com.ticketing.ticketing.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(@Value("${spring.data.redis.host}") String host,
            @Value("${spring.data.redis.port}") int port,
            @Value("${spring.data.redis.password}") String password) {
        Config config = new Config();
        String endpoint = "redis://" + host + ":" + port;
        SingleServerConfig singleServerConfig = config.useSingleServer().setAddress(endpoint);

        if (password != null && !password.isBlank()) {
            singleServerConfig.setPassword(password);
        }

        singleServerConfig
                .setConnectionMinimumIdleSize(5)
                .setConnectionPoolSize(10)
                .setIdleConnectionTimeout(10000)
                .setConnectTimeout(10000) // 연결 타임아웃 (ms)
                .setTimeout(3000) // 명령 실행 타임아웃 (ms)
                .setRetryAttempts(3);
        // .setRetryInterval(1500);

        return Redisson.create(config);
    }
}
