package com.ticketing.ticketing.config;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

        @Bean // SingleTone
        public ConsumerFactory<String, PaymentRequestedEvent> paymentConsumerFactory(
                        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                        @Value("${spring.kafka.consumer.group-id}") String groupId) {
                Map<String, Object> props = new HashMap<>();
                // Consumer 기본 설정
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
                props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.ticketing.ticketing.kafka");
                return new DefaultKafkaConsumerFactory<>(
                                props,
                                new StringDeserializer(),
                                new JsonDeserializer<>(PaymentRequestedEvent.class, false));
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, PaymentRequestedEvent> paymentKafkaListenerContainerFactory(
                        ConsumerFactory<String, PaymentRequestedEvent> paymentConsumerFactory) {
                ConcurrentKafkaListenerContainerFactory<String, PaymentRequestedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
                // Listener 컨테이너에 사용할 ConsumerFactory 주입
                factory.setConsumerFactory(paymentConsumerFactory);
                factory.setConcurrency(1);
                return factory;
        }

        @Bean
        public ProducerFactory<String, PaymentRequestedEvent> paymentProducerFactory(
                        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
                Map<String, Object> props = new HashMap<>();
                // Producer 기본 설정
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
                return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean
        public KafkaTemplate<String, PaymentRequestedEvent> paymentKafkaTemplate(
                        ProducerFactory<String, PaymentRequestedEvent> paymentProducerFactory) {
                // KafkaTemplate 빈 등록: PaymentProducer에서 주입받는 대상
                return new KafkaTemplate<>(paymentProducerFactory);
        }
}
