package com.ecommerce.producerservice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        Properties props = new Properties();

        // Specify Kafka broker(s)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        // Serialization
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Acknowledgment and reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas to acknowledge
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Enable idempotence for exactly-once delivery

        // Retries and backoff
        props.put(ProducerConfig.RETRIES_CONFIG, 5); // Retry up to 5 times
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500); // Backoff time between retries

        // Timeout settings
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); // 30 seconds for broker response
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // Max time for message delivery (2 minutes)

        // Optional for monitoring: Add client ID to identify this producer
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-service");

        return new KafkaProducer<>(props);
    }
}
