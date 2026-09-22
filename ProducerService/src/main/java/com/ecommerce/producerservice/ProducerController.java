package com.ecommerce.producerservice;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/produce")
public class ProducerController {

    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper; // Shared ObjectMapper for JSON

    public ProducerController(KafkaProducer<String, String> kafkaProducer, ObjectMapper objectMapper) {
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/create-order")
    public ResponseEntity<String> createOrder(@RequestParam String orderId, @RequestParam int numberOfItems) {
        Order order = generateOrder(orderId);
        List<OrderItem> items = new ArrayList<>();
        BigDecimal totalAmount = generateItemsAndGetTotalAmount(numberOfItems, items, BigDecimal.ZERO);
        order.setItems(items);
        order.setTotalAmount(totalAmount);

        try {
            // Serialize order to JSON
            String orderJson = objectMapper.writeValueAsString(order);

            // Create ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("order-create-events", orderId, orderJson);

            // Send asynchronously to Kafka
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    handleKafkaException(exception, orderId, orderJson);
                } else {
                    logger.info("Message sent to Kafka - topic: {}, partition: {}, offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            return ResponseEntity.ok("Order created and sent to Kafka: " + orderId);
        } catch (JsonProcessingException e) {
            logger.error("Serialization error while creating order: {}", e.getMessage(), e);
            sendToDLQ(orderId, "Serialization error: " + e.getMessage(), "producer-dlx", "order-create-events");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to serialize order: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during Kafka send operation: {}", e.getMessage(), e);
            sendToDLQ(orderId, "Unexpected error: " + e.getMessage(), "producer-dlx", "order-create-events");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Failed to send message to Kafka. Error: " + e.getMessage());
        }
    }

    @PutMapping("/update-order")
    public ResponseEntity<String> updateOrder(@RequestParam String orderId, @RequestParam String status) {
        try {
            UpdateOrder updateOrder = new UpdateOrder(orderId, status);
            String updateOrderJson = objectMapper.writeValueAsString(updateOrder);

            ProducerRecord<String, String> record = new ProducerRecord<>("order-update-events", orderId, updateOrderJson);

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    handleKafkaException(exception, orderId, updateOrderJson);
                } else {
                    logger.info("Message sent to Kafka - topic: {}, partition: {}, offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            return ResponseEntity.ok("Order update event sent to Kafka: " + orderId);
        } catch (JsonProcessingException e) {
            logger.error("Serialization error while updating order: {}", e.getMessage(), e);
            sendToDLQ(orderId, "Serialization error: " + e.getMessage(), "producer-dlx", "order-update-events");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to serialize order update event: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during Kafka send operation: {}", e.getMessage(), e);
            sendToDLQ(orderId, "Unexpected error: " + e.getMessage(), "producer-dlx", "order-update-events");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Failed to send update to Kafka. Error: " + e.getMessage());
        }
    }

    private void sendToDLQ(String key, String message, String deadLetterTopic, String originalTopic) {
        try {
            String enhancedMessage = String.format("Original Topic: %s, Message: %s, Order id : %s", originalTopic, message, key);
            kafkaProducer.send(new ProducerRecord<>(deadLetterTopic, key, enhancedMessage), (dlxMetadata, dlxException) -> {
                if (dlxException != null) {
                    logger.error("Failed to send message to DLQ: {}", dlxException.getMessage(), dlxException);
                } else {
                    logger.info("Message sent to DLQ - topic: {}, partition: {}, offset: {}",
                            dlxMetadata.topic(), dlxMetadata.partition(), dlxMetadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Critical error while forwarding to DLQ: {}", e.getMessage(), e);
        }
    }

    private void handleKafkaException(Exception exception, String key, String value) {
        String dlxTopic = "producer-dlx";

        if (exception.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
            logger.error("Timeout error while sending message to Kafka: {}", exception.getMessage(), exception);
        } else if (exception.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
            logger.error("Topic or partition error while sending message to Kafka: {}", exception.getMessage(), exception);
        } else if (exception.getCause() instanceof org.apache.kafka.common.errors.AuthorizationException) {
            logger.error("Authorization error while sending message: {}", exception.getMessage(), exception);
        } else {
            logger.error("Unknown error while sending message to Kafka: {}", exception.getMessage(), exception);
        }

        sendToDLQ(key, exception.getMessage(), dlxTopic, "Error forwarding to Kafka");
    }

    private static BigDecimal generateItemsAndGetTotalAmount(int numberOfItems, List<OrderItem> items, BigDecimal totalAmount) {
        for (int i = 0; i < numberOfItems; i++) {
            OrderItem item = new OrderItem();
            item.setItemId("ITEM-" + UUID.randomUUID().toString());
            item.setQuantity((int) (Math.random() * 10) + 1);
            BigDecimal price = BigDecimal.valueOf((Math.random() * 100) + 1).setScale(2, RoundingMode.HALF_UP);
            item.setPrice(price);

            items.add(item);
            totalAmount = totalAmount.add(price.multiply(BigDecimal.valueOf(item.getQuantity())));
        }
        return totalAmount;
    }

    private static Order generateOrder(String orderId) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setCustomerId(UUID.randomUUID().toString());
        order.setOrderDate(OffsetDateTime.now().toString());
        order.setCurrency("USD");
        order.setStatus("new");
        return order;
    }
}
