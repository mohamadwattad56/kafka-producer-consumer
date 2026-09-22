package com.ecommerce.consumerservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.ecommerce.consumerservice.RequestValidator.validateOrderCreation;
import static com.ecommerce.consumerservice.RequestValidator.validateOrderUpdate;

@RequestMapping("/api/orders")
@RestController
public class ConsumerController {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerController.class);
    private final Map<String, List<String>> topicOrderMapping = new ConcurrentHashMap<>();

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ConsumerController(OrderRepository orderRepository, ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    // Listener for the "order-create-events" topic
    @KafkaListener(topics = "order-create-events", groupId = "order-create-service-group", containerFactory = "orderCreateKafkaListenerContainerFactory")
    public void consumeOrderCreation(Order order) {
        try {
            // Validate the order
            validateOrderCreation(order.getOrderId(), order.getItems().size());

            // Calculate shipping cost
            BigDecimal shippingCost = order.getTotalAmount().multiply(BigDecimal.valueOf(0.02));
            order.setShippingCost(shippingCost);

            topicOrderMapping.computeIfAbsent("order-create-events", k -> new ArrayList<>()).add(order.getOrderId());

            // Save to MongoDB
            saveOrderWithRetry(order);

            logger.info("Order created and saved: {}", order.getOrderId());

        } catch (org.apache.kafka.common.errors.AuthorizationException e) {
            handleException("Authorization", "order-create-events", order.getOrderId(), e);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            handleException("Timeout", "order-create-events", order.getOrderId(), e);
        } catch (org.apache.kafka.common.errors.InvalidTopicException e) {
            handleException("Invalid Topic", "order-create-events", order.getOrderId(), e);
        } catch (IllegalArgumentException e) {
            handleException("Validation", "order-create-events", order.getOrderId(), e);
        } catch (Exception e) {
            handleException("Processing", "order-create-events", order.getOrderId(), e);
        }
    }



    // Listener for the "order-update-events" topic
    @KafkaListener(topics = "order-update-events", groupId = "order-update-service-group", containerFactory = "orderUpdateKafkaListenerContainerFactory")
    public void consumeOrderUpdate(String message) {
        String orderId = null; // Track orderId for better logging
        try {
            // Deserialize the message
            UpdateOrder updateOrder = objectMapper.readValue(message, UpdateOrder.class);
            orderId = updateOrder.getOrderId();

            // Validate the update
            validateOrderUpdate(orderId, updateOrder.getStatus());

            // Update order in MongoDB
            String finalOrderId = orderId;
            orderRepository.findById(orderId).ifPresentOrElse(order -> {
                order.setStatus(updateOrder.getStatus());
                orderRepository.save(order);
                logger.info("Order updated successfully. Order ID: {}, Status: {}", finalOrderId, updateOrder.getStatus());
            }, () -> {
                logger.warn("Order not found for update. Order ID: {}", finalOrderId);
                handleDeadLetter("Order not found", "order-update-events", finalOrderId);
            });

            // Track order in topic mapping
            topicOrderMapping.computeIfAbsent("order-update-events", k -> new ArrayList<>()).add(orderId);

        } catch (JsonProcessingException e) {
            handleException("Deserialization", "order-update-events", orderId, e);
        } catch (org.apache.kafka.common.errors.AuthorizationException e) {
            handleException("Authorization", "order-update-events", orderId, e);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            handleException("Timeout", "order-update-events", orderId, e);
        } catch (org.apache.kafka.common.errors.InvalidTopicException e) {
            handleException("Invalid Topic", "order-update-events", orderId, e);
        } catch (IllegalArgumentException e) {
            handleException("Validation", "order-update-events", orderId, e);
        } catch (Exception e) {
            handleException("Processing", "order-update-events", orderId, e);
        }
    }




    // Retryable method to save orders to MongoDB
    @Retryable(value = {RuntimeException.class}, maxAttempts = 3, backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 10000))
    private void saveOrderWithRetry(Order order) {
        orderRepository.save(order);
    }

    // Handle forwarding failed messages to a dead-letter topic
    private void handleDeadLetter(String message, String originalTopic, String orderId) {
        try {
            Map<String, String> dlqMessage = new HashMap<>();
            dlqMessage.put("originalTopic", originalTopic);
            dlqMessage.put("message", message);
            dlqMessage.put("OrderId", orderId);
            String jsonMessage = objectMapper.writeValueAsString(dlqMessage);

            kafkaTemplate.send("consumer-dlx", jsonMessage)
                    .thenAccept(result -> logger.info("Message sent to DLX - topic: {}, partition: {}, offset: {}",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()))
                    .exceptionally(ex -> {
                        logger.error("Failed to send message to DLX: {}", ex.getMessage(), ex);
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Critical error while forwarding to DLX: {}", e.getMessage(), e);
        }
    }



    private void handleException(String operation, String topic, String orderId, Exception e) {
        logger.error("Error during {} for topic: {}, orderId: {}. Exception: {}, Message: {}",
                operation, topic, orderId, e.getClass().getSimpleName(), e.getMessage(), e);

        // Optional: Forward to DLQ
        String errorMessage = String.format("Operation: %s, Topic: %s, Order ID: %s, Error: %s",
                operation, topic, orderId, e.getMessage());
        handleDeadLetter(errorMessage, topic,orderId);
    }




    // API to get order details
    @GetMapping("/order-details")
    public ResponseEntity<Order> getOrderDetails(@RequestParam String orderId) {
        return orderRepository.findById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/getAllOrderIdsFromTopic")
    public ResponseEntity<List<String>> getAllOrderIdsFromTopic(@RequestParam String topicName) {
        List<String> orderIds = topicOrderMapping.getOrDefault(topicName, Collections.emptyList());
        if (orderIds.isEmpty()) {
            logger.warn("No order IDs found for topic: {}", topicName);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(orderIds);
    }
}
