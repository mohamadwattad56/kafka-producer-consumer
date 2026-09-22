package com.ecommerce.producerservice;

public class RequestValidator {
    public static void validateOrderCreation(String orderId, int numberOfItems) {
        if (numberOfItems <= 0) {
            throw new IllegalArgumentException("Invalid number of items: " + numberOfItems);
        }
        validateOrderId(orderId);
    }

    public static void validateOrderUpdate(String orderId, String status) {
        validateOrderId(orderId);
        validateStatus(status);
    }

    private static void validateOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Invalid orderId: " + orderId);
        }
    }

    private static void validateStatus(String status) {
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("Invalid status: " + status);
        }
    }
}
