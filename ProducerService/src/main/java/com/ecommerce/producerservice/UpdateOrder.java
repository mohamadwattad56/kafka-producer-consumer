package com.ecommerce.producerservice;

import jakarta.validation.constraints.NotBlank;

public class UpdateOrder {

    @NotBlank
    private String orderId;

    @NotBlank
    private String status;

    // Default constructor for deserialization
    public UpdateOrder() {}

    public UpdateOrder(String orderId, String status) {
        this.orderId = orderId;
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "UpdateOrder{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
