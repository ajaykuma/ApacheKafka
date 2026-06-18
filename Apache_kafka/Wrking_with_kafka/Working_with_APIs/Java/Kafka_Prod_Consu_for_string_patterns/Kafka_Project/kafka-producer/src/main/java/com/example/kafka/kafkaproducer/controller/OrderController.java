package com.example.kafka.kafkaproducer.controller;

import com.example.kafka.kafkaproducer.model.OrderEvent;
import com.example.kafka.kafkaproducer.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * ADDITIONAL CONTROLLER — Order REST API
 * =========================================
 * Extends your existing ProducerController (which sends plain String to "Topic1")
 * with realistic order-processing endpoints demonstrating each producer pattern.
 *
 * Test each endpoint with curl or browser:
 *
 *   POST http://localhost:8080/orders/json?customerId=CUST-100&productId=PROD-A1
 *   POST http://localhost:8080/orders/async?customerId=CUST-101&productId=PROD-B3
 *   POST http://localhost:8080/orders/keyed?customerId=CUST-100&productId=PROD-A1&key=PROD-A1
 *   POST http://localhost:8080/orders/transactional?customerId=CUST-102&productId=PROD-C7&fail=false
 *   POST http://localhost:8080/orders/transactional?customerId=CUST-103&productId=PROD-D2&fail=true
 *   POST http://localhost:8080/orders/fire-and-forget?customerId=CUST-100&productId=PROD-A1
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping("/json")
    public String sendJsonOrder(
            @RequestParam String customerId,
            @RequestParam String productId,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam(defaultValue = "99.99") double amount) {

        var order = new OrderEvent(generateOrderId(), customerId, productId, quantity, amount, "PENDING");
        orderService.sendJsonOrder(order);
        return "Order sent (JSON): " + order.getOrderId();
    }

    @PostMapping("/async")
    public String sendAsyncOrder(
            @RequestParam String customerId,
            @RequestParam String productId,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam(defaultValue = "99.99") double amount) {

        var order = new OrderEvent(generateOrderId(), customerId, productId, quantity, amount, "PENDING");
        orderService.sendOrderAsync(order);
        return "Order send initiated (async, check logs for result): " + order.getOrderId();
    }

    @PostMapping("/keyed")
    public String sendKeyedOrder(
            @RequestParam String customerId,
            @RequestParam String productId,
            @RequestParam String key,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam(defaultValue = "99.99") double amount) {

        var order = new OrderEvent(generateOrderId(), customerId, productId, quantity, amount, "PENDING");
        orderService.sendOrderWithKey(order, key);
        return "Order sent with custom key='" + key + "': " + order.getOrderId();
    }

    @PostMapping("/transactional")
    public String sendTransactionalOrder(
            @RequestParam String customerId,
            @RequestParam String productId,
            @RequestParam(defaultValue = "false") boolean fail,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam(defaultValue = "99.99") double amount) {

        var order = new OrderEvent(generateOrderId(), customerId, productId, quantity, amount, "PENDING");
        try {
            orderService.sendOrdersTransactionally(order, fail);
            return "Transaction COMMITTED: " + order.getOrderId();
        } catch (Exception e) {
            return "Transaction ABORTED for " + order.getOrderId() + ": " + e.getMessage();
        }
    }

    @PostMapping("/fire-and-forget")
    public String sendFireAndForget(
            @RequestParam String customerId,
            @RequestParam String productId,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam(defaultValue = "99.99") double amount) {

        var order = new OrderEvent(generateOrderId(), customerId, productId, quantity, amount, "PENDING");
        orderService.sendOrderFireAndForget(order);
        return "Order fired (no confirmation): " + order.getOrderId();
    }

    private String generateOrderId() {
        return "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
