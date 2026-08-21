package com.kafka.examples.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Instant;
import java.util.UUID;

/**
 * Shared JSON utility and domain event models used across all examples.
 *
 * All events follow the same envelope pattern:
 *   eventId, eventType, timestamp, sourceService, payload
 */
public class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed for: " + json, e);
        }
    }

    // ── Domain Event Envelope ──────────────────────────────────────────────
    public record DomainEvent(
            String eventId,
            String eventType,
            String timestamp,
            String sourceService,
            Object payload
    ) {
        public static DomainEvent of(String eventType, String sourceService, Object payload) {
            return new DomainEvent(
                    UUID.randomUUID().toString(),
                    eventType,
                    Instant.now().toString(),
                    sourceService,
                    payload
            );
        }
    }

    // ── Domain Models ──────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AccountDeletedEvent(
            String customerId,
            String email,
            String reason,
            String requestedBy
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record VehicleMappedEvent(
            String vehicleId,
            String customerId,
            String vin,
            String modelYear,
            String brand
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderPlacedEvent(
            String orderId,
            String customerId,
            String productId,
            int quantity,
            double totalAmount,
            String status
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CustomerProfile(
            String customerId,
            String name,
            String email,
            String tier,           // GOLD, SILVER, STANDARD
            String preferredRegion
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ServiceNowIncident(
            String incidentId,
            String category,
            String severity,
            String description,
            String assignedTeam
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WarehouseEvent(
            String warehouseId,
            String productId,
            String action,         // STOCK_IN, STOCK_OUT, ADJUSTMENT
            int quantity,
            int newStockLevel
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ShipmentEvent(
            String shipmentId,
            String orderId,
            String status,         // PICKED, PACKED, SHIPPED, DELIVERED
            String location,
            String carrier
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PageView(
            String userId,
            String page,
            String sessionId,
            long durationMs
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SensorReading(
            String sensorId,
            String location,
            double temperature,
            double humidity,
            long readingTimeMs
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StockPrice(
            String ticker,
            double price,
            double previousClose,
            long timestampMs
    ) {}
}
