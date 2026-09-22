# Kafka Producer–Consumer

An event-driven order processing system built with two Spring Boot microservices that communicate through Apache Kafka, with MongoDB for persistence. The whole stack runs with a single Docker Compose command.

## Architecture

```
                 HTTP                                              
  Client ───────────────▶ Producer Service (:8080)                 
                               │                                   
                               ▼                                   
                 ┌──────────── Kafka ────────────┐                 
                 │  order-create-events          │                 
                 │  order-update-events          │                 
                 └───────────────────────────────┘                 
                               │                                   
                               ▼                                   
                     Consumer Service (:8081) ───▶ MongoDB         

  Failed messages ──▶ producer-dlx / consumer-dlx (dead-letter topics)
```

## Tech Stack

- Java & Spring Boot (Spring Kafka, Spring Data MongoDB, Spring Retry)
- Apache Kafka & ZooKeeper (Confluent images)
- MongoDB
- Docker Compose

## Getting Started

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)

```bash
git clone https://github.com/mohamadwattad56/kafka-producer-consumer.git
cd kafka-producer-consumer
docker compose up
```

This starts ZooKeeper, Kafka, MongoDB, and both services. An `init-topics` container creates all four Kafka topics automatically on startup.

- Producer service: http://localhost:8080
- Consumer service: http://localhost:8081

## Kafka Topics

| Topic | Purpose |
|---|---|
| `order-create-events` | New orders. The producer publishes serialized `Order` objects; the consumer calculates shipping costs, validates the order, and saves it to MongoDB. |
| `order-update-events` | Order status changes. The producer publishes messages with `orderId` and `status`; the consumer updates the matching order in MongoDB. |
| `producer-dlx` | Dead-letter topic for messages the producer failed to serialize or deliver. |
| `consumer-dlx` | Dead-letter topic for messages the consumer failed to deserialize, validate, or save. |

### Message key: `orderId`

Both order topics use `orderId` as the message key. This ensures:

- All events for the same order go to the same partition, so they are processed in the correct order.
- The consumer can immediately identify which order an event belongs to.
- Related events for one order stay consistent and are efficient to look up.

### Why separate topics for create and update?

Splitting creation and update events into two topics keeps each consumer focused on only the messages it cares about, with no filtering logic needed. It also makes it easier to add new services later and lets each part of the system scale independently as the application grows.

## API Endpoints

### Producer Service (port 8080)

| Endpoint | Description |
|---|---|
| `/api/produce/create-order?orderId={id}&numberOfItems={n}` | Publishes a new order event to `order-create-events`. |
| `/api/produce/update-order?orderId={id}&status={status}` | Publishes a status change to `order-update-events`. |

### Consumer Service (port 8081)

| Endpoint | Description |
|---|---|
| `/api/orders/order-details?orderId={id}` | Returns the order's details from MongoDB. Returns `404` if the order doesn't exist. |
| `/api/orders/getAllOrderIdsFromTopic?topicName={topic}` | Returns all order IDs for the given topic. Returns `404` if none are found. |

**Example:**

```
http://localhost:8080/api/produce/create-order?orderId=1001&numberOfItems=3
http://localhost:8081/api/orders/order-details?orderId=1001
```

## Error Handling

Every failed message is sent to a dead-letter topic along with its original topic, key, value, and an error description, so nothing is silently lost.

### Producer

- **Serialization errors:** caught during serialization, logged with debugging details, and sent to `producer-dlx`.
- **Delivery errors** (timeouts, unavailable partitions, network issues, authorization failures): logged by exception type (e.g. `TimeoutException`, `AuthorizationException`) and sent to `producer-dlx` for recovery or manual review.
- **Retries:** relies on Kafka's built-in producer retries with a backoff period for transient delivery failures.

### Consumer

- **Deserialization errors:** caught when parsing with `ObjectMapper`; the raw message and error are sent to `consumer-dlx`, and the consumer keeps processing other messages.
- **Validation errors:** logged with the reason for failure and sent to `consumer-dlx`, so invalid data never reaches the database.
- **Database errors** (e.g. MongoDB connection issues): retried with Spring Retry using exponential backoff:
  - Up to 3 attempts
  - Initial delay: 2000 ms
  - Multiplier: 2.0
  - Maximum delay: 10,000 ms

  If all retries fail, the error is logged and the message is sent to `consumer-dlx`.
- **Unexpected exceptions:** logged with the exception, topic, and key, then sent to `consumer-dlx`.

### Design goals

- **Reliability:** no message is dropped, even in unexpected failure scenarios.
- **Observability:** logs and dead-letter topics make every failure visible and traceable.
- **Separation of concerns:** failed messages are handled outside the main flow, so errors don't block normal processing.

## Author

**Mohamad Wattad** · Built as part of an Event-Driven Architecture course.
