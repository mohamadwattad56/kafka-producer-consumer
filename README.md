# Kafka Producer–Consumer

An event-driven order processing system made of two Spring Boot microservices that communicate through Apache Kafka. The consumer service stores orders in MongoDB. The whole stack runs with Docker Compose.

## Architecture

```
Client ── POST / PUT ──▶ Producer Service (:8080)
                                │
                                ▼
                    Kafka: order-create-events
                           order-update-events
                                │
                                ▼
Client ◀──── GET ────── Consumer Service (:8081) ──▶ MongoDB

Failed messages ──▶ producer-dlx / consumer-dlx
```

## Tech Stack

- Java with Spring Boot 3.4.1
- Spring Kafka (both services)
- Spring Data MongoDB (consumer service only)
- Apache Kafka and ZooKeeper (Confluent Docker images)
- MongoDB
- Docker Compose

## Getting Started

**Prerequisites:** Docker with Docker Compose.

```bash
git clone https://github.com/mohamadwattad56/kafka-producer-consumer.git
cd kafka-producer-consumer
docker compose up
```

This starts ZooKeeper, Kafka, MongoDB, and both services. An `init-topics` container creates the four Kafka topics on startup. The producer and consumer run from pre-built Docker Hub images (`mohamadwattad/kafka-producer` and `mohamadwattad/kafka-consumer`).

- Producer service: http://localhost:8080
- Consumer service: http://localhost:8081

## Kafka Topics

| Topic | Purpose |
|---|---|
| `order-create-events` | New orders. The producer publishes serialized `Order` objects. The consumer validates each order, calculates its shipping cost (2% of the order's total amount), and saves it to MongoDB. |
| `order-update-events` | Order status changes. The producer publishes messages with `orderId` and `status`. The consumer updates the status of the matching order in MongoDB. |
| `producer-dlx` | Dead-letter topic for messages the producer failed to serialize or deliver. |
| `consumer-dlx` | Dead-letter topic for messages the consumer failed to deserialize, validate, or save, and for update events whose order doesn't exist. |

### Message key: `orderId`

Both order topics use `orderId` as the message key. This ensures:

- All events for the same order go to the same partition, so they are processed in the correct order.
- The consumer can immediately identify which order an event belongs to.
- Related events for one order stay consistent and are efficient to look up.

### Why separate topics for create and update?

Splitting creation and update events into two topics keeps each consumer focused on only the messages it cares about, with no filtering logic needed. It also makes it easier to add new services later and lets each part of the system scale independently as the application grows.

## API Endpoints

### Producer Service (port 8080)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/produce/create-order?orderId={id}&numberOfItems={n}` | Publishes a new order event to `order-create-events`. |
| `PUT` | `/api/produce/update-order?orderId={id}&status={status}` | Publishes a status change to `order-update-events`. |

### Consumer Service (port 8081)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/orders/order-details?orderId={id}` | Returns the order's details from MongoDB. Returns `404` if the order doesn't exist. |
| `GET` | `/api/orders/getAllOrderIdsFromTopic?topicName={topic}` | Returns the IDs of orders the consumer has received from the given topic since it last started. This list is kept in memory, not in MongoDB, so it resets on restart. Returns `404` if none are found. |

### Examples

Replace the values in `< >` with your own:

```bash
curl -X POST "http://localhost:8080/api/produce/create-order?orderId=<orderId>&numberOfItems=<numberOfItems>"
curl -X PUT "http://localhost:8080/api/produce/update-order?orderId=<orderId>&status=<status>"
curl "http://localhost:8081/api/orders/order-details?orderId=<orderId>"
curl "http://localhost:8081/api/orders/getAllOrderIdsFromTopic?topicName=order-create-events"
```

## Error Handling

Failed messages are sent to a dead-letter topic with the topic name, the order ID (when available), and a description of the error, rather than being silently dropped.

### Producer

- **Serialization errors:** caught during serialization, logged with debugging details, and sent to `producer-dlx`.
- **Delivery errors** (timeouts, unavailable partitions, network issues, authorization failures): logged by exception type (e.g. `TimeoutException`, `AuthorizationException`) and sent to `producer-dlx`.
- **Delivery settings:** the producer is configured with `acks=all`, idempotence enabled, `retries=5`, and `retry.backoff.ms=500`.

Messages sent to `producer-dlx` use `orderId` as their key.

### Consumer

- **Deserialization errors:** caught when parsing with `ObjectMapper`. An error description is sent to `consumer-dlx`, and the consumer continues processing other messages.
- **Validation errors:** logged with the reason for the failure and sent to `consumer-dlx`. The invalid message isn't processed further.
- **Order not found on update:** a warning is logged and the event is sent to `consumer-dlx`.
- **Database errors** (e.g. MongoDB connection issues): logged and sent to `consumer-dlx`. See [Known Limitations](#known-limitations) regarding retries.
- **Unexpected exceptions:** logged with the exception, topic, and key, then sent to `consumer-dlx`.

Messages sent to `consumer-dlx` are JSON and have no message key.

### Design goals

- **Reliability:** failures are captured in dead-letter topics instead of being silently dropped.
- **Observability:** logs and dead-letter topics make failures visible and traceable.
- **Separation of concerns:** failed messages are handled outside the main flow, so errors don't block normal processing.

## Known Limitations

- **Database retries aren't active yet.** The consumer's save method is annotated with `@Retryable` (3 attempts, 2000 ms initial delay, 2.0 multiplier, 10,000 ms max delay), but `@EnableRetry` isn't enabled, and the method is private and called from within the same class, so Spring doesn't apply the retry. A failed save is currently not retried.
- **Dead-letter messages don't include the original payload.** They contain the topic, order ID, and error description, so a failed message can't be replayed from the dead-letter topic.
- **Wrong topic label for producer delivery failures.** When the producer fails to deliver a message to Kafka, the dead-letter message shows "Error forwarding to Kafka" in place of the original topic name.
- **No request validation in the producer.** A `RequestValidator` class exists in the producer service but isn't called, so invalid requests are only rejected by the consumer.

## Author

**Mohamad Wattad**
