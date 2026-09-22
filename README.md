# kafka-producer-consumer
1) Full name : Mohamad Wattad , ID : 212488381

2) Topic Names and Their Purpose:

i) Topic name : order-create-events
Purpose: Used for publishing and consuming events related to the creation of new orders. The producer sends serialized Order objects containing order details, and the consumer processes them to calculate shipping costs, validate the order, and persist it in MongoDB.

ii) Topic name : order-update-events:
Purpose: Used for publishing and consuming events related to updates to existing orders. The producer sends messages with orderId and status fields, and the consumer updates the corresponding order's status in MongoDB.

Key Used in both of the topics in the Message and Why :
Key: orderId
Purpose:
Ensures all events for the same order are sent to the same Kafka partition, guaranteeing the correct order of processing.
Simplifies consumer logic by uniquely identifying the order associated with the event.
Maintains consistency and allows for efficient lookup and processing of related order events.

iii) Topic name : producer-dlx

iv) Topic name : consumer-dlx

Purposes of these two topics: If all retries are exhausted or an unexpected exception occurs, a detailed error message (including the original topic, key, value, and error description) is sent to these topics.

Why Use Two Topics?

I opted to use two separate topics—one for creation events and another for update events—rather than a single topic. This design simplifies the addition of new services and features in the future. By segregating events into distinct topics, consumers can avoid complex filtering logic, ensuring each service processes only the messages relevant to its functionality. This approach supports independent scalability for services, accommodates anticipated application growth, and adheres to best practices for building scalable and maintainable systems.

3) Error Handling Approaches:

Producer:
1. Serialization Errors:
   The error is caught during serialization.
   The error message is logged with detailed information for debugging.
   The message (along with the error description) is sent to a Dead Letter Queue (DLQ) named producer-dlx.
   Why: Ensures that even messages with serialization issues are traceable and available for later analysis.

2. Kafka Delivery Errors:
   Scenario: If the Kafka broker fails to acknowledge a message due to:
   - Timeout
   - Partition unavailability
   - Network issues
   - Authorization errors
   Handling:
   - Categorized exceptions are logged to provide detailed insights into the nature of the error (e.g., TimeoutException, AuthorizationException).
   - The message and error details are forwarded to producer-dlx to prevent data loss.
   Why: Allows recovery or manual intervention by analyzing the messages in the DLQ.

Consumer:
1. Deserialization Errors:
   Handling:
   - The error is caught during deserialization using ObjectMapper.
   - The error message and the raw Kafka message are sent to the consumer-dlx for analysis.
   Why: Ensures traceability for invalid messages while allowing the consumer to process other valid messages.

2. Validation Errors:
   Handling:
   - The error is logged with details of the validation failure.
   - The invalid message is sent to the consumer-dlx with a descriptive error message.
   Why: Prevents further processing of invalid data while preserving it for debugging or recovery.

3. Database Operation Errors:
   Scenario: If an error occurs while saving or updating an order in MongoDB (e.g., connection issues).
   Handling:
   - Retries: The consumer uses Spring Retry to retry the operation up to 3 times with exponential backoff:
     - Initial delay: 2000ms
     - Multiplier: 2.0 (delay doubles after each attempt)
     - Maximum delay: 10,000ms
   - If all retries fail:
     - The error is logged.
     - The message and error details are sent to consumer-dlx.
   Why: Handles transient errors (e.g., database downtime) while ensuring persistent failures are not ignored.

General Exception Handling:
Scenario: Any unexpected exception during message processing.
Handling:
- The error is logged with details of the exception, topic, and key.
- The message is forwarded to consumer-dlx for later analysis.
Why: Ensures no message is dropped, even in unanticipated scenarios.

Retries and Backoff Mechanism:
Producer:
- Relies on Kafka's internal retries for transient failures during message delivery.
- Configured with multiple retry attempts and a backoff period.
Consumer:
- Uses Spring Retry to retry database operations or other transient failures.
- Employs exponential backoff to reduce system load during repeated failures.

Why These Approaches?
- Reliability: Ensures no data is lost, even in the presence of errors.
- Observability: Logs and DLQs provide visibility into failures and their causes.
- Separation of Concerns: DLQs allow error analysis and recovery without impacting the main processing flow.



4) Endpoints :
Create Order
URL: http://localhost:8080/api/produce/create-order?orderId=X&numberOfItems=Y
Description: Publishes an event to the order-create-events topic with details about the new order.

Update Order
URL: http://localhost:8080/api/produce/update-order?orderId=X&status=Y
Description: Publishes an event to the order-update-events topic to notify other services about the status change.



Here is the requested format:

Producer Service Endpoints:
Create Order

URL: http://localhost:8080/api/produce/create-order?orderId=X&numberOfItems=Y
Description: Publishes an event to the order-create-events topic with details about the new order.
Update Order

URL: http://localhost:8080/api/produce/update-order?orderId=X&status=Y
Description: Publishes an event to the order-update-events topic to notify other services about the status change.


Get Order Details
URL: http://localhost:8081/api/orders/order-details?orderId=X
Description: Retrieves the details of a specific order from MongoDB. Returns 404 Not Found if the order does not exist.
Get All Order IDs from a Topic

URL: http://localhost:8081/api/orders/getAllOrderIdsFromTopic?topicName=TOPIC_NAME
Description: Provides a list of all order IDs associated with the specified topic. Returns 404 Not Found if no orders are found for the given topic.


