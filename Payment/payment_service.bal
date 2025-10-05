// Basic imports: These are what we need for HTTP server, logging, Kafka, and time.
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;

// Configurable: Easy to change without editing code (from Config.toml or env vars).
configurable string kafkaBroker = ?;  // e.g., "localhost:9092"

// Simple in-memory map to store payments (like a dictionary). For demo only—use MongoDB later.
map<any> payments = {};  // Key: paymentId, Value: payment details

// Create HTTP listener on port 8083 (pick a free port).
listener http:Listener paymentListener = new http:Listener(8083);

// Create Kafka producer: Sends messages to topics.
kafka:Producer kafkaProducer = check new ({
    bootstrapServers: kafkaBroker,  // Connect to Kafka broker
    clientId: "payment-producer",   // Unique name for this producer
    acknowledgement: "all",         // Wait for all replicas to confirm (reliable)
    retryCount: 3                   // Retry 3 times if send fails
});

// Create Kafka consumer: Listens for incoming messages from topics.
kafka:Consumer kafkaConsumer = check new ({
    bootstrapServers: kafkaBroker,  // Same broker
    groupId: "payment-group",       // Group for this consumer (allows multiple instances)
    offsetReset: "earliest",        // Start from beginning if no offset
    autoCommit: true,               // Auto-save progress
    topics: ["ticket.payment-required"]  // Listen only to this topic
});

// Simple data type for a payment (like a struct in other languages).
type Payment record {|
    string paymentId;     // Unique ID for this payment
    string ticketId;      // Links to the ticket
    decimal amount;       // How much to pay (e.g., 5.50)
    string method;        // "card", "mobile", etc.
    string status;        // "PENDING", "COMPLETED", "FAILED"
    time:Date timestamp;  // When it happened
|};

// Type for incoming Kafka message (what Ticketing Service sends).
type PaymentRequiredEvent record {|
    string ticketId;
    string userId;
    decimal amount;
    string type;  // Ticket type, e.g., "single"
|};

// The main HTTP service (like a web server endpoint).
service /payment on paymentListener {

    // Simple health check: Call GET /payment/ping to see if it's running.
    resource function get ping() returns json {
        // What this does: Returns a simple JSON response to check if service is alive.
        return {
            service: "Payment Service",
            status: "Running",
            time: time:currentTime().toString()
        };
    }

    // Manual payment processing: POST /payment/process (for testing with curl).
    // Body example: {"ticketId": "ticket-123", "amount": 5.50, "method": "card"}
    // Why? Simulates what Kafka does, but you can test without Kafka.
    resource function post process(http:Request req) returns http:Response|error {
        // Step 1: Get JSON from request body.
        json|error payload = req.getJsonPayload();
        if payload is error {
            // If invalid JSON, log error and return 400 (Bad Request).
            log:printError("Bad JSON in request", 'error = payload);
            json errorMsg = {error: "Invalid JSON. Send like: {\"ticketId\":\"123\",\"amount\":5.50}"};
            return new http:Response(400, payload = errorMsg);
        }

        // Step 2: Check required fields (ticketId and amount).
        if !payload.hasKey("ticketId") {
            json errorMsg = {error: "Missing 'ticketId' in JSON"};
            return new http:Response(400, payload = errorMsg);
        }
        if !payload.hasKey("amount") {
            json errorMsg = {error: "Missing 'amount' in JSON"};
            return new http:Response(400, payload = errorMsg);
        }

        // Step 3: Extract data safely.
        string ticketId = check payload.ticketId.ensureType(string);  // Convert to string
        decimal amount = check <decimal> payload.amount;  // Assume it's a number (int or decimal)

        string method = "card";  // Default
        if payload.hasKey("method") {
            method = check payload.method.ensureType(string);
        }

        // Step 4: Simulate processing (in real app, call bank API here).
        // Add a small delay to mimic real time.
        runtime:sleep(1000);  // 1 second delay

        // Always succeed for demo (50% chance of failure in real? Add random check).
        string status = "COMPLETED";  // Or "FAILED" for testing

        // Step 5: Create unique payment ID and save to memory.
        string paymentId = "pay-" + ticketId + "-" + time:currentTime().toString();
        Payment newPayment = {
            paymentId: paymentId,
            ticketId: ticketId,
            amount: amount,
            method: method,
            status: status,
            timestamp: time:currentTime()
        };
        payments[paymentId] = newPayment;  // Store in map

        // Step 6: Build event to send to Kafka (tell other services payment is done).
        json event = {
            ticketId: ticketId,
            paymentId: paymentId,
            amount: amount,
            status: status,
            transactionId: paymentId,  // Fake transaction ID
            timestamp: time:currentTime().toString()
        };

        // Step 7: Send to Kafka topic "payments.processed".
        // Why? Ticketing Service listens here to update ticket status.
        kafka:ProducerMessage message = {
            topic: "payments.processed",
            value: event  // JSON as value
        };
        error? sendResult = kafkaProducer->send(message);
        if sendResult is error {
            // If Kafka fails, log it but still return OK (retry later in real app).
            log:printError("Failed to send to Kafka", 'error = sendResult);
            newPayment.status = "FAILED";  // Update status
            payments[paymentId] = newPayment;
            json resp = {error: "Payment saved, but Kafka notify failed"};
            return new http:Response(500, payload = resp);
        }

        // Step 8: Success! Return response.
        log:printInfo("Payment completed: " + paymentId);
        json resp = {
            message: "Payment processed successfully",
            paymentId: paymentId,
            ticketId: ticketId,
            status: status
        };
        return new http:Response(200, payload = resp);
    }

    // GET /payment/{paymentId} - Fetch a payment by ID (for checking).
    resource function get id(string paymentId) returns http:Response|error {
        // Step 1: Check if payment exists in memory.
        if payments.hasKey(paymentId) {
            // Found: Return it as JSON.
            return new http:Response(200, payload = payments.get(paymentId));
        } else {
            // Not found: Return 404.
            json errorMsg = {error: "Payment not found with ID: " + paymentId};
            return new http:Response(404, payload = errorMsg);
        }
    }
}

// Kafka Consumer Functions (runs in background).
// Why separate? Ballerina runs these when messages arrive—no need for loops.

// Function: Called when a message arrives on "ticket.payment-required".
// What it does: Simulates payment automatically (event-driven flow).
isolated function processPaymentRequired(kafka:Consumer consumer, kafka:Message message) returns error? {
    // Step 1: Get JSON from Kafka message.
    json|error payload = message.getPayloadAsJson();
    if payload is error {
        log:printError("Bad JSON from Kafka", 'error = payload);
        return payload;  // Stop if invalid
    }

    // Step 2: Extract data (similar to HTTP, but from Kafka event).
    string ticketId = check payload.ticketId.ensureType(string);
    decimal amount = check <decimal> payload.amount;  // From event

    string method = "card";  // Default for auto-processing
    string userId = "";  // Optional, from event if needed
    if payload.hasKey("userId") {
        userId = check payload.userId.ensureType(string);
    }

    // Step 3: Simulate processing (same as HTTP).
    runtime:sleep(500);  // Short delay
    string status = "COMPLETED";

    // Step 4: Create and store payment.
    string paymentId = "pay-" + ticketId + "-" + time:currentTime().toString();
    Payment newPayment = {
        paymentId: paymentId,
        ticketId: ticketId,
        amount: amount,
        method: method,
        status: status,
        timestamp: time:currentTime()
    };
    payments[paymentId] = newPayment;

    // Step 5: Send confirmation to Kafka.
    json event = {
        ticketId: ticketId,
        paymentId: paymentId,
        amount: amount,
        status: status,
        transactionId: paymentId,
        timestamp: time:currentTime().toString()
    };

    kafka:ProducerMessage msg = {
        topic: "payments.processed",
        value: event
    };
    error? sendResult = kafkaProducer->send(msg);
    if sendResult is error {
        log:printError("Kafka send failed in consumer", 'error = sendResult);
        newPayment.status = "FAILED";
        payments[paymentId] = newPayment;
    } else {
        log:printInfo("Auto-payment completed via Kafka: " + paymentId);
    }
}

// Initialize: Attach the consumer function when service starts.
public function main() returns error? {
    // Attach: Tell Kafka to call our function when messages arrive.
    check kafkaConsumer->attach(processPaymentRequired, "ticket.payment-required");
    
    // Start consumer (runs in background).
    check kafkaConsumer->start();
    
    // Start HTTP server.
    log:printInfo("Payment Service starting on http://localhost:8083");
    check paymentListener.'start();
}

// Optional: Graceful shutdown (Ballerina handles most of it).
public function 'stop() returns error? {
    check kafkaProducer->close();
    check kafkaConsumer->close();
    check paymentListener.gracefulStop(10);  // Wait 10s for ongoing requests
}