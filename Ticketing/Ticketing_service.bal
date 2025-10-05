import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;

// Configurable values from Config.toml
configurable string mongoUri = ?;
configurable string kafkaBootstrap = ?;

// Data types
type Ticket record {
    string id;          // Unique ticket ID (e.g., userId-routeId-timestamp)
    string userId;
    string routeId;
    string type;        // "single", "multi", "pass"
    string status;      // "CREATED", "PAID", "VALIDATED", "EXPIRED"
    time:Date expiry;
    int remainingUses;  // For "multi"; 1 for "single", unlimited (-1) for "pass"
    Validation[] validations;  // Array of validations
    time:Date createdAt;
    time:Date updatedAt;
};

type Validation record {
    string vehicleId;
    time:Date timestamp;
};

type ValidationRequest record {
    string vehicleId;
};

type KafkaTicketRequest record {
    string userId;
    string routeId;
    string type;
    decimal amount;  // For reference
    string timestamp;
};

type KafkaPaymentProcessed record {
    string ticketId;
    string status;  // "success" or "failed"
    string transactionId;
};

// HTTP listener for Ticketing Service (port 8081)
service /ticketing on new http:Listener(8081) {

    private final mongodb:Client mongoClient;
    private final kafka:Producer kafkaProducer;
    private final kafka:Consumer kafkaConsumer;

    function init() returns error? {
        // Initialize MongoDB client
        self.mongoClient = check new (uri = mongoUri);

        // Initialize Kafka producer (with retries)
        self.kafkaProducer = check new (kafkaBootstrapServers = [kafkaBootstrap], 
                                        clientId = "ticketing-producer",
                                        acknowledgement = "all",  // Wait for all replicas
                                        retryCount = 3);

        // Initialize Kafka consumer for multiple topics
        self.kafkaConsumer = check new (kafkaBootstrapServers = [kafkaBootstrap],
                                        groupId = "ticketing-group",
                                        offsetReset = "earliest",
                                        autoCommit = true,
                                        topics = ["ticket.requests", "payments.processed"]);

        // Attach consumers
        check self.kafkaConsumer->attach(self.onTicketRequest, "ticket.requests");
        check self.kafkaConsumer->attach(self.onPaymentProcessed, "payments.processed");

        // Start the consumer (in a separate worker or stream; here we use a simple start)
        check self.kafkaConsumer->start();

        // Start expiry checker worker (demo: checks every 5 min)
        worker expiryWorker {
            while true {
                check self.checkAndExpireTickets();
                runtime:sleep(300);  // 5 minutes
            }
        }

        log:printInfo("Ticketing Service initialized successfully");
    }

    // POST /ticketing/validate/{ticketId} - Validate ticket on boarding
    resource function post validate/[string ticketId](http:Request req) returns json|http:BadRequest|http:NotFound|http:InternalServerError {
        ValidationRequest|error payload = req.getJsonPayload();
        if payload is error {
            return <http:BadRequest>{body: {error: "Invalid validation data: vehicleId required"}};
        }

        // Find ticket
        mongodb:QueryDocument filter = {id: ticketId, status: {"$in": ["PAID", "VALIDATED"]}};  // Only validate if paid/validated (not expired)
        Ticket[]|error tickets = self.mongoClient->find(filter, Ticket, "tickets");
        if tickets is error || (tickets is Ticket[] && tickets.length() == 0) {
            return <http:NotFound>{body: {error: "Ticket not found or not eligible for validation"}};
        }

        Ticket ticket = tickets[0];

        // Check expiry
        if ticket.expiry < time:currentTime() {
            // Auto-expire if past expiry
            check self.updateTicketStatus(ticketId, "EXPIRED");
            return <http:BadRequest>{body: {error: "Ticket expired"}};
        }

        // For multi-use: decrement remainingUses
        if ticket.type == "multi" && ticket.remainingUses <= 0 {
            return <http:BadRequest>{body: {error: "No remaining uses"}};
        }

        // Update status and validations
        Validation newValidation = {vehicleId: payload.vehicleId, timestamp: time:currentTime()};
        mongodb:UpdateDocument update = {
            status: "VALIDATED",
            updatedAt: time:currentTime(),
            validations: {"$push": newValidation}
        };

        if ticket.type == "multi" {
            update.remainingUses = {"$inc": -1};
        }

        // Set expiry if first validation (for single/multi)
        if ticket.validations.length() == 0 {
            time:Date expiryTime = self.calculateExpiry(ticket.type);
            update.expiry = expiryTime;
        }

        mongodb:UpdateResult|error result = self.mongoClient->update({id: ticketId}, update, "tickets");
        if result is error {
            log:printError("Failed to validate ticket", result);
            return <http:InternalServerError>{body: {error: "Validation failed"}};
        }

        // Publish validation event
        json eventPayload = {
            ticketId: ticketId,
            vehicleId: payload.vehicleId,
            timestamp: time:currentTime().toString(),
            userId: ticket.userId
        };
        check self.publishEvent("ticket.validated", eventPayload);

        return {
            status: "validated",
            message: "Ticket validated successfully",
            remainingUses: ticket.type == "multi" ? (ticket.remainingUses - 1) : 0,
            expiry: ticket.expiry
        };
    }

    // GET /ticketing/ticket/{id} - Get ticket details
    resource function get ticket/[string ticketId]() returns json|http:NotFound|http:InternalServerError {
        mongodb:QueryDocument filter = {id: ticketId};
        Ticket[]|error ticket = self.mongoClient->find(filter, Ticket, "tickets");
        if ticket is error || (ticket is Ticket[] && ticket.length() == 0) {
            return <http:NotFound>{body: {error: "Ticket not found"}};
        }

        // Check and auto-expire if needed
        if ticket[0].status != "EXPIRED" && ticket[0].expiry < time:currentTime() {
            check self.updateTicketStatus(ticketId, "EXPIRED");
            ticket[0].status = "EXPIRED";
        }

        return {ticket: ticket[0]};
    }

    // POST /ticketing/expire/{ticketId} - Manually expire (for demo)
    resource function post expire/[string ticketId]() returns json|http:NotFound|http:InternalServerError {
        mongodb:QueryDocument filter = {id: ticketId};
        Ticket[]|error tickets = self.mongoClient->find(filter, Ticket, "tickets");
        if tickets is error || (tickets is Ticket[] && tickets.length() == 0) {
            return <http:NotFound>{body: {error: "Ticket not found"}};
        }

        mongodb:UpdateResult|error result = self.updateTicketStatus(ticketId, "EXPIRED");
        if result is error {
            return <http:InternalServerError>{body: {error: "Expiry failed"}};
        }

        return {status: "expired", message: "Ticket expired manually"};
    }

    // Kafka Consumer: Handle ticket requests
    isolated function onTicketRequest(kafka:Consumer consumer, kafka:Message[] messages) returns error? {
        foreach kafka:Message msg in messages {
            json|error payload = msg.getPayloadAsJson();
            if payload is json {
                KafkaTicketRequest|error ticketReq = payload.cloneWithType();
                if ticketReq is KafkaTicketRequest {
                    // Create unique ticket ID
                    string ticketId = ticketReq.userId + "-" + ticketReq.routeId + "-" + ticketReq.timestamp;

                    // Create ticket in CREATED state
                    Ticket newTicket = {
                        id: ticketId,
                        userId: ticketReq.userId,
                        routeId: ticketReq.routeId,
                        type: ticketReq.type,
                        status: "CREATED",
                        remainingUses: ticketReq.type == "multi" ? 5 : (ticketReq.type == "pass" ? -1 : 1),
                        validations: [],
                        createdAt: time:currentTime(),
                        updatedAt: time:currentTime(),
                        expiry: time:currentTime()  // Will be set on validation
                    };

                    // Insert to MongoDB
                    mongodb:InsertionResult|error insertResult = self.mongoClient->insert(newTicket, "tickets");
                    if insertResult is error {
                        log:printError("Failed to create ticket", insertResult);
                        return insertResult;
                    }

                    // Publish to payment-required topic
                    json paymentPayload = {
                        ticketId: ticketId,
                        userId: ticketReq.userId,
                        amount: ticketReq.amount,
                        type: ticketReq.type
                    };
                    check self.publishEvent("ticket.payment-required", paymentPayload);

                    log:printInfo("Ticket created and payment required: " + ticketId);
                }
            }
        }
    }

    // Kafka Consumer: Handle payment processed
    isolated function onPaymentProcessed(kafka:Consumer consumer, kafka:Message[] messages) returns error? {
        foreach kafka:Message msg in messages {
            json|error payload = msg.getPayloadAsJson();
            if payload is json {
                KafkaPaymentProcessed|error payment = payload.cloneWithType();
                if payment is KafkaPaymentProcessed && payment.status == "success" {
                    // Update ticket to PAID
                    mongodb:UpdateResult|error result = self.updateTicketStatus(payment.ticketId, "PAID");
                    if result is error {
                        log:printError("Failed to update ticket to PAID", result);
                        return result;
                    }

                    // Get ticket for notification
                    mongodb:QueryDocument filter = {id: payment.ticketId};
                    Ticket[]|error tickets = self.mongoClient->find(filter, Ticket, "tickets");
                    if tickets is Ticket[] && tickets.length() > 0 {
                        Ticket ticket = tickets[0];
                        // Publish notification
                        json notifPayload = {
                            userId: ticket.userId,
                            messageType: "ticket_purchased",
                            content: "Your ticket for route " + ticket.routeId + " is now paid and ready to use!",
                            ticketId: payment.ticketId
                        };
                        check self.publishEvent("notifications.send", notifPayload);

                        log:printInfo("Ticket paid and notification sent: " + payment.ticketId);
                    }
                } else {
                    // Handle failure: e.g., update to EXPIRED or notify
                    log:printWarn("Payment failed for ticket: " + (payment is KafkaPaymentProcessed ? payment.ticketId : "unknown"));
                }
            }
        }
    }

    // Helper: Publish event to Kafka
    private isolated function publishEvent(string topic, json payload) returns error? {
        kafka:ProducerMessage message = {
            topic: topic,
            value: payload,
            key: payload.ticketId ?: payload.userId  // For partitioning
        };
        error? result = self.kafkaProducer->send(message);
        if result is error {
            log:printError("Failed to publish to Kafka topic " + topic, result);
        }
        return result;
    }

    // Helper: Update ticket status atomically
    private isolated function updateTicketStatus(string ticketId, string newStatus) returns mongodb:UpdateResult|error {
        mongodb:UpdateDocument update = {
            status: newStatus,
            updatedAt: time:currentTime()
        };
        if newStatus == "EXPIRED" {
            update.validations = {"$set": []};  // Clear validations on expiry
        }
        mongodb:BsonValue filter = {id: ticketId};
        return self.mongoClient->update(filter, update, "tickets");
    }

    // Helper: Calculate expiry based on type
    private isolated function calculateExpiry(string ticketType) returns time:Date {
        time:Seconds duration = 0;
        match ticketType {
            "single" => duration = 3600;  // 1 hour
            "multi" => duration = 86400;  // 24 hours
            "pass" => duration = 2592000; // 30 days
            _ => duration = 3600;
        }
        return time:currentTime().plusSeconds(duration);
    }

    // Helper: Check and expire overdue tickets (called periodically)
    private isolated function checkAndExpireTickets() returns error? {
        mongodb:QueryDocument filter = {
            status: {"$in": ["PAID", "VALIDATED"]},
            expiry: {"$lt": time:currentTime()}
        };
        Ticket[]|error expiredTickets = self.mongoClient->find(filter, Ticket, "tickets");
        if expiredTickets is Ticket[] {
            foreach Ticket t in expiredTickets {
                check self.updateTicketStatus(t.id, "EXPIRED");
                log:printInfo("Auto-expired ticket: " + t.id);
            }
        }
        return expiredTickets;
    }
}

// Health check endpoint (optional, for monitoring)
service /health on new http:Listener(8081) {
    resource function get .() returns string {
        return "Ticketing Service is healthy";
    }
}