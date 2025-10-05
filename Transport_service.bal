ballerina
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;

// Configurable values
configurable string mongoUri = ?;
configurable string kafkaBootstrap = ?;

// Data types
type Ticket record {
    string id;
    string userId;
    string routeId;
    string type;
    string status;   // "CREATED", "PAID", "ISSUED"
    time:Date expiry;
    decimal amount;
};

service /ticketing on new http:Listener(7070) {

    private final mongodb:Client mongoClient;
    private final kafka:Consumer kafkaConsumer;
    private final kafka:Producer kafkaProducer;

    function init() returns error? {
        // DB connection
        self.mongoClient = check new (uri = mongoUri);

        // Kafka consumer (ticket.requests)
        self.kafkaConsumer = check new (kafkaBootstrapServers = [kafkaBootstrap],
            groupId = "ticketing-service",
            topics = ["ticket.requests"]);

        // Kafka producer (ticket.events)
        self.kafkaProducer = check new (kafkaBootstrapServers = [kafkaBootstrap]);

        _ = start self.listenForRequests();
        log:printInfo("Ticketing Service started");
    }

    // Background: Listen to requests
    private function listenForRequests() returns error? {
        while true {
            kafka:ConsumerRecord[]|error records = self.kafkaConsumer->poll(1);
            if records is kafka:ConsumerRecord[] {
                foreach var record in records {
                    json|error msg = record.value?.cloneWithType(json);
                    if msg is json {
                        check self.processTicket(msg);
                    }
                }
            }
        }
    }

    // Process new ticket
    private function processTicket(json msg) returns error? {
        string userId = <string>msg.userId;
        string routeId = <string>msg.routeId;
        string type = <string>msg.type;
        decimal amount = <decimal>msg.amount;

        Ticket ticket = {
            id: userId + "-" + time:currentTime().toString(),
            userId: userId,
            routeId: routeId,
            type: type,
            status: "PAID",  // simplified: auto-paid
            expiry: time:currentTime().addDays(1),
            amount: amount
        };

        check self.mongoClient->insert(ticket, "tickets");
        log:printInfo("Ticket finalized for user " + userId);

        // Publish event to Kafka (ticket.events)
        json event = {ticketId: ticket.id, userId: ticket.userId, status: ticket.status};
        kafka:ProducerMessage message = {topic: "ticket.events", value: event};
        check self.kafkaProducer->send(message);
    }

    // HTTP: check ticket by id
    resource function get tickets/[string id]() returns json|http:NotFound {
        mongodb:QueryDocument filter = {id: id};
        Ticket[]|error tickets = self.mongoClient->find(filter, Ticket, "tickets");
        if tickets is Ticket[] && tickets.length() > 0 {
            return {ticket: tickets[0]};
        }
        return <http:NotFound>{body: {error: "Ticket not found"}};
    }
}
