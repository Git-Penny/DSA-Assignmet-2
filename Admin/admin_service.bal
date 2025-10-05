import ballerina/http;
import ballerina/kafka;
import ballerina/io;
import ballerina/time;

// --- Data Models ---

// Represents a service disruption or schedule update event to be published to Kafka.
public type DisruptionUpdate record {
    // e.g., "DISRUPTION", "SCHEDULE_UPDATE", "MAINTENANCE"
    string 'type; 
    string routeId;
    string message;
    // Automatically sets the timestamp when the event is created.
    time:Utc timestamp = time:utcNow(); 
};

// Represents a route managed by the system.
public type Route record {
    string routeId;
    string name;
    string 'from;
    string to;
};

// --- Configuration ---
// These values can be configured via environment variables (e.g., KAFKA_BOOTSTRAP_SERVERS)
configurable string KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
configurable string KAFKA_TOPIC = "service_updates";
configurable int ADMIN_SERVICE_PORT = 9090;

// --- Kafka Producer Client Setup ---
// Initializes the producer. We use AS_STRING for the key (routeId) and 
// AS_JSON for the value (DisruptionUpdate record) which Ballerina handles automatically.
kafka:Producer kafkaProducer = check new (KAFKA_BOOTSTRAP_SERVERS, {
    keySerializer: kafka:AS_STRING,
    valueSerializer: kafka:AS_JSON
});

// --- HTTP Admin Service ---
// Service listens on the configured port and handles admin functions.
service /admin on new http:Listener(ADMIN_SERVICE_PORT) {

    // 1. Manage Routes and Trips (Mock storage)
    // Example: POST /admin/routes with a Route payload to add a new route.
    resource function post routes(@http:Payload Route newRoute) returns http:Created|error {
        // In a production system, this would persist the route data to a database.
        io:println("Admin Action: Route added successfully: " + newRoute.routeId + " (" + newRoute.name + ")");
        return http:CREATED;
    }

    // 2. View Simple Reports (Mock data)
    // Example: GET /admin/reports
    resource function get reports() returns json {
        // Generates a mock daily operational report.
        io:println("Admin Action: Generating simple report.");
        json report = {
            "title": "Daily Service Summary Report",
            "date": time:utcNow().toString(),
            "metrics": {
                "activeRoutes": 42,
                "totalTripsRunToday": 1250,
                "disruptionsReportedToday": 3,
                "kafkaBootstrapServers": KAFKA_BOOTSTRAP_SERVERS
            }
        };
        return report;
    }

    // 3. Publish Service Disruptions or Updates (via Kafka)
    // Example: POST /admin/disruption with a DisruptionUpdate payload
    resource function post disruption(@http:Payload DisruptionUpdate update) returns http:Accepted|error {
        
        // The routeId is used as the Kafka message key, which is important for
        // consumer partitioning and message ordering related to that specific route.
        string key = update.routeId;

        // Produce the message to the configured Kafka topic.
        var result = kafkaProducer->send({
            key: key,
            value: update
        }, KAFKA_TOPIC, 0.0); // 0.0 is the timeout
        
        if (result is error) {
            io:println("Admin Error: Failed to publish disruption for route " + update.routeId + ": " + result.message());
            return result;
        }

        io:println("Admin Success: Published '" + update.'type + "' update for route " + update.routeId + " to Kafka topic: " + KAFKA_TOPIC);
        return http:ACCEPTED;
    }
}
