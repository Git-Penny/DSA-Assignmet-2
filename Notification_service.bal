ballerina
import ballerina/log;
import ballerinax/kafka;

// Configurable values
configurable string kafkaBootstrap = ?;

service /notification on new kafka:Listener(kafkaBootstrap, "ticket.events") {

    resource function onMessage(kafka:ConsumerRecord[] records) {
        foreach var record in records {
            json|error msg = record.value?.cloneWithType(json);
            if msg is json {
                string ticketId = <string>msg.ticketId;
                string userId = <string>msg.userId;
                string status = <string>msg.status;

                // For demo: log notification
                log:printInfo(" Notification: Ticket " + ticketId +
                    " for user " + userId + " is now " + status);
            }
        }
    }
}
