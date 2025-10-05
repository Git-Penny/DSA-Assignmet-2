import ballerina/http;
import ballerina/jwt;
import ballerina/crypto;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;

// Configurable values from Config.toml
configurable string mongoUri = ?;
configurable string kafkaBootstrap = ?;
configurable string transportServiceUrl = ?;
configurable string jwtSecret = ?;

// Data types
type User record {
    string id;
    string email;
    string passwordHash;
    decimal balance;
};

type Ticket record {
    string id;
    string userId;
    string routeId;
    string type;  // "single", "multi", "pass"
    string status;  // "CREATED", "PAID", etc.
    time:Date expiry;
    // Add more fields as needed
};

type LoginRequest record {
    string email;
    string password;
};

type RegisterRequest record {
    string email;
    string password;
    decimal initialBalance;
};

type TicketRequest record {
    string routeId;
    string type;
    decimal amount;
};

type Profile record {
    string id;
    string email;
    decimal balance;
};

// JWT payload for auth
type JWTUser Payload record {
    string sub;  // userId
    string iss;  // issuer
    int exp;     // expiry
    string email;
};

// HTTP listener for Passenger Service
service /passenger on new http:Listener(8080) {

    private final mongodb:Client mongoClient;
    private final kafka:Producer kafkaProducer;
    private final http:Client transportClient;

    function init() returns error? {
        // Initialize MongoDB client
        self.mongoClient = check new (uri = mongoUri);

        // Initialize Kafka producer
        self.kafkaProducer = check new (kafkaBootstrapServers = [kafkaBootstrap]);

        // Initialize HTTP client for Transport Service
        self.transportClient = check new (transportServiceUrl);

        log:printInfo("Passenger Service initialized successfully");
    }

    // POST /passenger/register - User registration
    resource function post register(http:Request req) returns json|http:Unauthorized|http:InternalServerError {
        RegisterRequest|error payload = req.getJsonPayload();
        if payload is error {
            return <http:BadRequest>{body: {error: "Invalid registration data"}};
        }

        // Check if user already exists
        mongodb:QueryDocument filter = {email: payload.email};
        User[]|error existingUsers = self.mongoClient->find(filter, User, "users");
        if existingUsers is User[] && existingUsers.length() > 0 {
            return <http:Conflict>{body: {error: "User  already exists"}};
        }

        // Hash password
        string hashedPassword = check self.hashPassword(payload.password);

        // Create new user
        User newUser  = {
            id: payload.email,  // Use email as ID for simplicity
            email: payload.email,
            passwordHash: hashedPassword,
            balance: payload.initialBalance ?: 0.0
        };

        // Insert to MongoDB
        mongodb:InsertionResult|error result = self.mongoClient->insert(newUser , "users");
        if result is error {
            log:printError("Failed to register user", result);
            return <http:InternalServerError>{body: {error: "Registration failed"}};
        }

        return {
            status: "registered",
            userId: newUser .id,
            message: "Account created successfully"
        };
    }

    // POST /passenger/login - User login with JWT
    resource function post login(http:Request req) returns json|http:Unauthorized|http:BadRequest {
        LoginRequest|error payload = req.getJsonPayload();
        if payload is error {
            return <http:BadRequest>{body: {error: "Invalid login data"}};
        }

        // Find user by email
        mongodb:QueryDocument filter = {email: payload.email};
        User[]|error users = self.mongoClient->find(filter, User, "users");
        if users is error || (users is User[] && users.length() == 0) {
            return <http:Unauthorized>{body: {error: "Invalid credentials"}};
        }

        User user = users[0];

        // Verify password
        boolean validPassword = check self.verifyPassword(payload.password, user.passwordHash);
        if !validPassword {
            return <http:Unauthorized>{body: {error: "Invalid credentials"}};
        }

        // Generate JWT
        JWTUser Payload jwtPayload = {
            sub: user.id,
            iss: "passenger-service",
            exp: check time:currentTime().plusSeconds(3600).getTime(),  // 1 hour expiry
            email: user.email
        };
        string|error jwtToken = jwt:issue(jwtPayload, {
            keyId: "passenger-jwt",
            keyStore: {
                keyAlias: "passenger",
                keyPassword: jwtSecret,
                keyStorePassword: jwtSecret
            }
        });
        if jwtToken is error {
            log:printError("Failed to generate JWT", jwtToken);
            return <http:InternalServerError>{body: {error: "Login failed"}};
        }

        return {
            status: "logged in",
            token: jwtToken,
            user: {
                id: user.id,
                email: user.email,
                balance: user.balance
            }
        };
    }

    // GET /passenger/routes - Browse available routes (calls Transport Service)
    resource function get routes() returns json|http:InternalServerError {
        // Call Transport Service
        var response = self.transportClient->get("/routes");
        if response is http:Response {
            if response.statusCode == 200 {
                json|error payload = response.getJsonPayload();
                if payload is json {
                    return payload;
                }
            }
            return <http:InternalServerError>{body: {error: "Failed to fetch routes"}};
        } else {
            log:printError("Transport Service unavailable", response);
            return <http:InternalServerError>{body: {error: "Transport Service unavailable"}};
        }
    }

    // GET /passenger/tickets - View user's tickets (requires auth)
    resource function get tickets(@http:Header string authorization) returns json|http:Unauthorized|http:InternalServerError {
        string|error userId = self.extractUser IdFromJWT(authorization);
        if userId is error {
            return <http:Unauthorized>{body: {error: "Invalid or missing token"}};
        }

        // Query tickets by userId
        mongodb:QueryDocument filter = {userId: userId};
        Ticket[]|error tickets = self.mongoClient->find(filter, Ticket, "tickets");
        if tickets is error {
            log:printError("Failed to fetch tickets", tickets);
            return <http:InternalServerError>{body: {error: "Failed to fetch tickets"}};
        }

        return {tickets: tickets};
    }

    // POST /passenger/request-ticket - Request a ticket (publishes to Kafka, requires auth)
    resource function post request-ticket(http:Request req, @http:Header string authorization) returns json|http:Unauthorized|http:BadRequest {
        string|error userId = self.extractUser IdFromJWT(authorization);
        if userId is error {
            return <http:Unauthorized>{body: {error: "Invalid or missing token"}};
        }

        TicketRequest|error payload = req.getJsonPayload();
        if payload is error {
            return <http:BadRequest>{body: {error: "Invalid ticket request data"}};
        }

        // Prepare Kafka message (add userId)
        json kafkaPayload = {
            userId: userId,
            routeId: payload.routeId,
            type: payload.type,
            amount: payload.amount,
            timestamp: time:currentTime().toString()
        };

        // Publish to Kafka
        kafka:ProducerMessage message = {
            topic: "ticket.requests",
            value: kafkaPayload
        };
        error? sendResult = self.kafkaProducer->send(message);
        if sendResult is error {
            log:printError("Failed to send ticket request to Kafka", sendResult);
            return <http:InternalServerError>{body: {error: "Failed to request ticket"}};
        }

        return {
            status: "request sent",
            message: "Ticket request published successfully",
            requestId: userId + "-" + time:currentTime().toString()
        };
    }

    // GET /passenger/profile - View profile (requires auth)
    resource function get profile(@http:Header string authorization) returns json|http:Unauthorized|http:InternalServerError {
        string|error userId = self.extractUser IdFromJWT(authorization);
        if userId is error {
            return <http:Unauthorized>{body: {error: "Invalid or missing token"}};
        }

        // Query user
        mongodb:QueryDocument filter = {id: userId};
        User[]|error users = self.mongoClient->find(filter, User, "users");
        if users is error || (users is User[] && users.length() == 0) {
            return <http:InternalServerError>{body: {error: "Profile not found"}};
        }

        User user = users[0];
        Profile profile = {
            id: user.id,
            email: user.email,
            balance: user.balance
        };

        return {profile: profile};
    }

    // POST /passenger/profile - Update profile (e.g., top-up balance; requires auth)
    resource function post profile(http:Request req, @http:Header string authorization) returns json|http:Unauthorized|http:BadRequest|http:InternalServerError {
        string|error userId = self.extractUser IdFromJWT(authorization);
        if userId is error {
            return <http:Unauthorized>{body: {error: "Invalid or missing token"}};
        }

        json|error updatePayload = req.getJsonPayload();
        if updatePayload is error {
            return <http:BadRequest>{body: {error: "Invalid update data"}};
        }

        // Example: Update balance (extend for other fields)
        decimal|error newBalance = <decimal> updatePayload.balance;
        if newBalance is error {
            return <http:BadRequest>{body: {error: "Invalid balance value"}};
        }

        // Atomic update in MongoDB
        mongodb:UpdateDocument updateDoc = {balance: newBalance};
        mongodb:BsonValue filter = {id: userId};
        mongodb:UpdateResult|error result = self.mongoClient->update(filter, updateDoc, "users");
        if result is error {
            log:printError("Failed to update profile", result);
            return <http:InternalServerError>{body: {error: "Update failed"}};
        }

        return {
            status: "updated",
            message: "Profile updated successfully",
            balance: newBalance
        };
    }

    // Helper: Hash password
    private isolated function hashPassword(string password) returns string|error {
        byte[] key = jwtSecret.toBytes();
        byte[] hashed = check crypto:hmacSha256(password.toBytes(), key);
        return hashed.toBase64();
    }

    // Helper: Verify password
    private isolated function verifyPassword(string password, string storedHash) returns boolean|error {
        string hashed = check self.hashPassword(password);
        return hashed == storedHash;
    }

    // Helper: Extract userId from JWT
    private isolated function extractUser IdFromJWT(string authHeader) returns string|error {
        if !authHeader.startsWith("Bearer ") {
            return error("Invalid authorization header");
        }
        string token = authHeader.substring(7);

        jwt:Payload|error payload = check jwt:decode(token, {
            keyStore: {
                keyAlias: "passenger",
                keyPassword: jwtSecret,
                keyStorePassword: jwtSecret
            }
        });

        if payload is jwt:Payload {
            JWTUser Payload userPayload = check payload.getJWTUser Payload();
            return userPayload.sub;
        }
        return error("Invalid JWT token");
    }
}

// Health check endpoint (optional, for monitoring)
service /health on new http:Listener(8080) {
    resource function get .() returns string {
        return "Passenger Service is healthy";
    }
}
