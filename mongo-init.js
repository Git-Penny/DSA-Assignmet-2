db = db.getSiblingDB('ticketing');

db.createCollection('users');
db.createCollection('tickets');
db.createCollection('routes');

db.users.createIndex({ "email": 1 }, { unique: true });
db.tickets.createIndex({ "userId": 1 });
db.tickets.createIndex({ "status": 1 });
db.tickets.createIndex({ "expiry": 1 });
db.routes.createIndex({ "routeId": 1 }, { unique: true });

db.routes.insertMany([
    {
        routeId: "R001",
        name: "Downtown Express",
        from: "Central Station",
        to: "Business District",
        fare: 2.50,
        active: true
    },
    {
        routeId: "R002", 
        name: "Airport Shuttle",
        from: "City Center",
        to: "International Airport",
        fare: 5.00,
        active: true
    },
    {
        routeId: "R003",
        name: "University Line",
        from: "Student Union",
        to: "Campus East",
        fare: 1.50,
        active: true
    },
    {
        routeId: "R004",
        name: "Night Bus",
        from: "Entertainment District", 
        to: "Residential Area",
        fare: 3.00,
        active: true
    }
]);

print("MongoDB initialization completed successfully!");
