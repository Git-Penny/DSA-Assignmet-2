# Public Transport Ticketing System

This project implements a distributed ticketing platform for buses and trains using:
- Ballerina for microservices
- Kafka for event-driven communication
- MongoDB for persistence
- Docker Compose** for orchestration

## Services
1. Passenger Service — register/login and manage accounts
2. Transport Service — manage routes/trips
3. Ticketing Service — handle ticket lifecycle
4. Payment Service — simulate payments
5. Notification Service — send updates
6. Admin Service — manage reports and disruptions

## How to Run
```bash
docker-compose up --build
