@echo off
echo Initializing Kafka Topics for Smart Ticketing System...
echo.

timeout /t 20 /nobreak >nul

set KAFKA_BROKER=localhost:9092

echo Checking Kafka connectivity...
kafka-topics.bat --list --bootstrap-server %KAFKA_BROKER% >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Cannot connect to Kafka at %KAFKA_BROKER%
    echo Please ensure Kafka is running
    pause
    exit /b 1
)

echo Creating topics...

kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic service_updates --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic ticket.requests --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic ticket.payment-required --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic payments.processed --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic ticket.validated --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic ticket.events --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics.bat --create --bootstrap-server %KAFKA_BROKER% --topic notifications.send --partitions 3 --replication-factor 1 --if-not-exists

echo.
echo Kafka Topics Initialization Complete!
echo.
kafka-topics.bat --list --bootstrap-server %KAFKA_BROKER%

pause