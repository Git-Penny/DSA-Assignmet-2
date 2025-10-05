# Base Ballerina image
FROM ballerina/ballerina:latest

WORKDIR /home/ballerina

# Copy dependency files first for caching
COPY Ballerina.toml Ballerina.lock* ./

RUN bal build --skip-tests || true

# Copy all source code
COPY . .

# Build service
RUN bal build --skip-tests

EXPOSE 8080

CMD ["bal", "run"]
