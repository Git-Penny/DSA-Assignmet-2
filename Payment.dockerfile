# Use official Ballerina image
FROM ballerina/ballerina:latest

WORKDIR /home/ballerina

# Copy only TOML first to leverage Docker cache for deps
COPY Ballerina.toml Ballerina.lock* ./

# Try building dependencies (may fail harmlessly if no lock file) - improves caching
RUN bal build --skip-tests || true

# Copy source files
COPY . .

# Build service
RUN bal build --skip-tests

# Expose the container port (Ballerina defaults to 8080 in our code)
EXPOSE 8080

# Default command to run the service
CMD ["bal", "run", "--observability-included"]
# CMD ["bal", "run", "PaymentService.bal", "--observability-included"]  # Alternative: specify the main Ballerina file explicitly