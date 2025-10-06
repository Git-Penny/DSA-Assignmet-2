# Use the official Ballerina runtime image
FROM ballerina/ballerina:latest

# Set working directory
WORKDIR /home/ballerina

# Copy project files into the container
COPY . .

# Expose the service port
EXPOSE 8081

# Run the Ballerina service with the Config.toml file
CMD ["bal", "run",]
