# Use official OpenJDK base image for Java
FROM openjdk:21-jdk-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Maven build output (JAR file) into the container
COPY ./ProducerService-0.0.1-SNAPSHOT.jar /app/ProducerService.jar

# Expose port 8080 for the application
EXPOSE 8080

# Run the JAR file
ENTRYPOINT ["java", "-jar", "ProducerService.jar"]
