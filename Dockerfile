FROM eclipse-temurin:17-jre-alpine

WORKDIR /opt/Lavalink

# Download Lavalink
RUN wget https://github.com/lavalink-devs/Lavalink/releases/download/4.0.8/Lavalink.jar

# Copy application.yml
COPY application.yml /opt/Lavalink/application.yml

# Expose port
EXPOSE 2333

# Run Lavalink
CMD ["java", "-jar", "Lavalink.jar"]