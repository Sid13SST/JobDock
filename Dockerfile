# Runtime image for JobDock. Build the jar on the host first (tests need Docker, so they are
# not run inside this build):
#   ./mvnw -B -DskipTests package
#   docker compose build
FROM eclipse-temurin:21.0.13_9-jre-noble

RUN groupadd --system jobdock && useradd --system --gid jobdock --no-create-home jobdock
WORKDIR /app
COPY target/jobdock.jar /app/jobdock.jar
USER jobdock

EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/jobdock.jar"]
