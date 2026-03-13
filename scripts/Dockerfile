# ---- Build stage: create the single Spring Boot JAR ----
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Copy Project, using .dockerignore to exclude files not needed for build
COPY . .

RUN chmod +x mvnw

RUN ./mvnw -B -DskipTests clean package 

# ---- Runtime stage: production container ----
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN addgroup --system spring && adduser --system --ingroup spring spring

# If there is exactly one bootable JAR, this wildcard is safe
COPY --from=build --chown=spring:spring /app/target/*.jar app.jar

EXPOSE 8080
USER spring:spring

ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport"

ENTRYPOINT ["java", "-jar", "app.jar"]