# ── Stage 1: Build fat JAR ────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build

# Cache dependencies separately from source code
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ── Stage 2: Minimal runtime ──────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine
RUN apk add --no-cache libstdc++ libgcc
WORKDIR /app
COPY --from=builder /build/target/customer-profile-streams-1.0.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
