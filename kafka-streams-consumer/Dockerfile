FROM maven:3.9.8-eclipse-temurin-17 AS builder

WORKDIR /build

COPY kafka-streams-consumer/pom.xml /build/pom.xml
COPY kafka-streams-consumer/src /build/src

RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /build/target/kafka-streams-consumer-1.0.0.jar /app/kafka-streams-consumer.jar

CMD ["java", "-jar", "/app/kafka-streams-consumer.jar"]
