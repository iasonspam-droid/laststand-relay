FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY RelayServer.java .
RUN javac RelayServer.java
CMD ["java", "RelayServer"]
