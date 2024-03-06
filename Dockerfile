FROM gradle:8.4.0-jdk21-alpine AS build
LABEL authors="Senko-san"
LABEL authors="AgniaEndie"
LABEL authors="GekkStr"

WORKDIR /marspusher
COPY . /marspusher
RUN gradle bootJar
ENTRYPOINT ["java","-XX:+UseZGC","-Dotel.instrumentation.tomcat.enabled=false", "-jar", "/marspusher/build/libs/marspusher-0.0.1-SNAPSHOT.jar"]
