FROM gradle:8.4.0-jdk21-alpine AS build
LABEL authors="Senko-san"
LABEL authors="AgniaEndie"
LABEL authors="GekkStr"

WORKDIR /marsbridge
COPY . /marsbridge
RUN gradle bootJar
ENTRYPOINT ["java","-XX:+UseZGC", "-jar", "/marsbridge/build/libs/marsbridge-0.0.1-SNAPSHOT.jar"]
