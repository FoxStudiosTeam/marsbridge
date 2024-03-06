FROM gradle:8.4.0-jdk21-alpine AS build
LABEL authors="Senko-san"
LABEL authors="AgniaEndie"
LABEL authors="GekkStr"
LABEL autors="xxlegendzxx22"
WORKDIR /marsbridge
COPY . /marsbridge
RUN gradle jar
ENTRYPOINT ["java","-XX:+UseZGC", "-jar", "/marsbridge/build/libs/marsbridge-1.0-SNAPSHOT.jar"]
