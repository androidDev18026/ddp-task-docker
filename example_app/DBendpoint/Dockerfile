FROM maven:latest AS builder
COPY src /opt/src
COPY pom.xml /opt
WORKDIR /opt
RUN mvn -f /opt/pom.xml clean compile assembly:single

FROM openjdk:11
COPY --from=builder /opt/target/*.jar /opt/assets/job.jar
WORKDIR /opt/assets
#EXPOSE 1000
CMD java -jar -DlogLevel=CONFIG job.jar