FROM openjdk:11-jdk
WORKDIR /root
COPY target/kafka_client_delay-0.0.1-SNAPSHOT.jar /root/kafka_client_delay.jar
COPY src/main/resources/application.properties /root/application.properties
ENTRYPOINT ["java", "-Xmx512m", "-Xms512m", "-jar", "/root/kafka_client_delay.jar", "-Dspring.config.location=./application.properties"]
