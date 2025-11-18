FROM gradle:8.5-jdk21 as gradle

RUN apt-get update && \
    apt-get install -y git && \
    git clone https://github.com/ducanh2110/kcr.git && \
    cd kcr && \
    git checkout migrate/kotlin-to-java21 && \
    gradle clean build --no-daemon

FROM eclipse-temurin:21-jdk-alpine

COPY --from=gradle /home/gradle/kcr/build/libs/kcr-all.jar /usr/app/kcr-all.jar

WORKDIR /usr/app

ENTRYPOINT ["java", "-jar", "kcr-all.jar"]