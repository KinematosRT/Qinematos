FROM eclipse-temurin:25-jdk-alpine AS build

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN apk add --no-cache maven && \
    mvn package -DskipTests --enable-preview -B

FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

# Create IPC shared memory directory
RUN mkdir -p /dev/shm/qinematos/contexts && \
    mkdir -p /app/data/xodus

COPY --from=build /app/target/quarkus-app/lib/ /app/lib/
COPY --from=build /app/target/quarkus-app/*.jar /app/
COPY --from=build /app/target/quarkus-app/app/ /app/app/
COPY --from=build /app/target/quarkus-app/quarkus/ /app/quarkus/

EXPOSE 8080 9000

ENV JAVA_OPTS="--enable-preview"

CMD ["java", "--enable-preview", "-jar", "/app/quarkus-run.jar"]
