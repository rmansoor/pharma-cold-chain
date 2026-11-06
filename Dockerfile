FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml maven-settings.xml ./
RUN mvn -s maven-settings.xml -B -q dependency:go-offline
COPY src src
RUN mvn -s maven-settings.xml -B -q package dependency:copy-dependencies -DincludeScope=runtime
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/cold-chain-pipeline-1.0.0.jar /app/pipeline.jar
COPY --from=build /build/target/dependency /app/lib
ENV SPARK_LOCAL_IP=0.0.0.0
ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-Xmx2g", "-cp", "/app/pipeline.jar:/app/lib/*"]
CMD ["io.coldchain.Pipeline"]
