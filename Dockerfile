FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml maven-settings.xml ./
RUN mvn -s maven-settings.xml -B -q dependency:go-offline
COPY src src
# Tests run in CI (.github/workflows/ci.yml); the image build only packages.
RUN mvn -s maven-settings.xml -B -q package -DskipTests dependency:copy-dependencies -DincludeScope=runtime
FROM eclipse-temurin:17.0.21_9-jre
WORKDIR /app
COPY --from=build /build/target/cold-chain-pipeline-1.0.0.jar /app/pipeline.jar
COPY --from=build /build/target/dependency /app/lib
ENV SPARK_LOCAL_IP=0.0.0.0
# Runs as an unprivileged user. Mount points it owns become the owners of new named volumes; a lake volume created by an
# earlier, root-run image needs `chown -R 10001:10001` once (runbook).
RUN groupadd -r -g 10001 coldchain && useradd -r -u 10001 -g coldchain -d /app coldchain \
 && mkdir -p /data /cases /audit && chown 10001:10001 /data /cases /audit
USER 10001:10001
ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-Xmx2g", "-cp", "/app/pipeline.jar:/app/lib/*"]
CMD ["io.coldchain.Pipeline"]
