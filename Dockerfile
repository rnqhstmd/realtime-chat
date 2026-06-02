# --- build stage ---
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace
# 의존성 레이어 캐시: 빌드 스크립트/wrapper 먼저 복사
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies > /dev/null 2>&1 || true
# 소스 복사 후 bootJar(테스트는 Testcontainers라 컨테이너 빌드에서 제외)
COPY src ./src
RUN ./gradlew --no-daemon clean bootJar -x test

# --- runtime stage ---
FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
