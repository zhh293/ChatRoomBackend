FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre-jammy AS runtime

ARG APP_USER=chatroom
ARG APP_UID=10001
ARG APP_GID=10001

RUN groupadd --gid "${APP_GID}" "${APP_USER}" \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /usr/sbin/nologin "${APP_USER}" \
    && mkdir -p /app /data/chatroom-upload /data/chatroom-storage /data/applogs/xxl-job \
    && chown -R "${APP_UID}:${APP_GID}" /app /data

COPY --from=builder --chown=${APP_UID}:${APP_GID} \
    /workspace/target/chatroom-backend-1.0.0-SNAPSHOT.jar /app/app.jar

USER ${APP_UID}:${APP_GID}
WORKDIR /app

ENV TZ=Asia/Shanghai \
    JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Dfile.encoding=UTF-8"

EXPOSE 8080 9090 9999

HEALTHCHECK --interval=20s --timeout=3s --start-period=45s --retries=5 \
    CMD ["bash", "-c", ": >/dev/tcp/127.0.0.1/8080"]

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
