FROM eclipse-temurin:11 AS base
WORKDIR /usr/src/app
COPY gradle gradle
COPY gradle.properties gradle.properties
COPY gradlew gradlew
COPY settings.gradle settings.gradle
RUN ./gradlew --version

FROM base AS build
WORKDIR /usr/src/app
COPY src ./src
COPY build.gradle build.gradle
RUN ./gradlew --no-daemon shadowJar

FROM eclipse-temurin:11
WORKDIR /usr/src/app
COPY --from=build /usr/src/app/build/libs/openlineage-proxy-*.jar /usr/src/app
COPY proxy.dev.yml proxy.dev.yml
COPY docker/entrypoint.sh entrypoint.sh
EXPOSE 5000 5001
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]