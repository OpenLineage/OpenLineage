# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Java client and transports-nats, built with network access. The warm-up run fills the Gradle
# cache so dev/nats-offline/run.sh can repeat the tests with --offline on a network without
# internet access.
FROM docker.io/library/nats:2 AS nats

FROM docker.io/library/eclipse-temurin:17-jdk
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=nats /nats-server /usr/local/bin/nats-server
COPY spec /src/spec
COPY client/java /src/client/java
WORKDIR /src/client/java
# Test results do not matter here, only that every dependency and plugin gets resolved
RUN ./gradlew --no-daemon --console=plain --continue \
      :transports-nats:test :transports-nats:shadowJarTest > /tmp/warmup.log 2>&1; \
    tail -5 /tmp/warmup.log
CMD ["./gradlew", "--offline", "--no-daemon", "--console=plain", "--continue", \
     ":transports-nats:test", "--rerun", ":transports-nats:shadowJarTest", "--rerun"]
