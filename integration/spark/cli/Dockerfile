FROM amazoncorretto:17-alpine-jdk
ADD integration/sql /usr/lib/openlineage/integration/sql
ADD client/java /usr/lib/openlineage/client/java
ADD integration/spark-extension-entrypoint /usr/lib/openlineage/integration/spark-extension-entrypoint
ADD spec /usr/lib/openlineage/spec
VOLUME "/root/.m2" "/root/.gradle" "/root/.cargo" "/var/run/docker.sock"
RUN apk add --update alpine-sdk
RUN apk --no-cache add curl cargo build-base bash
RUN \
    cd /usr/lib/openlineage/client/java && \
    ./gradlew --no-daemon -x test shadowJar publishToMavenLocal && \
    cd /usr/lib/openlineage/integration/spark-extension-entrypoint && \
    ./gradlew --no-daemon jar publishToMavenLocal &&  \
    cd /usr/lib/openlineage/integration/sql/iface-java && \
    ./script/compile.sh && \
    ./script/build.sh
WORKDIR /usr/lib/openlineage/integration/spark
ENTRYPOINT \
    ./gradlew --no-daemon clean configurableIntegrationTest \
    -Pspark.conf.file=${SPARK_CONF_YML} \
    -Ptest.dir=${TEST_DIR} \
    -Phost.dir=${HOST_DIR} \
    -Pspark.version=${SPARK_VERSION} \
    -Pscala.binary.version=${SCALA_BINARY_VERSION}