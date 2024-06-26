FROM openjdk:11
ADD integration/sql /usr/lib/openlineage/integration/sql
ADD client/java /usr/lib/openlineage/client/java
ADD spec /usr/lib/openlineage/spec
VOLUME "/root/.m2" "/root/.gradle" "/root/.cargo" "/var/run/docker.sock"
RUN \
    cd /usr/lib/openlineage/client/java && \
    ./gradlew --no-daemon shadowJar publishToMavenLocal &&  \
    cd /usr/lib/openlineage/integration/sql/iface-java && \
    apt-get update && apt-get -y install build-essential && \
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