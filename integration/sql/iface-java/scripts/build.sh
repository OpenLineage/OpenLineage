#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

CURRENT_DIR=$(pwd)
SRC=iface-java/src
JAVA_SRC=$SRC/java/io/openlineage/sql
RESOURCES=iface-java/resources/io/openlineage/sql
SCRIPTS=iface-java/scripts
NATIVE_LIB_NAME=libopenlineage_sql_java.so

# Let's generate this header every run so that it is always
# up to date.
./$SCRIPTS/generate_jni_header.sh

# Build the Rust bindings
cargo build -p openlineage_sql_java

# Package into jar
cp $CURRENT_DIR/target/debug/$NATIVE_LIB_NAME $RESOURCES
cd iface-java
mvn package -Dmaven.test.skip

# Run tests
mvn test

# Run a simple integration test
printf "\n------ Running integration tests ------\n"
EXPECTED="{{\"inTables\": [table1], \"outTables\": []}}"
OUTPUT=$(./tests/integration/run_test.sh "SELECT * FROM table1;")
if [ "$OUTPUT" = "$EXPECTED" ]; then
    printf "\n${GREEN}Integration Tests Passed!${NC}\n"
else
    printf "\n${RED}Integration Tests Failed!${NC}\n"
fi
