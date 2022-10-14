#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

BASEDIR=$(dirname $BASH_SOURCE)
ROOT=$BASEDIR/..
SRC=$ROOT/src
JAVA_SRC=$SRC/java/io/openlineage/sql
RESOURCES=$ROOT/resources/io/openlineage/sql
SCRIPTS=$ROOT/scripts

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java.so
elif [[ "$OSTYPE" == "darwin"* ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java.dylib
else
    printf "\n${RED}Unsupported OS!\n${NC}"
fi

# Build the Rust bindings
cd $ROOT/..
cargo build -p openlineage_sql_java

cd iface-java

# Let's generate this header every run so that it is always
# up to date.
mvn dependency:copy-dependencies
./$SCRIPTS/generate_jni_header.sh

# Package into jar
mkdir -p $RESOURCES
cp target/debug/$NATIVE_LIB_NAME $RESOURCES
mvn assembly:assembly -Dmaven.test.skip

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
