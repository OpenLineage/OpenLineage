#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

BASEDIR=$(dirname $BASH_SOURCE)
ROOT=$BASEDIR/..
SRC=$ROOT/src
JAVA_SRC=$SRC/java/io/openlineage/sql
RESOURCES=$ROOT/src/main/resources/io/openlineage/sql
SCRIPTS=$ROOT/script

mkdir -p $RESOURCES

# Native lib should be compiled at this point by compile.sh
if [[ -f "$ROOT/../target/debug/libopenlineage_sql_java.so" ]]; then
    cp $ROOT/../target/debug/libopenlineage_sql_java.so $RESOURCES
fi
if [[ "$ROOT/../target/debug/libopenlineage_sql_java.dylib" ]]; then
    cp $ROOT/../target/debug/libopenlineage_sql_java.dylib $RESOURCES
fi

# Let's generate this header every run so that it is always
# up to date.
./gradlew clean getDependencies
./$SCRIPTS/generate_jni_header.sh

# Package into jar
./gradlew shadowJar

# Run a simple integration test
printf "\n------ Running smoke test ------\n"
EXPECTED="{{\"inTables\": [table1], \"outTables\": [], \"columnLineage\": [], \"errors\": []}}"
OUTPUT=$(./src/test/integration/run_test.sh "SELECT * FROM table1;")
if [ "$OUTPUT" = "$EXPECTED" ]; then
    printf "\n${GREEN}Smoke Test Passed!${NC}\n"
else
    printf "\n${RED}Smoke Test Failed!${NC}\n"
    printf "Expected ${EXPECTED}\n Got ${OUTPUT}"
    exit 1
fi
