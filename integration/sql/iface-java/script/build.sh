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

if [[ -d $ROOT/../target/debug ]]; then
  LIBS=$ROOT/..
elif [[ -d ~/openlineage/integration/sql ]]; then
  LIBS="~/openlineage/integration/sql"
fi

mkdir -p $RESOURCES

echo $LIBS
ls -halt $LIBS/target/debug

# Native lib should be compiled at this point by compile.sh
if [[ -f "$LIBS/target/debug/libopenlineage_sql_java_x86_64.so" ]]; then
    cp $LIBS/target/debug/libopenlineage_sql_java_x86_64.so $RESOURCES
fi
if [[ -f "$LIBS/target/debug/libopenlineage_sql_java_aarch64.so" ]]; then
    cp $LIBS/target/debug/libopenlineage_sql_java_aarch64.so $RESOURCES
fi
if [[ -f "$LIBS/target/debug/libopenlineage_sql_java.dylib" ]]; then
    cp $LIBS/target/debug/libopenlineage_sql_java.dylib $RESOURCES
fi
if [[ -f "$LIBS/target/debug/libopenlineage_sql_java_arm64.dylib" ]]; then
    cp $LIBS/target/debug/libopenlineage_sql_java_arm64.dylib $RESOURCES
fi

ls -halt $RESOURCES

# Let's generate this header every run so that it is always
# up to date.
$ROOT/gradlew clean getDependencies
./$SCRIPTS/generate_jni_header.sh

if [[ -n "${GPG_SIGNING_KEY}" ]]; then
    export ORG_GRADLE_PROJECT_signingKey=$(echo $GPG_SIGNING_KEY | base64 -d)
    export RELEASE_PASSWORD=$(echo $OSSRH_TOKEN_PASSWORD)
    export RELEASE_USERNAME=$(echo $OSSRH_TOKEN_USERNAME)
fi

# Install to maven
$ROOT/gradlew -x javadoc publishToMavenLocal

# Package into jar
$ROOT/gradlew shadowJar

# Run a simple integration test
printf "\n------ Running smoke test ------\n"
EXPECTED="{\"inTables\": [\"table1\"], \"outTables\": [], \"columnLineage\": [], \"errors\": []}"
OUTPUT=$(./src/test/integration/run_test.sh "SELECT * FROM table1;")
if [ "$OUTPUT" = "$EXPECTED" ]; then
    printf "\n${GREEN}Smoke Test Passed!${NC}\n"
else
    printf "\n${RED}Smoke Test Failed!${NC}\n"
    printf "Expected ${EXPECTED}\n Got ${OUTPUT}"
    exit 1
fi
