#!/bin/bash

# This script generates an expected C header that our Rust API
# should adhere to.

BASEDIR=$(dirname $BASH_SOURCE)
SRC=$BASEDIR/../src
DEPENDENCIES=$BASEDIR/../target/dependency
JAVA_SRC=$SRC/java/io/openlineage/sql
javac -cp "$DEPENDENCIES/*" -h $SRC/jni $JAVA_SRC/*.java
