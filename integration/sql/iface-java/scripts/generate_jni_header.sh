#!/bin/bash

# This script generates an expected C header that our Rust API
# should adhere to.

SRC=iface-java/src
JAVA_SRC=$SRC/java/io/openlineage/sql
javac -h $SRC/jni $JAVA_SRC/*.java
