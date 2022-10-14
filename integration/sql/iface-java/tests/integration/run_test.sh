#!/bin/bash

BASEDIR=$(dirname $BASH_SOURCE)
ROOT=$BASEDIR/../..
JAR=$ROOT/target/openlineage-sql-jar-with-dependencies.jar

rm -f $BASEDIR/*.class
javac -cp $JAR $BASEDIR/TestParser.java
java -cp $JAR:$BASEDIR TestParser "$@"