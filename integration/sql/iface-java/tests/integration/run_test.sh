#!/bin/bash

JAR=target/openlineage-sql.jar

rm -f tests/integration/*.class
javac -cp $JAR tests/integration/TestParser.java
java -cp $JAR:tests/integration TestParser "$@"