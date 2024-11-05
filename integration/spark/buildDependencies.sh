#!/bin/bash
set -x -e
(cd ../../client/java && ./gradlew clean publishToMavenLocal)
(cd ../spark-extension-interfaces && ./gradlew clean publishToMavenLocal)
(cd ../sql/iface-java && ./script/compile.sh && ./script/build.sh)
