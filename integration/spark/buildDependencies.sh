#!/bin/bash
set -x -e
(cd ../../client/java && ./gradlew publishToMavenLocal)
(cd ../spark-extension-interfaces && ./gradlew publishToMavenLocal)
(cd ../sql/iface-java && ./script/compile.sh && ./script/build.sh)
