#!/bin/sh
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Run the CI-shaped Gradle commands of the Spark integration locally. Used by
# integration/spark/Taskfile.yml; run from integration/spark.
#
# Usage:
#   spark-gradle.sh build <scala>
#   spark-gradle.sh build --matrix                     # rows "<java> <spark> <scala>" on stdin
#   spark-gradle.sh <test|integration-test> <java> <spark> <scala> [tests [module]]
#   spark-gradle.sh <test|integration-test> --matrix [tests [module]]
#
# Modes:
#   build             as the CI build step, which depends on the Scala version only:
#                     javadoc sourceJar shadowJar, then publishToMavenLocal (this
#                     writes to ~/.m2). With --matrix, runs once per distinct Scala
#                     version in the rows.
#   test              build check, or "test --tests <tests>" when tests is set. Set
#                     module (e.g. :app) to scope the filter to that module: Gradle
#                     fails every module that has no matching test.
#   integration-test  integrationTest -x test [--tests <tests>]
#
# JDK roles (resolved with an explicit version, so an exported JAVA_VERSION
# cannot change them):
#   compile JDK  always 17 (-Pjava.compile.home)
#   test JDK     <java> (-Papp.java.test.home)
#   Gradle JVM   17 for Java 21, else the test JDK, as in CI, which keeps the
#                Gradle build tools on 17 and launches only test JVMs on 21.
#                The build step always runs Gradle on 17.

set -eu

mode="${1:-}"
case "$mode" in
  build | test | integration-test) shift ;;
  *)
    echo "usage: spark-gradle.sh build (<scala> | --matrix)" >&2
    echo "       spark-gradle.sh <test|integration-test> (<java> <spark> <scala> [tests [module]] | --matrix [tests [module]])" >&2
    exit 2
    ;;
esac

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
# --strict: fail instead of silently using a JDK of another version.
java_home_for() { "$root_dir/dev/java-home.sh" --strict "$1"; }

# gradle <java_home> <progress> <args...>
gradle() {
  gradle_home="$1"
  progress="$2"
  shift 2
  printf '\033[1;34m%sJAVA_HOME=%s ./gradlew %s\033[0m\n' "$progress" "$gradle_home" "$*"
  JAVA_HOME="$gradle_home" ./gradlew "$@" </dev/null
}

# build_one <scala> <progress>
build_one() {
  scala="$1"
  progress="$2"
  compile_home="$(java_home_for 17)"
  gradle "$compile_home" "$progress" --console=plain javadoc sourceJar shadowJar \
    -Pscala.binary.version="$scala" -Pjava.compile.home="$compile_home"
  gradle "$compile_home" "$progress" --console=plain publishToMavenLocal \
    -Pscala.binary.version="$scala" -Pjava.compile.home="$compile_home"
}

# run_one <java> <spark> <scala> <tests> <module> <progress>
run_one() {
  java="$1"
  spark="$2"
  scala="$3"
  tests="$4"
  module="${5:-}"
  progress="$6"

  compile_home="$(java_home_for 17)"
  test_home="$(java_home_for "$java")"
  if [ "$java" = "21" ]; then gradle_home="$compile_home"; else gradle_home="$test_home"; fi

  set -- --console=plain --no-daemon
  case "$mode" in
    test)
      set -- "$@" --stacktrace
      if [ -z "$tests" ]; then
        set -- "$@" build check
      elif [ -n "$module" ]; then
        set -- "$@" "$module:test" --tests "$tests"
      else
        set -- "$@" test --tests "$tests"
      fi
      ;;
    integration-test)
      set -- "$@" integrationTest -x test
      if [ -n "$tests" ]; then set -- "$@" --tests "$tests"; fi
      ;;
  esac
  set -- "$@" -Pspark.version="$spark" -Pscala.binary.version="$scala" -Pjava.compile.home="$compile_home" \
    -Papp.java.test.home="$test_home" -Pspark.docker.java.version="$java"
  gradle "$gradle_home" "$progress" "$@"
}

if [ "${1:-}" = "--matrix" ]; then
  tests="${2:-}"
  module="${3:-}"
  matrix="$(sed '/^[[:space:]]*$/d')"
  if [ "$mode" = "build" ]; then
    # The build step depends on the Scala version only, as in CI.
    matrix="$(printf '%s\n' "$matrix" | awk '!seen[$3]++')"
  fi
  total="$(printf '%s\n' "$matrix" | wc -l | tr -d ' ')"
  idx=0
  # Iterate over a here-doc, not a pipe, so the loop stays in this shell.
  while read -r java spark scala; do
    idx=$((idx + 1))
    if [ "$mode" = "build" ]; then
      build_one "$scala" "[$idx/$total] "
    else
      run_one "$java" "$spark" "$scala" "$tests" "$module" "[$idx/$total] "
    fi
  done <<EOM
$matrix
EOM
elif [ "$mode" = "build" ]; then
  [ "$#" -ge 1 ] || { echo "spark-gradle.sh: <scala> required" >&2; exit 2; }
  build_one "$1" ""
else
  [ "$#" -ge 3 ] || { echo "spark-gradle.sh: <java> <spark> <scala> required" >&2; exit 2; }
  run_one "$1" "$2" "$3" "${4:-}" "${5:-}" ""
fi
