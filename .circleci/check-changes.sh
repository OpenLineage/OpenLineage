#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Based on the changed files (or if we are on the main branch), generate a yaml file with
# a list of workflow files. Use those files to build a workflow with a union of all tasks

# Print each command before executing it
set -x

# Set default values if not provided
NIGHTLY_RUN=${NIGHTLY_RUN:-"inactive"}
INTEGRATION_TYPE=${INTEGRATION_TYPE:-"*"}

function check_change () {
    MOD=$1
    shift
    EXCLUDE_MD=$1
    shift

    if [ "$EXCLUDE_MD" == "true" ]; then
        CHANGED_FILES=$(git diff --name-only main "$MOD" | grep -v ".*.md")
    else
        CHANGED_FILES=$(git diff --name-only main "$MOD")
    fi

    if [ -n "$CHANGED_FILES" ]; then
        echo "Change found in $MOD"
        if [ "$1" == "*" ]; then
            ls -d "$PWD/.circleci/workflows/"* > workflow_files.txt
        else
            for ln in "$@"; do
                echo "$PWD/.circleci/workflows/$ln" >> workflow_files.txt
            done
        fi
    fi
}

# Add always workflow
echo "$PWD/.circleci/workflows/openlineage-always.yml" >> workflow_files.txt

# Nightly build - add workflows to be included
if [ "$NIGHTLY_RUN" == "active" ]; then
    # run only Spark within nightly build
    echo "$PWD/.circleci/workflows/openlineage-spark.yml" >> workflow_files.txt
    echo "$PWD/.circleci/workflows/openlineage-java.yml" >> workflow_files.txt
elif [ -n "$CIRCLE_TAG" ]; then
    # If we are on tag, run all of the workflows
    ls -d "$PWD"/.circleci/workflows/* > workflow_files.txt
elif [ "$CIRCLE_BRANCH" == "main" ]; then
    # If we are on the main branch, run all of the workflows
    # if integration type is not all, we specify only a single integration type in workflow files
    if [ "$INTEGRATION_TYPE" != "*" ]; then
        ls -d "$PWD"/.circleci/workflows/openlineage-"$INTEGRATION_TYPE".yml > workflow_files.txt
    else
        ls -d "$PWD"/.circleci/workflows/* > workflow_files.txt
    fi
else
    # Changes to the spec require all workflows to run
    check_change spec true "*"
    check_change .circleci true "*"
    check_change integration/sql/ true "*"

    check_change client/java/ true openlineage-java.yml openlineage-flink.yml openlineage-spark.yml
    check_change integration/spark/ true openlineage-java.yml openlineage-spark.yml
    check_change integration/spark-extension-interfaces/ true openlineage-java.yml openlineage-spark.yml
    check_change integration/flink/ true openlineage-java.yml openlineage-flink.yml
    check_change client/python/ true openlineage-python.yml
    check_change integration/common/ true openlineage-python.yml
    check_change integration/airflow/ true openlineage-python.yml
    check_change integration/dagster/ true openlineage-python.yml
    check_change integration/dbt/ true openlineage-python.yml
    check_change proxy/backend/ true openlineage-proxy-backend.yml
    check_change proxy/fluentd/ true openlineage-proxy-fluentd.yml
    check_change website false openlineage-website.yml
    check_change integration/hive true openlineage-java.yml openlineage-hive.yml
fi
touch workflow_files.txt
FILES=$(sort workflow_files.txt | uniq | tr "\n" " ")

# yq eval-all the workflow files specified in the workflow_files.txt file.
# Collect all the jobs from each workflow except for the "workflow_complete" job and
# create a union of all jobs.
# Collect the "workflow_complete" job from each workflow and concatenate the "requires"
# section of each and create a single "workflow_complete" job that is the union of all.
# The output of this is a circleci configuration with a single workflow called "build"
# that contains the union of all jobs plus the "workflow_complete" job that depends on
# all required jobs.
#
# This configuration is piped into yq along with the continue_config.yml file and the
# union of the two files is output to complete_config.yml

# shellcheck disable=SC2016,SC2086
yq eval-all '.workflows | . as $wf ireduce({}; . * $wf) | to_entries |
    .[] |= (
        with(select(.key == "openlineage-always"); .) |
        with(select(.key != "openlineage-always"); .value.jobs |= map(select(.[].requires == null) |= .[].requires = ["always_run"]))
        ) | from_entries |
    ((map(.jobs[] | select(has("workflow_complete") | not)) | . as $item ireduce ([]; (. *+ $item) ))
    + [(map(.jobs[] | select(has("workflow_complete"))) | .[] as $item ireduce ({}; . *+ $item))])' $FILES | \
yq eval-all '{"workflows": {"build": {"jobs": .}}}' - | \
yq eval-all '. as $wf ireduce({}; . * $wf)' .circleci/continue_config.yml - > complete_config.yml
cat complete_config.yml  # to reproduce generated workflow