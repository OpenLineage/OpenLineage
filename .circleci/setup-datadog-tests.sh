#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Configure isolated pytest environments or the nested Spark test container.
# CircleCI sources BASH_ENV again before the next run step.
set -euo pipefail

mode="${1:?Expected pytest or java-container}"
service="${2:?Expected a Datadog service name}"

if [[ -z "${DD_API_KEY:-}" ]]; then
  echo "DD_API_KEY is unavailable; running tests without Datadog instrumentation."
  exit 0
fi

: "${BASH_ENV:?CircleCI must provide BASH_ENV}"

case "$mode" in
  pytest)
    test_python="${3:?Expected the test Python interpreter}"
    uv pip install --python "$test_python" ddtrace
    # Installing into tox/.venv is necessary: the runner's Python is separate.
    pytest_options="${PYTEST_ADDOPTS:-}"
    case " $pytest_options " in
      *" --ddtrace "*) ;;
      *) pytest_options="${pytest_options:+$pytest_options }--ddtrace" ;;
    esac
    printf 'export PYTEST_ADDOPTS=%q\n' "$pytest_options" >> "$BASH_ENV"
    ;;
  java-container)
    agent_dir=$(mktemp -d "${TMPDIR:-/tmp}/openlineage-datadog.XXXXXX")
    if ! curl --fail --silent --show-error --location https://dtdg.co/latest-java-tracer \
      --output "$agent_dir/dd-java-agent.jar"; then
      rm -rf "$agent_dir"
      exit 1
    fi
    printf 'export DD_TEST_JAVA_AGENT=%q\n' "$agent_dir/dd-java-agent.jar" >> "$BASH_ENV"
    ;;
  *)
    echo "Unsupported test instrumentation mode: $mode" >&2
    exit 1
    ;;
esac

# Credentials stay in CircleCI's environment; never write them to BASH_ENV.
{
  printf 'export DD_SITE=%q\n' "${DD_SITE:-us5.datadoghq.com}"
  printf 'export DD_ENV=%q\n' "${DD_ENV:-ci}"
  printf 'export DD_SERVICE=%q\n' "$service"
  printf '%s\n' 'export DD_CIVISIBILITY_ENABLED=true' 'export DD_CIVISIBILITY_AGENTLESS_ENABLED=true'
} >> "$BASH_ENV"
