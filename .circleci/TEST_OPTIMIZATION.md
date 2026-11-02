# Datadog Test Optimization

Test Optimization uses native test-runner instrumentation for the supported CI suites below. JUnit reports continue to
be produced for existing consumers, but are not uploaded to Datadog as a second copy of the same tests.

## Credentials and verification

Configure `DD_API_KEY` as a secret in the CircleCI project and the GitHub repository. A GitHub secret is not
automatically available in CircleCI. The CircleCI project variable must reach jobs using the `pr`, `release`, and
`integration-tests` contexts as well as jobs without a context. Do not enable secret sharing for untrusted fork builds
to collect test data.

The site is `us5.datadoghq.com`, consistent with the existing Datadog checks. The Python/container setup scripts respect
a `DD_SITE` override; change the orb/action `site` inputs too when moving the project to another Datadog site. Services
are grouped by component and use `DD_ENV=ci`.

The setup pipeline passes only a `datadog-enabled` boolean to the continuation pipeline, gating the CircleCI orb when
the project key is absent. The custom Python/container setup and GitHub action guards also skip instrumentation when the
key is absent. Tests still run. No script writes API keys to files or command arguments.

Configuration validation does not prove test delivery. After a credentialed CI run, check
[Test Optimization Explorer](https://us5.datadoghq.com/ci/test/runs?query=@git.repository.name:OpenLineage/OpenLineage)
for the commit, component service, individual test cases, failures, and runtime matrix variants. Confirm startup and
delivery on Java 8/11/17/21, the client Gradle 9 build, Python 3.10–3.14, and the Go CI toolchain before considering the
rollout verified. Test Impact Analysis and other optimization features also depend on tracer/runtime support and the
project's Datadog settings.

## CI coverage audit

The audit follows test entry points, framework dependencies, and nested scripts across the repository; it does not claim
every test case was executed locally.

| CI job or suite                                                                                | Instrumentation                              | Scope                                                                                                           |
| ---------------------------------------------------------------------------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `build-client-java`                                                                            | Datadog CircleCI orb, Java                   | Client, generator, and all transport subprojects using Gradle/JUnit                                             |
| `build-integration-sql-java`, `release-integration-sql-java`                                   | Datadog CircleCI orb, Java                   | JUnit tests inside `script/build.sh`; the standalone shell smoke assertion is not a test span                   |
| `test-integration-spark`, `integration-test-integration-spark`                                 | Datadog CircleCI orb, Java                   | JUnit tests in the Java/Scala/Spark matrix, including custom Gradle `Test` tasks                                |
| `build-integration-spark-extension-interfaces`, `build-integration-spark-extension-entrypoint` | Datadog CircleCI orb, Java                   | Gradle test lifecycle; entrypoint currently has no test sources                                                 |
| `integration-test-databricks-integration-spark`                                                | Datadog CircleCI orb, Java                   | JUnit driver on CircleCI; retains the existing nightly/manual condition                                         |
| `configurable-integration-test-spark`                                                          | Native Java tracer in the Docker test runner | Both Spark configurations; tracer JAR mounted read-only and credentials/CI metadata passed at container runtime |
| `test-integration-flink`, `integration-test-integration-flink`                                 | Datadog CircleCI orb, Java                   | JUnit unit/integration tests across the existing Flink matrix                                                   |
| `test-integration-hive`, `integration-test-integration-hive`                                   | Datadog CircleCI orb, Java                   | JUnit unit/integration tests                                                                                    |
| `unit-test-client-python`                                                                      | Native `ddtrace` pytest plugin               | Every existing tox environment, Python 3.10–3.14                                                                |
| `unit-test-integration-common`                                                                 | Native `ddtrace` pytest plugin               | Both dbt dependency environments in tox                                                                         |
| `integration-test-integration-dbt`                                                             | Native `ddtrace` pytest plugin               | Host pytest runner, including its Docker-backed integration tests                                               |
| `build-client-go`                                                                              | Datadog CircleCI orb, Go                     | Existing `go test ./... -v -race -coverprofile=coverage.out`                                                    |
| GitHub `run-integration-tests-emr`, `run-integration-tests-azure`                              | Datadog GitHub action, Java                  | SQL tests reached through `task setup`, then AWS/Azure JUnit drivers                                            |
| GitHub `take-screenshots-main`, `take-screenshots-pull-request`                                | Datadog GitHub action, JavaScript            | Playwright tests with the action's `DD_TRACE_PACKAGE` explicitly loaded through `NODE_OPTIONS`                  |

Datadog's CircleCI orb is pinned to `1.2.1`; GitHub uses the documented `v3` action. The isolated Python environments
resolve `ddtrace` for their own interpreter. The nested Spark container downloads Datadog's latest Java tracer. These
tracer versions should be recorded from the CI setup logs when validating or diagnosing a run.

Tox filters inherited environment variables, so both tox configurations pass `DD_*`, `CIRCLE*`, `CI`, and
`PYTEST_ADDOPTS`. The tracer is installed after tox creates each environment and before pytest runs. For dbt,
`uv run --no-sync` keeps the CI-installed tracer in the already-synced virtual environment. Datadog transport unit tests
clear only their own API-key/site environment variables so CI instrumentation settings do not change their expected
defaults.

Testcontainers, dbt containers, remote Spark drivers/executors, and browser processes are workloads exercised by the
instrumented host test runners. They are not separately instrumented or given the Datadog API key. The configurable
Spark CLI is different: its JUnit runner itself runs in Docker, so that container receives the Java tracer and CI
metadata. Docker image builds do not receive the key; SQL tests executed while building that image are not instrumented.

The main-branch screenshot job explicitly uses the checked-out main commit for Datadog's Git metadata, since its
triggering event belongs to a pull request.

## Explicit exclusions

| Tests or checks                                                                                    | Reason they are not instrumented                                                                                                                                                                                                             |
| -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rust SQL parser: `build-integration-sql-python` (x86/ARM) and `build-integration-sql-python-macos` | These jobs run Cargo's Rust tests, despite their Python job names. Datadog's setup guidance falls back to JUnit ingestion for this runner; the current Cargo commands do not produce JUnit XML. A compatible report adapter is still needed. |
| Fluentd: `build-proxy-fluentd`                                                                     | Uses Ruby `Test::Unit`, not a supported RSpec/Minitest runner. A JUnit reporter/adapter is still needed; no native Ruby instrumentation is claimed.                                                                                          |
| `run-pre-commit`, schema/facet fixture validation, and `jarVerification` tasks                     | Validation commands rather than supported test runners; retained as CI checks. The instrumentation regression tests run through a pre-commit hook.                                                                                           |
| `generator/go` unit tests and `integration/sql/iface-py/tests/python`                              | Present in source but not invoked by the current CI commands. `go run` of the generator and Python wheel import smoke checks do not execute these suites.                                                                                    |
| Common integration Great Expectations tests                                                        | Explicitly excluded by the existing pytest configuration.                                                                                                                                                                                    |

No workflow filters, test selections, or excluded suites are expanded by this setup. A source file containing a test
does not imply that CI currently runs it.

## Local configuration checks

With PyYAML installed, run `python -m unittest discover -s .circleci/tests -v`. The same checks run through the
`test-datadog-ci` pre-commit hook. They execute the setup and container-launch scripts with mocked installers/Docker,
checking interpreter selection, credential omission, safe environment quoting, installer failures, test failure exit
status, and the optional Playwright bootstrap. They also check tox metadata forwarding and setup placement in CircleCI.

---

SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
