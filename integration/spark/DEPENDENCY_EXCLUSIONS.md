# Dependency upgrades excluded from dependabot bump (#4658)

This PR bumps 26 dependencies in `integration/spark`. Several of those bumps break the
build or force the build tooling / bytecode baseline above Java 8, which this module
still needs to support (`sourceCompatibility`/`targetCompatibility` are pinned to
`VERSION_1_8` in `buildSrc/src/main/kotlin/io/openlineage/gradle/plugin/CommonConfigPlugin.kt`,
and several CircleCI test matrix entries run entirely on a Java 8 JDK).

The following individual bumps were reverted; everything else in the dependabot PR was
kept as proposed.

## gradle-wrapper: 8.9 -> 9.6.0

**Why excluded:** Gradle 9 requires JVM 17+ to run the build itself (independent of what
bytecode level the project targets), which conflicts with the Java-8 CI matrix. It also
breaks the build outright: `integration/spark/build.gradle` configures
`jacocoTestReport.reports.html.destination`, a property removed in the Gradle 9 reporting
API (`Could not set unknown property 'destination' for Report html of type
org.gradle.api.reporting.internal.SingleDirectoryReport`). This was the root cause of
every failing CI job on this PR (`build-integration-spark-2.12/2.13`, `run-pre-commit`,
and the `java:17` test jobs) — they all fail at Gradle configuration time, before any
test code runs.

**How to make it compatible:** Two independent blockers need to be cleared before this
upgrade can land:
1. Migrate `jacocoTestReport` in `integration/spark/build.gradle` to the new reporting
   API — replace `enabled`/`destination` with `required.set(true)` /
   `outputLocation.set(file(...))` (or a single `xml.required`/`html.outputLocation` pair).
2. Drop Java 8 as a supported build/test JVM for `integration/spark` (Gradle 9 cannot run
   on it at all), which means removing or reworking the `java:8-spark:*` entries in
   `.circleci/workflows/openlineage-spark.yml`, or keeping those matrix entries on the old
   wrapper via a separate Gradle installation — not a small change, so out of scope here.

## org.junit:junit-bom / org.junit.jupiter:junit-jupiter(-api): 5.11.4 -> 6.1.0

**Why excluded:** JUnit 6 raises its minimum supported Java version to 17, so any
`java:8-*` CircleCI test matrix entry would fail to even load the test classes.

**How to make it compatible:** Same prerequisite as the Gradle bump — Java 8 needs to be
dropped as a supported test JVM for this module before JUnit 6 can be adopted. Once that
happens this bump can be re-applied on its own (it compiled and ran cleanly against
JDK 17 in local verification).

## com.google.cloud.spark:spark-*-bigquery / spark-bigquery-with-dependencies_*: 0.42.2 -> 0.44.2

**Why excluded:** This bump is unrelated to the Java-8 constraint but still breaks
compilation: `com.google.cloud.spark.bigquery.BigQueryRelationProvider` was removed from
the artifact between 0.42.2 and 0.44.2, and it's referenced directly from
`integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/BigQueryNodeOutputVisitor.java`,
`BigQueryNodeInputVisitor.java`, and `SaveIntoDataSourceCommandVisitor.java`
(`cannot find symbol ... BigQueryRelationProvider`).

**How to make it compatible:** Check the 0.44.x release notes/source for the
replacement type or entry point that superseded `BigQueryRelationProvider` (it may have
been renamed/relocated as part of the connector's DSv2 migration) and update the three
visitor classes above to use it, then re-apply this version bump.

**Note:** the `junit5Version` property is declared independently in every module's
`build.gradle` (there's no shared version catalog), so this revert had to be applied in
`app/`, `shared/`, `spark3/`, `spark31/`-`spark40/`, and the vendor modules
`vendor/iceberg/`, `vendor/gcp/`, and `vendor/snowflake/`. The vendor modules were missed
in an earlier pass and caused `:iceberg:compileTestJava` / `:gcp:compileTestJava` to fail
in CI with the same "only compatible with JVM runtime version 17 or newer" resolution
error before being caught and fixed.

## org.apache.kafka:kafka-clients: 4.1.1 -> 4.3.0

**Why excluded:** Not a Java-8 issue, but it breaks dependency resolution: newer
`kafka-clients` depends on the lz4-java fork `at.yawk.lz4:lz4-java:1.10.2` instead of the
original `org.lz4:lz4-java` artifact. Both jars declare the same Gradle capability
(`org.lz4:lz4-java`), and `:shared`'s test runtime classpath also pulls in
`org.lz4:lz4-java:1.7.1` transitively via `org.apache.spark:spark-hive_2.13:3.2.4` ->
`spark-core_2.13:3.2.4`. Gradle refuses to resolve two different modules that provide the
same capability, so `:shared:testScala213RuntimeClasspath` (and the analogous 2.12
classpath) fails with `Cannot select module with conflict on capability
'org.lz4:lz4-java:...' also provided by [...]`, which in turn fails
`:shared:executeTestScala213`/`executeTestScala212` in the `java:17-spark:*-scala:*` CI
jobs.

**How to make it compatible:** Either wait for the Spark/`spark-core` dependency this
module tests against to move onto the `at.yawk.lz4` fork as well (so both sides agree on
one capability provider), or add an explicit Gradle capability resolution strategy in
`integration/spark/shared/build.gradle` (`resolutionStrategy.capabilitiesResolution` or a
per-configuration `exclude group: 'org.lz4'` / `exclude group: 'at.yawk.lz4'` on the
`kafka-clients` dependency) to force one side consistently, then re-apply this bump.

## Kotlin toolchain (buildSrc only): kept at pre-bump versions

Also reverted, purely as a consequence of keeping `gradle-wrapper` at 8.9:

- `kotlin("plugin.serialization")` Gradle plugin: `2.4.0` -> `2.1.10`
- `org.jetbrains.kotlinx:kotlinx-serialization-json`: `1.11.0` -> `1.8.0`
- `org.javassist:javassist`: `3.31.0-GA` -> `3.30.2-GA`

**Why excluded:** These aren't independently broken, but Gradle 8.9 embeds Kotlin 1.9.23
for its `kotlin-dsl` plugin, and `buildSrc` uses `kotlin-dsl` together with an explicit
`kotlin("plugin.serialization")` version. Kotlin 2.4.0 is far enough ahead of 1.9.23 that
`buildSrc:compileKotlin` fails outright (`Language version 1.8 is no longer supported;
use version 2.0 or greater instead`), and even at 2.1.10 the `kotlinx-serialization-json`
1.11.0 artifact (compiled with Kotlin 2.3.x metadata) fails to link against it
(`Module was compiled with an incompatible version of Kotlin`). `javassist` was rolled
back alongside it only for consistency with the rest of this Kotlin-toolchain group; it
has no known incompatibility of its own.

**How to make it compatible:** These can be re-applied together with the `gradle-wrapper`
bump once that lands (a newer Gradle version embeds a newer `kotlin-dsl` Kotlin runtime
that these versions are compatible with).

## Verification performed

With the above four items reverted (and the rest of the dependabot bump kept), the
following were run locally and passed:

```
./gradlew --console=plain javadoc sourceJar shadowJar publishToMavenLocal \
    -Pscala.binary.version=2.12 -Pjava.compile.home=<jdk17>
./gradlew --console=plain javadoc sourceJar shadowJar publishToMavenLocal \
    -Pscala.binary.version=2.13 -Pjava.compile.home=<jdk17>
```

matching the exact commands `build-integration-spark` runs in CircleCI, plus a sample
`:app:test` run (`ArgumentParserTest`) against Spark 3.5.6 / Scala 2.13 to confirm the
reverted JUnit version executes normally under JDK 17.
