import io.openlineage.gradle.plugin.variant.spark.BuildConfigurerDsl

plugins {
    id("java-library")
    id("pmd")
    id("com.diffplug.spotless")
    id("io.freefair.lombok")
    id("com.github.johnrengelman.shadow")
    id("de.undercouch.download")
    id("io.openlineage.common-config")
    id("io.openlineage.spark-variant-build")
    id("io.openlineage.docker-build")
    id("com.adarshr.test-logger") version "3.2.0"
    id("org.gradle.test-retry") version "1.5.8"
}

val dockerFileTemplate = layout.projectDirectory.file("docker/Dockerfile.template")
val dockerManifests = layout.projectDirectory.file("docker/manifests.json")

val spark248Scala212 = "Spark248Scala212"
val spark324Scala212 = "Spark324Scala212"
val spark324Scala213 = "Spark324Scala213"
val spark334Scala212 = "Spark334Scala212"
val spark334Scala213 = "Spark334Scala213"
val spark342Scala212 = "Spark342Scala212"
val spark342Scala213 = "Spark342Scala213"
val spark350Scala212 = "Spark350Scala212"
val spark350Scala213 = "Spark350Scala213"

val apacheHttpClientVersion = "5.3"
val assertjVersion = "3.25.1"
val awaitilityVersion = "4.2.0"
val bigqueryVersion = "0.29.0"
val junit5Version = "5.10.2"
val mockitoVersion = "4.11.0"
val postgresqlVersion = "42.7.1"
val testcontainersVersion = "1.19.3"

val scala212Version = "2.12.18"
val scala213Version = "2.13.10"
val scalaCollectionCompatVersion = "2.11.0"

fun String.determineScalaVersion() = when (this) {
    "2.12" -> scala212Version
    "2.13" -> scala213Version
    else -> throw IllegalArgumentException("Unknown Scala binary version: '$this'")
}

/*
"3.5": ["delta": "NA", "gcs": "hadoop3-2.2.9", "snowflake": "2.13.0-spark_3.4", "iceberg": "NA", "hadoopclient": "3.3.4"],
"3.4": ["delta": "2.4.0", "gcs": "hadoop3-2.2.9", "snowflake": "2.13.0-spark_3.4", "iceberg": "iceberg-spark-runtime-3.4_2.12:1.3.0", "hadoopclient": "3.3.4"],
"3.3": ["delta": "2.1.0", "gcs": "hadoop3-2.2.9", "snowflake": "2.11.0-spark_3.3", "iceberg": "iceberg-spark-runtime-3.3_2.12:0.14.0", "hadoopclient": "3.3.4"],
"3.2": ["delta": "1.1.0", "gcs": "hadoop3-2.2.9", "snowflake": "2.11.0-spark_3.2", "iceberg": "iceberg-spark-runtime-3.2_2.12:0.14.0", "hadoopclient": "3.3.4"],
"2.4": ["delta": "NA", "gcs": "hadoop2-2.2.9", "snowflake": "2.9.3-spark_2.4", "iceberg": "NA", "hadoopclient": "2.10.2"]
*/

builds {
    add(spark248Scala212, "2.4.8", "2.12").configure {
        dependencies {
            val hadoopClientVersion = "2.10.2"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.9.3-spark_2.4"

            implementation(project(":shared", "scala212RuntimeElements"))
            implementation(project(":spark3", "scala212RuntimeElements"))
            implementation(project(":spark32", "scala212RuntimeElements"))
            implementation(project(":spark33", "scala212RuntimeElements"))
            implementation(project(":spark34", "scala212RuntimeElements"))
            implementation(project(":spark35", "scala212RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            testRuntimeOnly("org.apache.hadoop:hadoop-client:${hadoopClientVersion}") {
                exclude(group = "org.apache.hadoop", module = "hadoop-hdfs-client")
                exclude(group = "org.apache.hadoop", module = "hadoop-mapreduce-client-core")
                exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
                exclude(group = "com.fasterxml.jackson.core")
            }

            integrationTestMountOnly("org.slf4j:slf4j-api:2.0.10")
            integrationTestMountOnly("org.slf4j:slf4j-reload4j:2.0.10")
            integrationTestMountOnly("ch.qos.reload4j:reload4j:1.2.25")
            integrationTestMountOnly(dependencies.project(":scala-fixtures", "scala212RuntimeElements"))
        }

        settings {
            disableDeltaTests()
            disableIcebergTests()
        }
    }

    add(spark324Scala212, "3.2.4", "2.12").configure {
        dependencies {
            val deltaVersion = "2.0.2"
            val icebergVersion = "0.14.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.9.3-spark_2.4"

            implementation(project(":shared", "scala212RuntimeElements"))
            implementation(project(":spark3", "scala212RuntimeElements"))
            implementation(project(":spark32", "scala212RuntimeElements"))
            implementation(project(":spark33", "scala212RuntimeElements"))
            implementation(project(":spark34", "scala212RuntimeElements"))
            implementation(project(":spark35", "scala212RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.2_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")
        }
    }

    add(spark324Scala213, "3.2.4", "2.13").configure {
        dependencies {
            val deltaVersion = "2.0.2"
            val icebergVersion = "0.14.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.11.0-spark_3.2"

            implementation(project(":shared", "scala213RuntimeElements"))
            implementation(project(":spark3", "scala213RuntimeElements"))
            implementation(project(":spark32", "scala213RuntimeElements"))
            implementation(project(":spark33", "scala213RuntimeElements"))
            implementation(project(":spark34", "scala213RuntimeElements"))
            implementation(project(":spark35", "scala213RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.2_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")
        }
    }

    add(spark334Scala212, "3.3.4", "2.12").configure {
        dependencies {
            val deltaVersion = "2.3.0"
            val icebergVersion = "0.14.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.11.0-spark_3.3"

            implementation(project(":shared", "scala212RuntimeElements"))
            implementation(project(":spark3", "scala212RuntimeElements"))
            implementation(project(":spark32", "scala212RuntimeElements"))
            implementation(project(":spark33", "scala212RuntimeElements"))
            implementation(project(":spark34", "scala212RuntimeElements"))
            implementation(project(":spark35", "scala212RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.3_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            integrationTestMountOnly(dependencies.project(":scala-fixtures", "scala212RuntimeElements"))
        }
    }

    add(spark334Scala213, "3.3.4", "2.13").configure {
        dependencies {
            val deltaVersion = "2.3.0"
            val icebergVersion = "0.14.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.11.0-spark_3.3"

            implementation(project(":shared", "scala213RuntimeElements"))
            implementation(project(":spark3", "scala213RuntimeElements"))
            implementation(project(":spark32", "scala213RuntimeElements"))
            implementation(project(":spark33", "scala213RuntimeElements"))
            implementation(project(":spark34", "scala213RuntimeElements"))
            implementation(project(":spark35", "scala213RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.3_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            integrationTestMountOnly(dependencies.project(":scala-fixtures", "scala213RuntimeElements"))
        }
    }

    add(spark342Scala212, "3.4.2", "2.12").configure {
        dependencies {
            val deltaVersion = "2.4.0"
            val icebergVersion = "1.3.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.13.0-spark_3.4"

            implementation(project(":shared", "scala212RuntimeElements"))
            implementation(project(":spark3", "scala212RuntimeElements"))
            implementation(project(":spark32", "scala212RuntimeElements"))
            implementation(project(":spark33", "scala212RuntimeElements"))
            implementation(project(":spark34", "scala212RuntimeElements"))
            implementation(project(":spark35", "scala212RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.4_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            testRuntimeOnly("org.apache.logging.log4j:log4j-api:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.22.1")
        }
    }

    add(spark342Scala213, "3.4.2", "2.13").configure {
        dependencies {
            val deltaVersion = "2.4.0"
            val icebergVersion = "1.3.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.13.0-spark_3.4"

            implementation(project(":shared", "scala213RuntimeElements"))
            implementation(project(":spark3", "scala213RuntimeElements"))
            implementation(project(":spark32", "scala213RuntimeElements"))
            implementation(project(":spark33", "scala213RuntimeElements"))
            implementation(project(":spark34", "scala213RuntimeElements"))
            implementation(project(":spark35", "scala213RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-core_${scalaBinaryVersion}:${deltaVersion}")
            testRuntimeOnly("org.apache.iceberg:iceberg-spark-runtime-3.4_${scalaBinaryVersion}:${icebergVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            testRuntimeOnly("org.apache.logging.log4j:log4j-api:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.22.1")
        }
    }

    add(spark350Scala212, "3.5.0", "2.12").configure {
        dependencies {
            val deltaVersion = "3.1.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.13.0-spark_3.4"

            implementation(project(":shared", "scala212RuntimeElements"))
            implementation(project(":spark3", "scala212RuntimeElements"))
            implementation(project(":spark32", "scala212RuntimeElements"))
            implementation(project(":spark33", "scala212RuntimeElements"))
            implementation(project(":spark34", "scala212RuntimeElements"))
            implementation(project(":spark35", "scala212RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-spark_${scalaBinaryVersion}:${deltaVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            testRuntimeOnly("org.apache.logging.log4j:log4j-api:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.22.1")
        }

        settings {
            disableDeltaTests()
            disableIcebergTests()
        }
    }

    add(spark350Scala213, "3.5.0", "2.13").configure {
        dependencies {
            val deltaVersion = "3.1.0"
            val scalaVersion = scalaBinaryVersion.determineScalaVersion()
            val snowflakeVersion = "2.13.0-spark_3.4"

            implementation(project(":shared", "scala213RuntimeElements"))
            implementation(project(":spark3", "scala213RuntimeElements"))
            implementation(project(":spark32", "scala213RuntimeElements"))
            implementation(project(":spark33", "scala213RuntimeElements"))
            implementation(project(":spark34", "scala213RuntimeElements"))
            implementation(project(":spark35", "scala213RuntimeElements"))

            compileOnly("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            compileOnly("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("org.apache.spark:spark-core_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-hive_${scalaBinaryVersion}:${sparkVersion}")
            testImplementation("org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}")

            testImplementation("com.google.cloud.spark:spark-bigquery-with-dependencies_${scalaBinaryVersion}:${bigqueryVersion}") {
                exclude(group = "com.fasterxml.jackson.core")
                exclude(group = "com.fasterxml.jackson.module")
            }

            testImplementation("net.snowflake:spark-snowflake_${scalaBinaryVersion}:${snowflakeVersion}") {
                exclude(group = "com.google.guava:guava")
                exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
            }

            testRuntimeOnly("io.delta:delta-spark_${scalaBinaryVersion}:${deltaVersion}")

            testRuntimeOnly("org.scala-lang:scala-library:${scalaVersion}")
            testRuntimeOnly("org.scala-lang:scala-reflect:${scalaVersion}")
            testRuntimeOnly("org.scala-lang.modules:scala-collection-compat_${scalaBinaryVersion}:${scalaCollectionCompatVersion}")

            testRuntimeOnly("org.apache.logging.log4j:log4j-api:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.22.1")
            testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.22.1")

            integrationTestMountOnly(dependencies.project(":scala-fixtures", "scala213RuntimeElements"))
        }

        settings {
            disableDeltaTests()
            disableIcebergTests()
        }
    }

    commonDependencies {
        // These 2 are stuck at Scala 2.12, and they will be added to the Scala 2.13 builds.
        implementation(project(":spark2", "scala212RuntimeElements"))
        implementation(project(":spark31", "scala212RuntimeElements"))

        implementation("org.apache.httpcomponents.client5:httpclient5:${apacheHttpClientVersion}")

        testImplementation(dependencies.platform("org.junit:junit-bom:${junit5Version}"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testImplementation("org.junit.jupiter:junit-jupiter-params")
        testImplementation("org.postgresql:postgresql:${postgresqlVersion}")
        testImplementation("org.hamcrest:hamcrest-library:2.2")
        testImplementation("org.xerial:sqlite-jdbc:3.45.0.0")
        testImplementation("org.testcontainers:junit-jupiter:${testcontainersVersion}")
        testImplementation("org.testcontainers:postgresql:${testcontainersVersion}")
        testImplementation("org.testcontainers:mockserver:${testcontainersVersion}")
        testImplementation("org.testcontainers:kafka:${testcontainersVersion}")
        testImplementation("org.awaitility:awaitility:${awaitilityVersion}")
        testImplementation("org.assertj:assertj-core:${assertjVersion}")
        testImplementation("org.mockito:mockito-core:${mockitoVersion}")
        testImplementation("org.mockito:mockito-inline:${mockitoVersion}")
        testImplementation("org.mockito:mockito-junit-jupiter:${mockitoVersion}")
        testImplementation("com.databricks:databricks-sdk-java:0.4.0") {
            exclude(group = "com.fasterxml.jackson.core")
            exclude(group = "com.fasterxml.jackson.module")
        }
        testImplementation("org.mock-server:mockserver-netty:5.14.0:shaded") {
            exclude(group = "com.fasterxml.jackson.core")
            exclude(group = "com.fasterxml.jackson.dataformat")
            exclude(group = "com.fasterxml.jackson.datatype")
            exclude(group = "com.google.guava")
            exclude(group = "org.mock-server.mockserver-client-java")
        }
    }

    setDefaultBuild(spark324Scala213)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
    failFast = true
}
