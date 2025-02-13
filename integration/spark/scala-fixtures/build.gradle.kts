plugins {
    id("java-library")
    id("scala")
}

repositories {
    mavenCentral()
}

configureScalaVariant("2.11", "2.4.8")
configureScalaVariant("2.12", "3.5.4")
configureScalaVariant("2.13", "3.5.4")

dependencies {
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.4")
}

fun configureScalaVariant(scalaBinaryVersion: String, sparkVersion: String) {
    val scalaFmt = scalaBinaryVersion.replace(".", "")

    val compileOnlyCfg = configurations.create("scala${scalaFmt}CompileOnly") {
        isCanBeConsumed = false
    }
    val artifactCfg = configurations.create("scala${scalaFmt}RuntimeElements") {
        isCanBeConsumed = true
    }

    dependencies.add(compileOnlyCfg.name, "org.apache.spark:spark-sql_${scalaBinaryVersion}:${sparkVersion}")

    val compileTask = tasks.register<ScalaCompile>("compileScala${scalaFmt}") {
        group = "build"
        description = "Compiles the main Scala source set using Scala $scalaBinaryVersion"
        classpath = compileOnlyCfg
        setSource(sourceSets["main"].scala.srcDirs)
        destinationDirectory.set(layout.buildDirectory.dir("classes/scala-${scalaBinaryVersion}"))
        scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
        scalaCompileOptions.incrementalOptions.analysisFile.set(layout.buildDirectory.file("tmp/scala-${scalaBinaryVersion}/compilerAnalysis/${this.name}.analysis"))
        scalaCompileOptions.incrementalOptions.classfileBackupDir.set(layout.buildDirectory.file("tmp/scala-${scalaBinaryVersion}/classfileBackup/${this.name}.backup"))
    }

    val jarTask = tasks.register<Jar>("jarScala${scalaFmt}") {
        dependsOn(compileTask)
        group = "build"
        description = "Assembles a jar archive containing the main classes compiled with Scala $scalaBinaryVersion"
        from(compileTask.get().outputs)
        archiveBaseName.set("openlineage-spark-scala-fixtures_${scalaBinaryVersion}")
        destinationDirectory.set(layout.buildDirectory.dir("libs/scala-${scalaBinaryVersion}"))
        includeEmptyDirs = false
    }

    tasks.named("build").configure {
        dependsOn(jarTask)
    }

    artifacts.add(artifactCfg.name, jarTask)
}

tasks.named("jar").configure {
    enabled = false
}
