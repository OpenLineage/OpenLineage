/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;
import org.testcontainers.shaded.org.yaml.snakeyaml.constructor.Constructor;

@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class ConfigurableTestConfig {
  String appName;
  String sparkVersion;
  String scalaBinaryVersion;
  boolean enableHiveSupport;
  List<String> packages;
  Map<String, String> sparkConf;

  File inputScript;
  List<File> expectationJsons;

  public static ConfigurableTestConfig load(String testDir, String sparkConfFile) {
    ConfigurableTestConfig conf = loadFromSparkConfFile(sparkConfFile);
    loadFromTestDir(testDir, conf);

    return conf;
  }

  /**
   * In case of pyspark inputScript, script content is returner. In case of sql inputScript, sql is
   * decorated in pyspark code
   *
   * @return
   */
  @SneakyThrows
  public String generatePySparkCode() {
    if (inputScript.getName().toLowerCase(Locale.ROOT).endsWith(".py")) {
      return new String(Files.readAllBytes(inputScript.toPath()), StandardCharsets.UTF_8);
    } else {
      // decorate sql lines into pyspark code
      return decorateSqlToPySpark(Files.readAllLines(inputScript.toPath()));
    }
  }

  @SneakyThrows
  public Path generateRunDir(Path testDir) {
    Path runDir =
        Paths.get(
            "/usr/lib/openlineage/integration/spark/cli/runs/run"
                + new SimpleDateFormat("MMdd_hhmm", Locale.ROOT).format(new Date())
                + "_"
                + testDir.getFileName());
    log.info("Test run output will be written to: {}", runDir);
    Files.createDirectories(runDir);

    Set<PosixFilePermission> perms = new HashSet<>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.OWNER_WRITE);
    perms.add(PosixFilePermission.OWNER_EXECUTE);

    perms.add(PosixFilePermission.OTHERS_READ);
    perms.add(PosixFilePermission.OTHERS_WRITE);
    perms.add(PosixFilePermission.OTHERS_EXECUTE);

    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.GROUP_WRITE);
    perms.add(PosixFilePermission.GROUP_EXECUTE);

    Files.setPosixFilePermissions(runDir, perms);

    return runDir;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ConfigurableTestConf[");
    builder.append("appName: ").append(this.getAppName()).append(", ");
    builder.append("inputScript: ").append(this.getInputScript()).append(", ");
    builder.append("sparkVersion: ").append(this.getSparkVersion()).append(", ");
    builder.append("scalaBinaryVersion: ").append(this.getScalaBinaryVersion()).append(", ");
    builder
        .append("expectedJsons: [")
        .append(
            String.join(
                ",", expectationJsons.stream().map(File::getName).collect(Collectors.toList())))
        .append("], ");

    builder.append("sparkConf: [");
    sparkConf.forEach((k, v) -> builder.append(k).append(":").append(v));
    builder.append("]");
    builder.append("]");

    return builder.toString();
  }

  private String decorateSqlToPySpark(List<String> sqls) {
    List<String> script = new ArrayList<>();
    script.add("import os");
    script.add("import time");
    script.add("from pyspark.sql import SparkSession");
    script.add("");

    String sparkSession =
        String.format("spark = SparkSession.builder.master('local').appName('%s')", appName);
    if (isEnableHiveSupport()) {
      sparkSession += ".enableHiveSupport()";
    }
    sparkSession += ".getOrCreate()";
    script.add(sparkSession);

    sqls.forEach(line -> script.add("spark.sql('" + line + "')"));
    script.add("");

    // make sure OL events get written to file
    script.add("time.sleep(3)");
    script.add("");

    return String.join(System.lineSeparator(), script);
  }

  private static ConfigurableTestConfig loadFromSparkConfFile(String sparkConfFile) {
    Path path = Paths.get(sparkConfFile);
    if (Files.exists(path)) {
      try {
        Yaml yaml = new Yaml(new Constructor(ConfigurableTestConfig.class));
        InputStream inputStream = new FileInputStream(sparkConfFile);
        return yaml.load(inputStream);
      } catch (IOException e) {
        throw new RuntimeException("Couldn't read value from spark-conf file: " + path, e);
      }
    }

    throw new RuntimeException("spark-conf file does not exist: " + sparkConfFile);
  }

  private static void loadFromTestDir(String testDir, ConfigurableTestConfig conf) {
    File dir = new File(testDir);
    if (!dir.isDirectory()) {
      throw new RuntimeException("test directory is not a valid directory");
    }

    // there should be at least one .sql or .py file
    Arrays.stream(dir.listFiles())
        .filter(
            f -> {
              String name = f.getName().toLowerCase(Locale.ROOT);
              return name.endsWith(".sql") || name.endsWith(".py");
            })
        .findFirst()
        .ifPresent(f -> conf.setInputScript(f));

    conf.setExpectationJsons(
        Arrays.stream(dir.listFiles())
            .filter(f -> f.getName().toLowerCase(Locale.ROOT).endsWith(".json"))
            .collect(Collectors.toList()));

    if (conf.getInputScript() == null) {
      throw new RuntimeException("No SQL/Python script file found in test directory specified");
    }

    if (conf.getExpectationJsons() == null || conf.getExpectationJsons().isEmpty()) {
      throw new RuntimeException("No JSON expectation files found in test directory specified");
    }
  }
}
