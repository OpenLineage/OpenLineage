/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import static io.openlineage.hive.testutils.HiveContainerProperties.HOOK_JAR;
import static io.openlineage.hive.testutils.HiveContainerProperties.HOST_BUILD_DIR;
import static io.openlineage.hive.testutils.HiveContainerProperties.RESOURCES_DIR;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class HiveContainerTestUtils {

  static final DockerImageName HIVE_IMAGE =
      DockerImageName.parse("quay.io/openlineage/hive").withTag("3.1.3");

  public static GenericContainer<?> makeHiveContainer(Network network) {
    GenericContainer<?> container =
        new GenericContainer<>(HIVE_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("hiveserver2")
            .withEnv("SERVICE_NAME", "hiveserver2")
            .withExposedPorts(10000, 10002)
            .withEnv("HIVE_SERVER2_THRIFT_PORT", "10000");
    mountPath(
        container,
        HOST_BUILD_DIR.resolve("libs").resolve(HOOK_JAR),
        Paths.get("/opt/hive/lib").resolve(HOOK_JAR));
    mountPath(
        container,
        RESOURCES_DIR.resolve("hive-site.xml"),
        Paths.get("/opt/hive/conf/hive-site.xml"));
    return container;
  }

  static void mountPath(GenericContainer<?> container, Path sourcePath, Path targetPath) {
    if (log.isDebugEnabled()) {
      log.debug(
          "[image={}]: Mount volume '{}:{}'",
          container.getDockerImageName(),
          sourcePath,
          targetPath);
    }
    container.withFileSystemBind(sourcePath.toString(), targetPath.toString(), BindMode.READ_WRITE);
  }

  @SneakyThrows
  static void mountFiles(GenericContainer<?> container, Path sourceDir, Path targetDir) {
    if (!Files.exists(sourceDir)) {
      log.warn("Source directory {} does not exist, skipping mount", sourceDir);
      return;
    }

    try (Stream<Path> files = Files.list(sourceDir)) {
      files.forEach(
          filePath -> {
            Path hostPath = filePath.toAbsolutePath();
            Path fileName = hostPath.getFileName();
            Path containerPath = targetDir.resolve(fileName);
            mountPath(container, hostPath, containerPath);
          });
    }
  }
}
