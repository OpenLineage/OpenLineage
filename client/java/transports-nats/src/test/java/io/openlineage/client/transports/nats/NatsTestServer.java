/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A JetStream-enabled NATS server for tests: a local nats-server binary when one is on the PATH,
 * otherwise the nats Docker image. Tests are skipped when neither is available.
 */
final class NatsTestServer implements AutoCloseable {
  private static final int NATS_PORT = 4222;

  private final Process process;
  private final GenericContainer<?> container;
  private final String url;

  private NatsTestServer(Process process, GenericContainer<?> container, String url) {
    this.process = process;
    this.container = container;
    this.url = url;
  }

  static NatsTestServer start(String... extraArgs) throws Exception {
    Optional<Path> binary = findOnPath("nats-server");
    if (binary.isPresent()) {
      return startProcess(binary.get(), extraArgs);
    }
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(), "needs nats-server on PATH or Docker");
    return startContainer(extraArgs);
  }

  String getUrl() {
    return url;
  }

  @Override
  public void close() throws Exception {
    if (process != null) {
      process.destroy();
      process.waitFor();
    }
    if (container != null) {
      container.stop();
    }
  }

  private static NatsTestServer startProcess(Path binary, String... extraArgs) throws Exception {
    int port = freePort();
    String host = InetAddress.getLoopbackAddress().getHostAddress();
    Path store = Files.createTempDirectory("nats-jetstream");
    List<String> command =
        new ArrayList<>(
            Arrays.asList(
                binary.toString(),
                "-js",
                "-a",
                host,
                "-p",
                String.valueOf(port),
                "-sd",
                store.toString()));
    command.addAll(Arrays.asList(extraArgs));
    Process process =
        new ProcessBuilder(command)
            .redirectOutput(store.resolve("nats-server.log").toFile())
            .redirectErrorStream(true)
            .start();
    String url = "nats://" + host + ":" + port;
    waitUntilReachable(url);
    return new NatsTestServer(process, null, url);
  }

  private static NatsTestServer startContainer(String... extraArgs) {
    List<String> command = new ArrayList<>();
    command.add("-js");
    command.addAll(Arrays.asList(extraArgs));
    GenericContainer<?> container =
        new GenericContainer<>("nats:2")
            .withCommand(command.toArray(new String[0]))
            .withExposedPorts(NATS_PORT)
            .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
    container.start();
    return new NatsTestServer(
        null,
        container,
        "nats://" + container.getHost() + ":" + container.getMappedPort(NATS_PORT));
  }

  private static void waitUntilReachable(String url) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    Options options =
        new Options.Builder()
            .server(url)
            .noReconnect()
            .connectionTimeout(Duration.ofSeconds(1))
            .build();
    while (true) {
      try (Connection ignored = Nats.connect(options)) {
        return;
      } catch (IOException e) {
        // a server started with auth rejects the anonymous probe, which still proves it is up
        if (e.getMessage() != null && e.getMessage().contains("Authorization")) {
          return;
        }
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("NATS server did not start at " + url, e);
        }
        Thread.sleep(100);
      }
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static Optional<Path> findOnPath(String executable) {
    String path = System.getenv("PATH");
    if (path == null) {
      return Optional.empty();
    }
    return Arrays.stream(path.split(File.pathSeparator))
        .map(dir -> new File(dir, executable).toPath())
        .filter(Files::isExecutable)
        .findFirst();
  }
}
