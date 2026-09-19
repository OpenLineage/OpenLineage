/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
 * A JetStream-enabled NATS server for tests: {@code NATS_URL} when set, else a local nats-server
 * binary on the PATH, else the nats Docker image. Tests are skipped when none is available.
 */
final class NatsTestServer implements AutoCloseable {
  private static final int NATS_PORT = 4222;
  private static final String GREETING = "INFO";

  private final Process process;
  private final GenericContainer<?> container;
  private final String url;

  private NatsTestServer(Process process, GenericContainer<?> container, String url) {
    this.process = process;
    this.container = container;
    this.url = url;
  }

  static NatsTestServer start() throws Exception {
    String externalUrl = System.getenv("NATS_URL");
    if (externalUrl != null) {
      waitUntilReachable(externalUrl);
      return new NatsTestServer(null, null, externalUrl);
    }
    if (findOnPath("nats-server").isPresent()) {
      return local().start();
    }
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "needs NATS_URL, nats-server on PATH or Docker");
    return startContainer();
  }

  /** A server with custom arguments, config, port or storage; needs the nats-server binary. */
  static Local local() {
    return new Local();
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

  static final class Local {
    private final List<String> args = new ArrayList<>();
    private String config;
    private Integer port;
    private Path store;
    private boolean jetstream = true;

    Local args(String... values) {
      args.addAll(Arrays.asList(values));
      return this;
    }

    Local config(String value) {
      this.config = value;
      return this;
    }

    Local port(int value) {
      this.port = value;
      return this;
    }

    Local store(Path value) {
      this.store = value;
      return this;
    }

    Local withoutJetStream() {
      this.jetstream = false;
      return this;
    }

    NatsTestServer start() throws Exception {
      Optional<Path> binary = findOnPath("nats-server");
      assumeTrue(binary.isPresent(), "needs nats-server on PATH");
      int actualPort = port == null ? freePort() : port;
      String host = InetAddress.getLoopbackAddress().getHostAddress();
      Path workdir = Files.createTempDirectory("nats-server");
      List<String> command =
          new ArrayList<>(
              Arrays.asList(binary.get().toString(), "-a", host, "-p", String.valueOf(actualPort)));
      if (jetstream) {
        Path storeDir = store == null ? workdir.resolve("jetstream") : store;
        command.addAll(Arrays.asList("-js", "-sd", storeDir.toString()));
      }
      if (config != null) {
        Path configFile = workdir.resolve("nats.conf");
        Files.write(configFile, config.getBytes(StandardCharsets.UTF_8));
        command.addAll(Arrays.asList("-c", configFile.toString()));
      }
      command.addAll(args);
      Process process =
          new ProcessBuilder(command)
              .redirectOutput(workdir.resolve("nats-server.log").toFile())
              .redirectErrorStream(true)
              .start();
      String url = "nats://" + host + ":" + actualPort;
      waitUntilReachable(url);
      return new NatsTestServer(process, null, url);
    }
  }

  private static NatsTestServer startContainer() {
    GenericContainer<?> container =
        new GenericContainer<>("nats:2")
            .withCommand("-js")
            .withExposedPorts(NATS_PORT)
            .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
    container.start();
    return new NatsTestServer(
        null,
        container,
        "nats://" + container.getHost() + ":" + container.getMappedPort(NATS_PORT));
  }

  private static void waitUntilReachable(String url) throws InterruptedException {
    URI uri = URI.create(url);
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (true) {
      // nats-server greets every client with an INFO line, also when it requires auth or TLS
      try (Socket socket = new Socket(uri.getHost(), uri.getPort())) {
        socket.setSoTimeout(1000);
        byte[] greeting = new byte[GREETING.length()];
        int read = socket.getInputStream().read(greeting);
        if (GREETING.equals(
            new String(greeting, 0, Math.max(read, 0), StandardCharsets.US_ASCII))) {
          return;
        }
      } catch (IOException e) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("NATS server did not start at " + url, e);
        }
      }
      Thread.sleep(100);
    }
  }

  static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  static Optional<Path> findOnPath(String executable) {
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
