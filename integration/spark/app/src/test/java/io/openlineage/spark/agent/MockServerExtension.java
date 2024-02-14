package io.openlineage.spark.agent;

import static org.mockserver.model.HttpRequest.request;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

class MockServerExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, TestInstancePostProcessor {
  public static final String LOCAL_IP = "127.0.0.1";

  private static final AtomicInteger basePort = new AtomicInteger(1081);

  private ClientAndServer clientAndServer;

  @Override
  public void afterAll(ExtensionContext context) {
    if (clientAndServer != null) {
      clientAndServer.stop();
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    int port = basePort.getAndIncrement();
    Configuration configuration = new Configuration();
    configuration.logLevel(Level.ERROR);
    clientAndServer = ClientAndServer.startClientAndServer(configuration, port);
    clientAndServer
        .when(request("/api/v1/lineage"))
        .respond(HttpResponse.response().withStatusCode(201));

    if (context.getTestClass().isPresent()) {
      Logger logger = LoggerFactory.getLogger(context.getTestClass().get());
      Awaitility.await("wait-for-mock-server-start-up")
          .atMost(Duration.ofMinutes(2))
          .pollInterval(Duration.ofSeconds(1))
          .until(
              () -> {
                logger.info(
                    "Waiting for mock server to start on port {}", clientAndServer.getPort());
                return clientAndServer.isRunning();
              });
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    if (clientAndServer != null) {
      clientAndServer.reset();
    }
  }

  @Override
  public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
    if (testInstance instanceof MockServerAware) {
      MockServerAware instance = (MockServerAware) testInstance;
      instance.setClientAndServer(clientAndServer);
    }
  }
}
