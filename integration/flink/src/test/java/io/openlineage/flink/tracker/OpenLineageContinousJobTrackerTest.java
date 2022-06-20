package io.openlineage.flink.tracker;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.openlineage.flink.agent.facets.CheckpointFacet;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenLineageContinousJobTrackerTest {

  WireMockServer wireMockServer = new WireMockServer(18088);
  ReadableConfig config = mock(ReadableConfig.class);
  OpenLineageContinousJobTracker tracker =
      new OpenLineageContinousJobTracker(config, Duration.ofMillis(100));
  FlinkExecutionContext context = mock(FlinkExecutionContext.class);
  JobID jobID = new JobID(1, 2);
  CheckpointFacet expectedCheckpointFacet = new CheckpointFacet(1, 5, 6, 7, 1);

  String jsonCheckpointResponse =
      "{\"counts\":"
          + "{"
          + "\"completed\":%d,"
          + "\"failed\":5,"
          + "\"in_progress\":6,"
          + "\"restored\":7,"
          + "\"total\":%d"
          + "}"
          + "}";

  @BeforeEach
  public void setup() {
    wireMockServer.start();
    configureFor("localhost", 18088);
    when(context.getJobId()).thenReturn(jobID);
    when(config.get(RestOptions.ADDRESS)).thenReturn("localhost");
    when(config.get(RestOptions.PORT)).thenReturn(18088);
  }

  @AfterEach
  public void stop() {
    wireMockServer.stop();
  }

  @Test
  @SneakyThrows
  public void testStartTrackingEventsEmitted() {
    stubFor(
        get(urlEqualTo(String.format("/jobs/%s/checkpoints", jobID.toString())))
            .inScenario("checkpoints")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withBody(String.format(jsonCheckpointResponse, 0, 0)))
            .willSetStateTo("second checkpoint"));

    stubFor(
        get(urlEqualTo(String.format("/jobs/%s/checkpoints", jobID.toString())))
            .inScenario("checkpoints")
            .whenScenarioStateIs("second checkpoint")
            .willReturn(aResponse().withBody(String.format(jsonCheckpointResponse, 1, 1))));

    CountDownLatch methodDone = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              methodDone.countDown();
              return null;
            })
        .when(context)
        .onJobCheckpoint(any());

    tracker.startTracking(context);
    methodDone.await(10, TimeUnit.SECONDS);

    verify(context, times(1)).onJobCheckpoint(eq(expectedCheckpointFacet));
    tracker.stopTracking();
  }

  @Test
  @SneakyThrows
  public void testTrackerContinuesToWorkWhenRestApiGoesDownForSomeTime() {
    stubFor(
        get(urlEqualTo(String.format("/jobs/%s/checkpoints", jobID.toString())))
            .inScenario("checkpoints")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withBody(String.format(jsonCheckpointResponse, 0, 0)))
            .willSetStateTo("api goes down"));

    stubFor(
        get(urlEqualTo(String.format("/jobs/%s/checkpoints", jobID.toString())))
            .inScenario("checkpoints")
            .whenScenarioStateIs("api goes down")
            .willReturn(aResponse().withStatus(403))
            .willSetStateTo("second checkpoint"));

    stubFor(
        get(urlEqualTo(String.format("/jobs/%s/checkpoints", jobID.toString())))
            .inScenario("checkpoints")
            .whenScenarioStateIs("second checkpoint")
            .willReturn(aResponse().withBody(String.format(jsonCheckpointResponse, 1, 1))));

    CountDownLatch methodDone = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              methodDone.countDown();
              return null;
            })
        .when(context)
        .onJobCheckpoint(any());

    tracker.startTracking(context);
    methodDone.await(10, TimeUnit.SECONDS);

    verify(context, times(1)).onJobCheckpoint(eq(expectedCheckpointFacet));
    tracker.stopTracking();
  }
}
