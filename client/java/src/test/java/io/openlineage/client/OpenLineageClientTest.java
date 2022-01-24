package io.openlineage.client;

import static io.openlineage.client.OpenLineageClient.DEFAULT_BASE_URL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for {@link OpenLineageClient}. */
@ExtendWith(MockitoExtension.class)
public class OpenLineageClientTest {
  private final String NAMESPACE = "test";
  private final URI PRODUCER = URI.create("https://github.com/OpenLineage/tree/0.0.1/client/java");
  private final OpenLineage OL = new OpenLineage(PRODUCER);
  private final UUID RUN_ID = UUID.randomUUID();
  private final ZonedDateTime RUN_EVENT_TIME = ZonedDateTime.now(ZoneId.of("UTC"));
  private final OpenLineage.RunEvent RUN_EVENT =
      OL.newRunEvent(
          "COMPLETE",
          RUN_EVENT_TIME,
          OL.newRunBuilder().runId(RUN_ID).build(),
          OL.newJobBuilder().namespace(NAMESPACE).name("test-job").build(),
          Collections.emptyList(),
          Collections.emptyList());

  @Mock private OpenLineageHttp http;
  private OpenLineageClient client;

  @BeforeEach
  public void setUp() {
    client = new OpenLineageClient(http);
  }

  @Test
  public void testClientBuilder_default() {
    final OpenLineageClient client = OpenLineageClient.builder().build();
    assertThat(client.http.baseUrl).isEqualTo(DEFAULT_BASE_URL);
    assertThat(client.http.apiKey).isNull();
  }

  @Test
  public void testClientBuilder_overrideUrl() throws Exception {
    final URL url = new URL("http://test.com:8080");
    final OpenLineageClient client = OpenLineageClient.builder().baseUrl(url).build();
    assertThat(client.http.baseUrl).isEqualTo(url);
  }

  @Test
  public void testClientBuilder_throwsOnBadUrl() {
    final String badUrlString = "test.com/api/v1";
    assertThatExceptionOfType(AssertionError.class)
        .isThrownBy(() -> OpenLineageClient.builder().baseUrl(badUrlString).build());
  }

  @Test
  public void testClient_emit() {
    client.emit(RUN_EVENT);

    verify(http, times(1)).post(http.url("/lineae"), Utils.toJson(RUN_EVENT));
  }
}
