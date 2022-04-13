package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.transports.HttpTransport;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ConfigTest {
  @Test
  void testLoadConfigFromYaml() throws URISyntaxException {
    Path configPath =
        Paths.get(this.getClass().getClassLoader().getResource("config/http.yaml").toURI());
    OpenLineageClient client = Clients.newClient(new TestConfigPathProvider(configPath));
    assertThat(client.transport).isInstanceOf(HttpTransport.class);
  }

  static class TestConfigPathProvider implements ConfigPathProvider {
    private final Path path;

    public TestConfigPathProvider(Path path) {
      this.path = path;
    }

    @Override
    public List<Path> getPaths() {
      return Collections.singletonList(this.path);
    }
  }
}
