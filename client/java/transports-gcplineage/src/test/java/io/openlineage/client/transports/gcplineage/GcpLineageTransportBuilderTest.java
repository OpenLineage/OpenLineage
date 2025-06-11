package io.openlineage.client.transports.gcplineage;

import static io.openlineage.client.OpenLineageClientUtils.newObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.gcplineage.GcpLineageTransportConfig.Mode;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class GcpLineageTransportBuilderTest {

  private static final ObjectMapper YML = newObjectMapper(new YAMLFactory());

  @ParameterizedTest
  @CsvSource({"config/lowercase_async_config.yaml", "config/uppercase_async_config.yaml"})
  public void buildFromYamlConfigLowercaseMode(String relativePath)
      throws URISyntaxException {
    Path path = Paths.get(
        this.getClass().getClassLoader().getResource(relativePath).toURI());

    OpenLineageConfig openLineageConfig = OpenLineageClientUtils.loadOpenLineageConfigYaml(
        () -> Collections.singletonList(path),
        new TypeReference<OpenLineageConfig>() {
        });

    GcpLineageTransportConfig gcpLineageTransportConfig = (GcpLineageTransportConfig) openLineageConfig.getTransportConfig();
    assertEquals(Mode.ASYNC, gcpLineageTransportConfig.getMode());
  }

}
