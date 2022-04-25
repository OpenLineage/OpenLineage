package io.openlineage.client.transports;

import java.util.Properties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class KafkaConfig implements TransportConfig {
  @Getter @Setter private String topicName;
  @Getter @Setter private String localServerId;
  @Getter @Setter private Properties properties;

  public boolean hasLocalServerId() {
    return (localServerId != null);
  }
}
