package io.openlineage.client.transports;

import java.net.URL;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class HttpConfig implements TransportConfig {
  @Getter @Setter private URL url;
  @Getter @Setter private @Nullable TokenProvider auth;
}
