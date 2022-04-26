package io.openlineage.client.transports;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public class ApiKeyTokenProvider implements TokenProvider {
  @Getter @Setter private String apiKey;

  @Override
  public String getToken() {
    return String.format("Bearer %s", apiKey);
  }
}
