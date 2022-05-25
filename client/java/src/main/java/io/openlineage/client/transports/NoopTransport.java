package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopTransport extends Transport {
  public NoopTransport() {
    super(Type.NOOP);
    log.info("OpenLineage client is disabled");
  }

  @Override
  public void emit(OpenLineage.RunEvent runEvent) {}
}
