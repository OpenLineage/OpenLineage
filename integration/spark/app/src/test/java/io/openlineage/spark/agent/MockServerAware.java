package io.openlineage.spark.agent;

import org.mockserver.integration.ClientAndServer;

interface MockServerAware {
  void setClientAndServer(ClientAndServer clientAndServer);
}
