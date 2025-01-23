package io.openlineage.client.circuitBreaker;

public class TaskQueueCircuitBreakerBuilder implements CircuitBreakerBuilder {
  @Override
  public String getType() {
    return "asyncTaskQueue";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return new TaskQueueCircuitBreakerConfig();
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new TaskQueueCircuitBreaker((TaskQueueCircuitBreakerConfig) config);
  }
}
