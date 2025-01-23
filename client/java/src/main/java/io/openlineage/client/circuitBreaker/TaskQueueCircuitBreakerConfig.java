package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.CircuitBreaker.CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

import io.openlineage.client.MergeConfig;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class TaskQueueCircuitBreakerConfig
    implements CircuitBreakerConfig, MergeConfig<TaskQueueCircuitBreakerConfig> {
  private static final Integer DEFAULT_THREAD_COUNT = 2;
  private static final Integer DEFAULT_QUEUE_SIZE = 2;
  private static final Long DEFAULT_TIMEOUT = 1L;
  private static final Long DEFAULT_SHUTDOWN_TIMEOUT = 60L;

  @Getter @Setter private Integer threadCount = DEFAULT_THREAD_COUNT;
  @Getter @Setter private Integer queueSize = DEFAULT_QUEUE_SIZE;
  @Getter @Setter private Long timeoutSeconds = DEFAULT_TIMEOUT;
  @Getter @Setter private Long shutdownTimeoutSeconds = DEFAULT_SHUTDOWN_TIMEOUT;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

  @Override
  public TaskQueueCircuitBreakerConfig mergeWithNonNull(TaskQueueCircuitBreakerConfig other) {
    return new TaskQueueCircuitBreakerConfig(
        mergeWithDefaultValue(threadCount, other.threadCount, DEFAULT_THREAD_COUNT),
        mergeWithDefaultValue(this.queueSize, other.queueSize, DEFAULT_QUEUE_SIZE),
        mergeWithDefaultValue(this.timeoutSeconds, other.timeoutSeconds, DEFAULT_TIMEOUT),
        mergeWithDefaultValue(
            this.shutdownTimeoutSeconds, other.shutdownTimeoutSeconds, DEFAULT_SHUTDOWN_TIMEOUT),
        mergeWithDefaultValue(
            this.circuitCheckIntervalInMillis,
            other.circuitCheckIntervalInMillis,
            CIRCUIT_CHECK_INTERVAL_IN_MILLIS));
  }
}
