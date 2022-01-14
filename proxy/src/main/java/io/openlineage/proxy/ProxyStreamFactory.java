package io.openlineage.proxy;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

import com.google.common.collect.Sets;
import io.openlineage.proxy.ProxyStreamConfig;
import io.openlineage.proxy.api.models.ConsoleConfig;
import io.openlineage.proxy.api.models.ConsoleLineageStream;
import io.openlineage.proxy.api.models.KafkaConfig;
import io.openlineage.proxy.api.models.KafkaLineageStream;
import io.openlineage.proxy.api.models.LineageStream;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ProxyStreamFactory {
  private static final String DEFAULT_PROXY_LINEAGE_SOURCE = "openLineageProxyBackend";
  private static final Set<ProxyStreamConfig> DEFAULT_STREAMS = Sets.newHashSet(new ConsoleConfig());

  @Getter @Setter private String source = DEFAULT_PROXY_LINEAGE_SOURCE;
  @Getter @Setter private Set<ProxyStreamConfig> streams = DEFAULT_STREAMS;

  public ImmutableSet<LineageStream> build() {
    final ImmutableSet.Builder lineageStreams = ImmutableSet.builder();
    for (final ProxyStreamConfig config : streams) {
        if (config instanceof ConsoleConfig) {
          lineageStreams.add(new ConsoleLineageStream());
        } else if (config instanceof KafkaConfig) {
          final KafkaConfig kafkaConfig = (KafkaConfig) config;
          if (!kafkaConfig.hasLocalServerId()) {
            // ...
            kafkaConfig.setLocalServerId(source);
          }
          kafkaConfig.getProperties().put("bootstrap.servers", kafkaConfig.getBootstrapServerUrl());
          kafkaConfig.getProperties().put("server.id", kafkaConfig.getLocalServerId());
          lineageStreams.add(new KafkaLineageStream((KafkaConfig) config));
        }
    }
    return lineageStreams.build();
  }
}
