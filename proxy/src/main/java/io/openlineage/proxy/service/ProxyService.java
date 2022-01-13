/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.proxy.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.openlineage.proxy.ProxyConfig;
import io.openlineage.proxy.api.models.ConsoleLineageStream;
import io.openlineage.proxy.api.models.KafkaLineageStream;
import io.openlineage.proxy.api.models.LineageStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProxyService {
  private static final Logger log = LoggerFactory.getLogger(ProxyService.class);

  private final ImmutableSet<LineageStream> lineageStreams;

  /**
   * Constructor reviews the content of the config file and sets up the appropriate lineage streams.
   *
   * @param config configuration properties supplied to the application
   */
  public ProxyService(final ProxyConfig config) {
    /*
     * There are two supported lineage streams.  The first is logging to the console.  This is activated by default or if requested in the config.
     * The second is kafka.  At the moment, the Kafka configuration is set up to be mandatory and so the kafka stream is always set up.
     */
    if (config.getConsoleLog()) {
      this.lineageStreams =
          ImmutableSet.of(
              new ConsoleLineageStream(),
              new KafkaLineageStream(
                  config.getLineageSourceName(),
                  config.getKafkaTopicName(),
                  config.getKafkaBootstrapServerURL(),
                  config.getKafkaProperties()));
    } else {
      this.lineageStreams =
          ImmutableSet.of(
              new KafkaLineageStream(
                  config.getLineageSourceName(),
                  config.getKafkaTopicName(),
                  config.getKafkaBootstrapServerURL(),
                  config.getKafkaProperties()));
    }
  }

  /**
   * process an incoming event by sending it to all configured lineage streams.
   *
   * @param event incoming event
   * @return completion future
   */
  public CompletableFuture<Void> emitAsync(@NonNull String event) {
    log.info("Inbound event: {}", event);

    final List<CompletableFuture> collectionFutures = Lists.newArrayList();
    lineageStreams.forEach(
        lineageStream ->
            collectionFutures.add(CompletableFuture.runAsync(() -> lineageStream.collect(event))));
    return CompletableFuture.allOf(collectionFutures.toArray(CompletableFuture[]::new));
  }
}
