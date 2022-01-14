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
import io.openlineage.proxy.api.models.LineageStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ProxyService {
  private final ImmutableSet<LineageStream> lineageStreams;

  /**
   * Constructor reviews the content of the config file and sets up the appropriate lineage streams.
   *
   * @param config configuration properties supplied to the application
   */
  public ProxyService(@NonNull final ProxyConfig config) {
    this.lineageStreams = config.getProxyStreamFactory().build();
  }

  /**
   * process an incoming event by sending it to all configured lineage streams.
   *
   * @param eventAsString incoming event
   * @return completion future
   */
  public CompletableFuture<Void> proxyEventAsync(@NonNull String eventAsString) {
    log.info("Inbound event: {}", eventAsString);

    final List<CompletableFuture> collectionFutures = Lists.newArrayList();
    lineageStreams.forEach(
        lineageStream ->
            collectionFutures.add(
                CompletableFuture.runAsync(() -> lineageStream.collect(eventAsString))));
    return CompletableFuture.allOf(collectionFutures.toArray(CompletableFuture[]::new));
  }
}
