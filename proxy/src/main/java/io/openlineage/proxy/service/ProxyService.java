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
import io.openlineage.proxy.api.models.LineageEvent;
import io.openlineage.proxy.api.models.LineageStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public final class ProxyService {
  private final ImmutableSet<LineageStream> lineageStreams;

  public ProxyService(final ProxyConfig config) {
    this.lineageStreams = config.getLineageStreams();
  }

  public CompletableFuture<Void> emitAsync(@NonNull LineageEvent event) {
    final List<CompletableFuture> collectionFutures = Lists.newArrayList();
    lineageStreams.forEach(
        lineageStream ->
            collectionFutures.add(CompletableFuture.runAsync(() -> lineageStream.collect(event))));
    return CompletableFuture.allOf(collectionFutures.toArray(CompletableFuture[]::new));
  }
}
