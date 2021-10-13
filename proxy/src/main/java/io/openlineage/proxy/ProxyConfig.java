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

package io.openlineage.proxy;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.Configuration;
import io.openlineage.proxy.api.models.ConsoleLineageStream;
import io.openlineage.proxy.api.models.LineageStream;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public final class ProxyConfig extends Configuration {
  private static final ImmutableSet<LineageStream> DEFAULT_LINEAGE_STREAMS =
      ImmutableSet.of(new ConsoleLineageStream());

  @Getter private ImmutableSet<LineageStream> lineageStreams = DEFAULT_LINEAGE_STREAMS;
}
