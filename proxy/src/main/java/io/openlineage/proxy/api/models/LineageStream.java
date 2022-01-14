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

package io.openlineage.proxy.api.models;

import lombok.NonNull;

/**
 * LineageStream provides the generic implementation of the backend destinations supported by the
 * proxy backend.
 */
public abstract class LineageStream {
  /**
   * The Type enum (and JsonSubTypes above) are extended for each new type of destination that the
   * proxy backend supports. There is a subtype class for each of these destination types.
   */
  enum Type {
    CONSOLE,
    KAFKA
  }

  private final Type type;

  /**
   * The constructor sets up the type for destination for logging purposes.
   *
   * @param type type of destination implemented by the subtype.
   */
  LineageStream(@NonNull final Type type) {
    this.type = type;
  }

  /**
   * Ths is the method that is called when a new lineage event is emitted from the data platform.
   * The specific destination class implements this method with the logic to send the event to its
   * supported destination.
   *
   * @param eventAsString the OpenLineage event as a {code string} value
   */
  public abstract void collect(String eventAsString);
}
