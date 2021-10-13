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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleLineageStream.class, name = "CONSOLE"),
  @JsonSubTypes.Type(value = KafkaLineageStream.class, name = "KAFKA"),
  @JsonSubTypes.Type(value = KinesisLineageStream.class, name = "KINESIS")
})
public abstract class LineageStream {
  enum Type {
    KAFKA,
    KINESIS,
    CONSOLE
  }

  private final Type type;

  LineageStream(@NonNull Type type) {
    this.type = type;
  }

  public abstract void collect(@NonNull LineageEvent event);
}
