/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.transport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DummyTransport extends Transport {

  @Getter
  private static final List<OpenLineage.BaseEvent> events =
      Collections.synchronizedList(new ArrayList<>());

  public void emit(@NonNull OpenLineage.BaseEvent event) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Object jsonObject = mapper.readValue(OpenLineageClientUtils.toJson(event), Object.class);
      String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
      System.out.println(prettyJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    events.add(event);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit((OpenLineage.BaseEvent) runEvent);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit((OpenLineage.BaseEvent) datasetEvent);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit((OpenLineage.BaseEvent) jobEvent);
  }

  public static OpenLineage.RunEvent getLastEvent() {
    return (OpenLineage.RunEvent) events.get(events.size() - 1);
  }

  public static OpenLineage.OutputDataset getOutputDataset(String output) {
    OpenLineage.RunEvent lastEvent = getLastEvent();
    for (OpenLineage.OutputDataset od : lastEvent.getOutputs()) {
      if (od.getName().endsWith("/" + output)) {
        return od;
      }
    }
    throw new RuntimeException("Could not find output: " + output);
  }

  public static OpenLineage.ColumnLineageDatasetFacet getColumnLineage(String output) {
    return getOutputDataset(output).getFacets().getColumnLineage();
  }
}
