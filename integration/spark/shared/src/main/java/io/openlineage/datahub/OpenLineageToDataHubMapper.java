package io.openlineage.datahub;

import datahub.event.MetadataChangeProposalWrapper;
import datahub.shaded.com.google.common.collect.Streams;
import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpenLineageToDataHubMapper {

  public static List<MetadataChangeProposalWrapper> getMCPWs(OpenLineage.RunEvent runEvent) {

    Stream<MetadataChangeProposalWrapper> inputs =
        runEvent.getInputs().stream()
            .map(InputDatasetMapper::new)
            .flatMap(e -> e.mapDatasetToDataHubEvents().stream());

    Stream<MetadataChangeProposalWrapper> outputs =
        runEvent.getOutputs().stream()
            .map(o -> new OutputDatasetMapper(o, runEvent.getInputs()))
            .flatMap(e -> e.mapDatasetToDataHubEvents().stream());

    return Streams.concat(inputs, outputs).collect(Collectors.toList());
  }
}
