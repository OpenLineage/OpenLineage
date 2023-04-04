package io.openlineage.datahub;

import datahub.event.MetadataChangeProposalWrapper;
import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.List;

public class InputDatasetMapper extends DatasetMapper {

  public InputDatasetMapper(OpenLineage.Dataset dataset) {
    super(dataset);
  }

  public List<MetadataChangeProposalWrapper> mapDatasetToDataHubEvents() {
    MetadataChangeProposalWrapper schemaMetadata = getSchemaMetadata();

    return Collections.singletonList(schemaMetadata);
  }
}
