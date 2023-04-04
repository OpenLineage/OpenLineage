package io.openlineage.datahub;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.Schemaless;
import datahub.event.MetadataChangeProposalWrapper;
import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.stream.Collectors;

public abstract class DatasetMapper {
  MetadataChangeProposalWrapper.AspectStepBuilder template;
  OpenLineage.Dataset dataset;

  public DatasetMapper(OpenLineage.Dataset dataset) {
    this.dataset = dataset;
    this.template =
        MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(DatasetMapperUtils.getUrn(dataset))
            .upsert();
  }

  public abstract List<MetadataChangeProposalWrapper> mapDatasetToDataHubEvents();

  MetadataChangeProposalWrapper getSchemaMetadata() {
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setSchemaless(new Schemaless());
    SchemaMetadata schemaMetadata =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    dataset.getFacets().getSchema().getFields().stream()
                        .map(f -> DatasetMapperUtils.getSchemaField(f))
                        .collect(Collectors.toList())))
            .setSchemaName("schema_" + dataset.getName())
            .setPlatform(new DataPlatformUrn(DatasetMapperUtils.getPlatform(dataset)))
            .setVersion(0)
            .setHash("hash" + dataset.getName())
            .setPlatformSchema(platformSchema);
    return template.aspect(schemaMetadata).build();
  }
}
