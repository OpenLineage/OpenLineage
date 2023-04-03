package io.openlineage.datahub;

import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.schema.SchemaMetadata;
import datahub.event.MetadataChangeProposalWrapper;
import io.openlineage.client.OpenLineage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OutputDatasetMapper extends DatasetMapper{
    List<OpenLineage.InputDataset> inputs;
    public OutputDatasetMapper(OpenLineage.OutputDataset dataset, List<OpenLineage.InputDataset> inputs){
        super(dataset);
        this.inputs = inputs;
    }

    public List<MetadataChangeProposalWrapper> mapDatasetToDataHubEvents(){
        MetadataChangeProposalWrapper schemaMetadata = getSchemaMetadata();
        MetadataChangeProposalWrapper upstreams = getUpstreams();
        return Arrays.asList(schemaMetadata, upstreams);
    }

    private MetadataChangeProposalWrapper getUpstreams() {
        UpstreamArray upstreams = new UpstreamArray();
        upstreams.addAll(inputs.stream().map(DatasetMapperUtils::getUrn).map(e-> new Upstream().setDataset(e).setType(DatasetLineageType.TRANSFORMED)).collect(Collectors.toList()));
        return template.aspect(new UpstreamLineage().setUpstreams(upstreams)).build();
    }

}
