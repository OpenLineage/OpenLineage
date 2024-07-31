package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.namespace.resolver.HostListNamespaceResolverConfig;
import io.openlineage.spark.api.DatasetFactory;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KinesisMicroBatchStreamStrategy extends StreamStrategy {

    public KinesisMicroBatchStreamStrategy(
            DatasetFactory<OpenLineage.InputDataset> inputDatasetDatasetFactory,
            StreamingDataSourceV2Relation relation) {
        super(inputDatasetDatasetFactory, relation);

        new HostListNamespaceResolverConfig();
    }

    @Override
    public List<OpenLineage.InputDataset> getInputDatasets() {
        if (relation.stream() == null) {
            return Collections.emptyList();
        }

        Optional<Object> options = tryReadField(relation.stream(), "options");
        if (!options.isPresent()) {
            return Collections.emptyList();
        }

        Optional<String> endpointUrl = tryReadField(options.get(), "endpointUrl");
        Optional<String> streamName = tryReadField(options.get(), "streamName");

        return Collections.singletonList(datasetFactory.getDataset(
                streamName.orElse(""),
                "kinesis://" + endpointUrl.map(URI::create)
                        .map(uri -> isValidPort(uri.getPort()) ? uri.getHost() + ":" + uri.getPort() : uri.getHost()).
                        orElse(""),
                relation.schema()
        ));
    }

    boolean isValidPort(int port) {
        return port != 443 && port != 80 && port > 0;
    }

    private <T> Optional<T> tryReadField(Object target, String fieldName) {
        try {
            T value = (T) FieldUtils.readField(target, fieldName, true);
            return Optional.ofNullable(value);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        } catch (IllegalAccessException e) {
            return Optional.empty();
        }
    }
}
