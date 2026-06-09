package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;

import java.util.*;
import java.util.function.BiFunction;

public class EventMergerV1 {

    public static OpenLineage.DatasetEvent combine(OpenLineage ol, OpenLineage.DatasetEvent left, OpenLineage.DatasetEvent right) {
        return right;
    }

    public static OpenLineage.RunEvent combine(OpenLineage ol, OpenLineage.RunEvent lft, OpenLineage.RunEvent rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newRunEventBuilder();

            var enableDeepMerge = Objects.equals(System.getenv().getOrDefault("OPENLINEAGE__DEEP_MERGE", "false"), "true");
            List<OpenLineage.InputDataset> inputs;
            List<OpenLineage.OutputDataset> outputs;

            if (enableDeepMerge) {
                var inputsDict = new HashMap<String, OpenLineage.InputDataset>();
                var outputsDict = new HashMap<String, OpenLineage.OutputDataset>();

                left.getInputs().forEach(v -> mergeOrPut(inputsDict, v, (l, r) -> EventMergerV1.combine(ol, l, r)));
                right.getInputs().forEach(v -> mergeOrPut(inputsDict, v, (l, r) -> EventMergerV1.combine(ol, l, r)));

                left.getOutputs().forEach(v -> mergeOrPut(outputsDict, v, (l, r) -> EventMergerV1.combine(ol, l, r)));
                right.getOutputs().forEach(v -> mergeOrPut(outputsDict, v, (l, r) -> EventMergerV1.combine(ol, l, r)));

                inputs = new ArrayList<>(inputsDict.values());
                outputs = new ArrayList<>(outputsDict.values());
            } else {
                inputs = new ArrayList<>(left.getInputs());
                inputs.addAll(right.getInputs());

                outputs = new ArrayList<>(left.getOutputs());
                outputs.addAll(right.getOutputs());
            }

            return builder
                    .eventTime(right.getEventTime())
                    .eventType(right.getEventType())
                    .run(right.getRun())
                    .job(right.getJob())
                    .inputs(inputs)
                    .outputs(outputs)
                    .build();
        });
    }

    public static OpenLineage.OutputDataset combine(OpenLineage ol, OpenLineage.OutputDataset lft, OpenLineage.OutputDataset rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newOutputDatasetBuilder();
            return builder
                    .name(right.getName())
                    .namespace(right.getNamespace())
                    .outputFacets(combine(ol, left.getOutputFacets(), right.getOutputFacets()))
                    .facets(combine(ol, left.getFacets(), right.getFacets()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.OutputDatasetOutputFacets combine(OpenLineage ol, OpenLineage.OutputDatasetOutputFacets lft, OpenLineage.OutputDatasetOutputFacets rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newOutputDatasetOutputFacetsBuilder();
            return builder
                    .icebergCommitReport(combine(ol, left.getIcebergCommitReport(), right.getIcebergCommitReport()))
                    .outputStatistics(combine(ol, left.getOutputStatistics(), right.getOutputStatistics()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.OutputStatisticsOutputDatasetFacet combine(OpenLineage ol, OpenLineage.OutputStatisticsOutputDatasetFacet lft, OpenLineage.OutputStatisticsOutputDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newOutputStatisticsOutputDatasetFacetBuilder();
            return builder
                    .fileCount(left.getFileCount() + right.getFileCount())
                    .rowCount(left.getRowCount() + right.getRowCount())
                    .size(left.getSize() + right.getSize())
                    .build()
                    ;
        });
    }

    public static OpenLineage.IcebergCommitReportOutputDatasetFacet combine(OpenLineage ol, OpenLineage.IcebergCommitReportOutputDatasetFacet left, OpenLineage.IcebergCommitReportOutputDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.InputDataset combine(OpenLineage ol, OpenLineage.InputDataset lft, OpenLineage.InputDataset rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newInputDatasetBuilder();
            return builder
                    .name(right.getName())
                    .namespace(right.getNamespace())
                    .inputFacets(combine(ol, left.getInputFacets(), right.getInputFacets()))
                    .facets(combine(ol, left.getFacets(), right.getFacets()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.InputDatasetInputFacets combine(OpenLineage ol, OpenLineage.InputDatasetInputFacets lft, OpenLineage.InputDatasetInputFacets rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newInputDatasetInputFacetsBuilder();
            return builder
                    .subset(combine(ol, left.getSubset(), left.getSubset()))
                    .dataQualityMetrics(combine(ol, left.getDataQualityMetrics(), left.getDataQualityMetrics()))
                    .dataQualityAssertions(combine(ol, left.getDataQualityAssertions(), left.getDataQualityAssertions()))
                    .icebergScanReport(combine(ol, left.getIcebergScanReport(), right.getIcebergScanReport()))
                    .inputStatistics(combine(ol, left.getInputStatistics(), right.getInputStatistics()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.InputStatisticsInputDatasetFacet combine(OpenLineage ol, OpenLineage.InputStatisticsInputDatasetFacet lft, OpenLineage.InputStatisticsInputDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newInputStatisticsInputDatasetFacetBuilder();
            return builder
                    .fileCount(left.getFileCount() + right.getFileCount())
                    .rowCount(left.getRowCount() + right.getRowCount())
                    .size(left.getSize() + right.getSize())
                    .build()
                    ;
        });
    }

    public static OpenLineage.IcebergScanReportInputDatasetFacet combine(OpenLineage ol, OpenLineage.IcebergScanReportInputDatasetFacet left, OpenLineage.IcebergScanReportInputDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DataQualityAssertionsDatasetFacet combine(OpenLineage ol, OpenLineage.DataQualityAssertionsDatasetFacet lft, OpenLineage.DataQualityAssertionsDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newDataQualityAssertionsDatasetFacetBuilder();
            var items = new ArrayList<>(left.getAssertions());
            items.addAll(right.getAssertions());

            return builder
                    .assertions(items)
                    .build();
        });
    }

    public static OpenLineage.DataQualityMetricsInputDatasetFacet combine(OpenLineage ol, OpenLineage.DataQualityMetricsInputDatasetFacet lft, OpenLineage.DataQualityMetricsInputDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newDataQualityMetricsInputDatasetFacetBuilder();
            return builder
                    .columnMetrics(combine(ol, left.getColumnMetrics(), left.getColumnMetrics()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics combine(OpenLineage ol, OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics lft, OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder();

            left.getAdditionalProperties().forEach((k, v) -> builder.put(k, v));
            right.getAdditionalProperties().forEach((k, v) -> builder.put(k, v));

            return builder
                    .build()
                    ;
        });
    }

    public static OpenLineage.InputSubsetInputDatasetFacet combine(OpenLineage ol, OpenLineage.InputSubsetInputDatasetFacet lft, OpenLineage.InputSubsetInputDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newInputSubsetInputDatasetFacetBuilder();
            return builder
                    .inputCondition(combine(ol, left.getInputCondition(), left.getInputCondition()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.LocationSubsetCondition combine(OpenLineage ol, OpenLineage.LocationSubsetCondition lft, OpenLineage.LocationSubsetCondition rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newLocationSubsetConditionBuilder();
            var locations = new HashSet<>(left.getLocations());
            locations.addAll(right.getLocations());

            return builder
                    .locations(new ArrayList<>(locations))
                    .build()
                    ;
        });
    }

    public static OpenLineage.DatasetFacets combine(OpenLineage ol, OpenLineage.DatasetFacets lft, OpenLineage.DatasetFacets rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newDatasetFacetsBuilder();

            return builder
                    .dataSource(right.getDataSource())
                    .version(right.getVersion())
                    .datasetType(right.getDatasetType())
                    .storage(right.getStorage())
                    .lifecycleStateChange(right.getLifecycleStateChange())
                    .documentation(right.getDocumentation())
                    .ownership(right.getOwnership())
                    .schema(combine(ol, left.getSchema(), right.getSchema())) // no need to merge, enriching happening on lineage service
                    .tags(combine(ol, left.getTags(), right.getTags()))
                    .symlinks(combine(ol, left.getSymlinks(), right.getSymlinks()))
                    .columnLineage(combine(ol, left.getColumnLineage(), right.getColumnLineage()))
                    .catalog(combine(ol, left.getCatalog(), right.getCatalog()))
                    .dataQualityMetrics(combine(ol, left.getDataQualityMetrics(), right.getDataQualityMetrics()))
                    .hierarchy(combine(ol, left.getHierarchy(), right.getHierarchy()))
                    .build()
                    ;
        });
    }

    public static OpenLineage.HierarchyDatasetFacet combine(OpenLineage ol, OpenLineage.HierarchyDatasetFacet left, OpenLineage.HierarchyDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DataQualityMetricsDatasetFacet combine(OpenLineage ol, OpenLineage.DataQualityMetricsDatasetFacet left, OpenLineage.DataQualityMetricsDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.CatalogDatasetFacet combine(OpenLineage ol, OpenLineage.CatalogDatasetFacet left, OpenLineage.CatalogDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.SchemaDatasetFacet combine(OpenLineage ol, OpenLineage.SchemaDatasetFacet left, OpenLineage.SchemaDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.TagsDatasetFacet combine(OpenLineage ol, OpenLineage.TagsDatasetFacet lft, OpenLineage.TagsDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newTagsDatasetFacetBuilder();
            var merge = new HashMap<String, OpenLineage.TagsDatasetFacetFields>();

            left.getTags().forEach(v -> merge.put(v.getKey(), v));
            right.getTags().forEach(v -> merge.put(v.getKey(), v));

            return builder
                    .tags(new ArrayList<>(merge.values()))
                    .build();
        });
    }

    public static OpenLineage.SymlinksDatasetFacet combine(OpenLineage ol, OpenLineage.SymlinksDatasetFacet lft, OpenLineage.SymlinksDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newSymlinksDatasetFacetBuilder();
            var merge = new HashMap<String, OpenLineage.SymlinksDatasetFacetIdentifiers>();

            left.getIdentifiers().forEach(v -> merge.put(v.getName(), v));
            right.getIdentifiers().forEach(v -> merge.put(v.getName(), v));

            return builder
                    .identifiers(new ArrayList<>(merge.values()))
                    .build();
        });
    }

    public static OpenLineage.TagsJobFacet combine(OpenLineage ol, OpenLineage.TagsJobFacet lft, OpenLineage.TagsJobFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newTagsJobFacetBuilder();
            var merge = new HashMap<String, OpenLineage.TagsJobFacetFields>();

            left.getTags().forEach(v -> merge.put(v.getKey(), v));
            right.getTags().forEach(v -> merge.put(v.getKey(), v));

            return builder
                    .tags(new ArrayList<>(merge.values()))
                    .build();
        });
    }

    public static OpenLineage.ColumnLineageDatasetFacet combine(OpenLineage ol, OpenLineage.ColumnLineageDatasetFacet lft, OpenLineage.ColumnLineageDatasetFacet rght) {
        return safeMerge(lft, rght, (left, right) -> {
            var builder = ol.newColumnLineageDatasetFacetBuilder();
            var datasets = new ArrayList<>(left.getDataset());
            datasets.addAll(right.getDataset());

            var merge = ol.newColumnLineageDatasetFacetFieldsBuilder();

            left.getFields().getAdditionalProperties().forEach((k, v) -> merge.put(k, v));
            right.getFields().getAdditionalProperties().forEach((k, v) -> merge.put(k, v));

            return builder
                    .dataset(datasets)
                    .fields(merge.build())
                    .build();
        });
    }

    private static <T extends OpenLineage.Dataset> void put(Map<String, T> dict, T entry) {
        if (entry != null) {
            dict.put(entry.getNamespace() + "." + entry.getName(), entry);
        }
    }

    private static <T extends OpenLineage.Dataset> void mergeOrPut(Map<String, T> dict, T entry, BiFunction<T, T, T> merge) {
        if (entry != null) {
            var key = entry.getNamespace() + "." + entry.getName();
            if (dict.containsKey(key)) {
                dict.put(key, merge.apply(dict.get(key), entry));
            } else {
                dict.put(key, entry);
            }
        }
    }

    private static <T> T safeMerge(T left, T right, BiFunction<T, T, T> merge) {
        if (left != null && right != null) {
            return merge.apply(left, right);
        } else if (right != null) {
            return right;
        } else {
            return left;
        }
    }
}
