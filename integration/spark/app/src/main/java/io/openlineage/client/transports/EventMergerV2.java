package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.TagField;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
@Slf4j
public class EventMergerV2 {

    public static OpenLineage.TagsDatasetFacet combine(OpenLineage ol, OpenLineage.TagsDatasetFacet left, OpenLineage.TagsDatasetFacet right) {
        return merge(right, left, (l, r) -> {
            Map<String, OpenLineage.TagsDatasetFacetFields> merge = new HashMap<>();
            if (l != null && l.getTags() != null) {
                l.getTags().forEach(v -> merge.put(v.getKey(), v));
            }
            if (r != null && r.getTags() != null) {
                r.getTags().forEach(v -> merge.put(v.getKey(), v));
            }
            var builder = ol.newTagsDatasetFacetBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            var preparedTags = new ArrayList<>(merge.values());
            preparedTags.forEach(x -> {
                builder.put(x.getKey(), x.getValue());
            });

            builder.tags(new ArrayList<>(merge.values()));
            return builder.build();
        });
    }

    public static OpenLineage.SymlinksDatasetFacet combineAndAdd(OpenLineage ol, OpenLineage.SymlinksDatasetFacet old, OpenLineage.SymlinksDatasetFacet newData, List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlink) {
        var combined = combine(ol, old, newData);

        if (symlink != null && !symlink.isEmpty()) {
            combined = combine(ol, combined, ol.newSymlinksDatasetFacet(symlink));
        }
        return combined;
    }

    public static OpenLineage.TagsJobFacet combine(OpenLineage ol, OpenLineage.TagsJobFacet left, OpenLineage.TagsJobFacet right) {
        return combineWithTags(ol, left, right, ol.newTagsJobFacet(new ArrayList<>()));
    }

    public static OpenLineage.TagsJobFacet combineWithTags(OpenLineage ol, OpenLineage.TagsJobFacet left, OpenLineage.TagsJobFacet right, OpenLineage.TagsJobFacet extra) {
        return merge(right, left, (l, r) -> {

            Map<String, OpenLineage.TagsJobFacetFields> merge = new HashMap<>();
            l.getTags().forEach(v -> {
                        if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue())) {
                            merge.put(v.getKey(), v);
                        }
                    }
            );
            r.getTags().forEach(v -> {
                if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue()))
                    merge.put(v.getKey(), v);
            });
            if (extra != null) {
                extra.getTags().forEach(v -> {
                    if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue())) {
                        merge.put(v.getKey(), v);
                    }
                });
            }

            var builder = ol.newTagsJobFacetBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            var preparedTags = new ArrayList<>(merge.values());
            preparedTags.forEach(x -> {
                builder.put(x.getKey(), x.getValue());
            });


            return builder
                    .tags(preparedTags)
                    .build();
        });
    }

    public static OpenLineage.RunEvent combine(OpenLineage ol,
                                               OpenLineage.RunEvent evt,
                                               Optional<OpenLineage.TagsJobFacet> jobTagsOpt,
                                               Optional<OpenLineage.TagsRunFacet> runTagsOpt
    ) {
        var jobTags = prepareJobsTags(
                ol,
                Optional.ofNullable(evt.getJob())
                        .map(x -> x.getFacets())
                        .map(x -> x.getTags()),
                jobTagsOpt
        );


        var runTags = prepareRunTags(ol,
                Optional.ofNullable(evt.getRun())
                        .map(x -> x.getFacets())
                        .map(x -> x.getTags()),
                runTagsOpt
        );


        var runFacets = ol.newRunFacetsBuilder()
                .parent(evt.getRun().getFacets().getParent())
                .tags(runTags)
                .externalQuery(evt.getRun().getFacets().getExternalQuery())
                .gcp_dataproc(evt.getRun().getFacets().getGcp_dataproc())
                .gcp_composer_run(evt.getRun().getFacets().getGcp_composer_run())
                .extractionError(evt.getRun().getFacets().getExtractionError())
                .nominalTime(evt.getRun().getFacets().getNominalTime())
                .errorMessage(evt.getRun().getFacets().getErrorMessage())
                .environmentVariables(evt.getRun().getFacets().getEnvironmentVariables())
                .executionParameters(evt.getRun().getFacets().getExecutionParameters())
                .test(evt.getRun().getFacets().getTest())
                .processing_engine(evt.getRun().getFacets().getProcessing_engine())
                .jobDependencies(evt.getRun().getFacets().getJobDependencies());

        // TODO: causes duplicates
        // evt.getRun().getFacets().getAdditionalProperties().forEach(runFacets::put);

        var run = ol.newRunBuilder()
                .runId(evt.getRun().getRunId())
                .facets(runFacets.build())
                .build();
        var jobFacets = ol.newJobFacetsBuilder()
                .jobType(evt.getJob().getFacets().getJobType())
                .tags(jobTags)
                .ownership(evt.getJob().getFacets().getOwnership())
                .documentation(evt.getJob().getFacets().getDocumentation())
                .sourceCode(evt.getJob().getFacets().getSourceCode())
                .sourceCodeLocation(evt.getJob().getFacets().getSourceCodeLocation())
                .gcp_lineage(evt.getJob().getFacets().getGcp_lineage())
                .gcp_composer_job(evt.getJob().getFacets().getGcp_composer_job())
                .sql(evt.getJob().getFacets().getSql());

        // TODO: causes duplicates
        // evt.getJob().getFacets().getAdditionalProperties().forEach(jobFacets::put);

        var job = ol.newJobBuilder()
                .namespace(evt.getJob().getNamespace())
                .name(evt.getJob().getName())
                .facets(jobFacets.build())
                .build();

        return ol.newRunEventBuilder()
                .eventTime(evt.getEventTime())
                .eventType(evt.getEventType())
                .run(run)
                .job(job)
                .inputs(evt.getInputs())
                .outputs(evt.getOutputs())
                .build();
    }

    private static OpenLineage.TagsJobFacet prepareJobsTags(
            OpenLineage ol,
            Optional<OpenLineage.TagsJobFacet> parent,
            Optional<OpenLineage.TagsJobFacet> extra
    ) {
        var r = extra.map(x -> {
            var b = ol.newTagsJobFacetBuilder();
            merge(
                    x.getAdditionalProperties(),
                    parent
                            .map(OpenLineage.TagsJobFacet::getAdditionalProperties)
                            .orElse(new HashMap<>()),
                    b::put
            );

            var tags = new HashMap<String, OpenLineage.TagsJobFacetFields>();
            x.getTags().forEach(f -> tags.put(f.getKey(), f));
            parent.ifPresent(i -> i.getTags().forEach(f -> tags.put(f.getKey(), f)));

            return b
                    .tags(new ArrayList<>(tags.values()))
                    .build();
        }).or(() -> parent).orElseGet(() -> ol.newTagsJobFacetBuilder()
                .tags(new ArrayList<>())
                .build()
        );

        r.getTags().forEach(x -> log.debug("MERGED JOB TAG: {} - {}", x.getKey(), x.getValue()));

        return r;
    }

    private static OpenLineage.TagsRunFacet prepareRunTags(
            OpenLineage ol,
            Optional<OpenLineage.TagsRunFacet> parent,
            Optional<OpenLineage.TagsRunFacet> extra
    ) {
        var r = extra.map(x -> {
            var b = ol.newTagsRunFacetBuilder();
            merge(
                    x.getAdditionalProperties(),
                    parent
                            .map(OpenLineage.TagsRunFacet::getAdditionalProperties)
                            .orElse(new HashMap<>()),
                    b::put
            );

            var tags = new HashMap<String, OpenLineage.TagsRunFacetFields>();
            x.getTags().forEach(f -> tags.put(f.getKey(), f));
            parent.ifPresent(i -> i.getTags().forEach(f -> tags.put(f.getKey(), f)));

            return b
                    .tags(new ArrayList<>(tags.values()))
                    .build();
        }).or(() -> parent).orElseGet(() -> ol.newTagsRunFacetBuilder()
                .tags(new ArrayList<>())
                .build()
        );

        r.getTags().forEach(x -> log.debug("MERGED RUN TAG: {} - {}", x.getKey(), x.getValue()));

        return r;
    }

    public static OpenLineage.JobEvent combine(OpenLineage ol,
                                               OpenLineage.JobEvent evt,
                                               Optional<OpenLineage.TagsJobFacet> jobTagsOpt
    ) {
        var jobTags = prepareJobsTags(
                ol,
                Optional.ofNullable(evt.getJob())
                        .map(OpenLineage.Job::getFacets)
                        .map(OpenLineage.JobFacets::getTags),
                jobTagsOpt);


        var job = ol.newJobBuilder()
                .namespace(evt.getJob().getNamespace())
                .name(evt.getJob().getName())
                .facets(
                        ol.newJobFacetsBuilder()
                                .jobType(evt.getJob().getFacets().getJobType())
                                .tags(jobTags)
                                .ownership(evt.getJob().getFacets().getOwnership())
                                .documentation(evt.getJob().getFacets().getDocumentation())
                                .sourceCode(evt.getJob().getFacets().getSourceCode())
                                .sourceCodeLocation(evt.getJob().getFacets().getSourceCodeLocation())
                                .gcp_lineage(evt.getJob().getFacets().getGcp_lineage())
                                .gcp_composer_job(evt.getJob().getFacets().getGcp_composer_job())
                                .sql(evt.getJob().getFacets().getSql())
                                .build()
                )
                .build();


        return ol.newJobEventBuilder()
                .eventTime(evt.getEventTime())
                .job(job)
                .inputs(evt.getInputs())
                .outputs(evt.getOutputs())
                .build();

    }

    public static OpenLineage.RunEvent combine(OpenLineage ol, OpenLineage.RunEvent left, OpenLineage.RunEvent right) {
        return merge(left, right, (l, r) -> {
            List<OpenLineage.InputDataset> inputs = mergeInputs(ol, left.getInputs(), right.getInputs());
            List<OpenLineage.OutputDataset> outputs = mergeOutputs(ol, left.getOutputs(), right.getOutputs());
            OpenLineage.Run run = combine(ol, left.getRun(), right.getRun());
            OpenLineage.Job job = combine(ol, left.getJob(), right.getJob());

            return ol.newRunEventBuilder()
                    .eventTime(right.getEventTime())
                    .eventType(right.getEventType())
                    .run(run)
                    .job(job)
                    .inputs(inputs)
                    .outputs(outputs)
                    .build();
        });
    }

    public static OpenLineage.JobEvent combine(OpenLineage ol, OpenLineage.JobEvent left, OpenLineage.JobEvent right) {
        return merge(left, right, (l, r) -> ol.newJobEventBuilder()
                .job(combine(ol, l.getJob(), r.getJob()))
                .eventTime(r.getEventTime())
                .inputs(mergeInputs(ol, left.getInputs(), right.getInputs()))
                .outputs(mergeOutputs(ol, left.getOutputs(), right.getOutputs()))
                .build());
    }

    public static OpenLineage.Job combine(OpenLineage ol, OpenLineage.Job left, OpenLineage.Job right) {
        return merge(left, right, (l, r) -> ol.newJobBuilder()
                .namespace(right.getNamespace())
                .name(right.getName())
                .facets(combine(ol, left.getFacets(), right.getFacets()))
                .build());
    }

    public static OpenLineage.JobFacets combine(OpenLineage ol, OpenLineage.JobFacets left, OpenLineage.JobFacets right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newJobFacetsBuilder()
                    .ownership(right.getOwnership())
                    .documentation(right.getDocumentation())
                    .jobType(right.getJobType())
                    .tags(combine(ol, left.getTags(), right.getTags()))
                    .sourceCode(right.getSourceCode())
                    .sql(right.getSql())
                    .gcp_lineage(right.getGcp_lineage())
                    .gcp_composer_job(right.getGcp_composer_job())
                    .sourceCodeLocation(right.getSourceCodeLocation());

//            merge(
//                    l.getAdditionalProperties(),
//                    r.getAdditionalProperties(),
//                    builder::put
//            );

            return builder.build();
        });
    }

    public static OpenLineage.Run combine(OpenLineage ol, OpenLineage.Run left, OpenLineage.Run right) {
        return ol.newRunBuilder()
                .runId(right.getRunId())
                .facets(combine(ol, left.getFacets(), right.getFacets()))
                .build();
    }

    public static OpenLineage.RunFacets combine(OpenLineage ol, OpenLineage.RunFacets left, OpenLineage.RunFacets right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newRunFacetsBuilder()
                    .environmentVariables(right.getEnvironmentVariables())
                    .errorMessage(right.getErrorMessage())
                    .externalQuery(right.getExternalQuery())
                    .extractionError(right.getExtractionError())
                    .executionParameters(right.getExecutionParameters())
                    .gcp_dataproc(right.getGcp_dataproc())
                    .nominalTime(right.getNominalTime())
                    .parent(right.getParent())
                    .processing_engine(right.getProcessing_engine())
                    .tags(combine(ol, left.getTags(), right.getTags()));

//            merge(
//                    l.getAdditionalProperties(),
//                    r.getAdditionalProperties(),
//                    builder::put
//            );

            return builder.build();
        });
    }

    public static OpenLineage.TagsRunFacet combine(OpenLineage ol, OpenLineage.TagsRunFacet left, OpenLineage.TagsRunFacet right) {
        return combineWithTags(ol, left, right, ol.newTagsRunFacet(new ArrayList<>()));
    }

    public static OpenLineage.TagsRunFacet combineWithTags(OpenLineage ol, OpenLineage.TagsRunFacet left, OpenLineage.TagsRunFacet right, OpenLineage.TagsRunFacet extra) {
        return merge(left, right, (l, r) -> {

            Map<String, OpenLineage.TagsRunFacetFields> merge = new HashMap<>();
            l.getTags().forEach(v -> {
                        if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue())) {
                            merge.put(v.getKey(), v);
                        }
                    }
            );

            r.getTags().forEach(v -> {
                if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue()))
                    merge.put(v.getKey(), v);
            });

            if (extra != null) {
                extra.getTags().forEach(v -> {
                    if (!merge.containsKey(v.getKey()) && Objects.nonNull(v.getValue())) {
                        merge.put(v.getKey(), v);
                    }
                });
            }

            var builder = ol.newTagsRunFacetBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            var preparedTags = new ArrayList<>(merge.values());
            preparedTags.forEach(x -> {
                builder.put(x.getKey(), x.getValue());
            });

            return builder
                    .tags(new ArrayList<>(merge.values()))
                    .build();
        });
    }

    public static OpenLineage.TagsJobFacet combineWithTags(OpenLineage ol, OpenLineage.TagsJobFacet left, List<TagField> jobTags) {
        Map<String, OpenLineage.TagsJobFacetFields> merge = new HashMap<>();
        if (jobTags != null && !jobTags.isEmpty()) {
            jobTags.forEach(x -> {
                var field = ol.newTagsJobFacetFields(x.getKey(), x.getValue(), x.getSource());
                merge.put(field.getKey(), field);
            });
        }
        left.getTags().forEach(v -> {
                    if (Objects.nonNull(v.getValue())) {
                        merge.put(v.getKey(), v);
                    }
                }
        );

        var builder = ol.newTagsJobFacetBuilder();

        merge(
                left.getAdditionalProperties(),
                new HashMap<>(),
                builder::put
        );

        var preparedTags = new ArrayList<>(merge.values());
        preparedTags.forEach(x -> {
            builder.put(x.getKey(), x.getValue());
        });


        return builder
                .tags(preparedTags)
                .build();
    }

    public static OpenLineage.TagsRunFacet combineWithTags(OpenLineage ol, OpenLineage.TagsRunFacet left, List<TagField> runTags) {

        Map<String, OpenLineage.TagsRunFacetFields> merge = new HashMap<>();
        if (runTags != null && !runTags.isEmpty()) {
            runTags.forEach(x -> {
                var field = ol.newTagsRunFacetFields(x.getKey(), x.getValue(), x.getSource());
                merge.put(field.getKey(), field);
            });
        }
        left.getTags().forEach(v -> {
                    if (Objects.nonNull(v.getValue())) {
                        merge.put(v.getKey(), v);
                    }
                }
        );

        var builder = ol.newTagsRunFacetBuilder();
        merge(
                left.getAdditionalProperties(),
                new HashMap<>(),
                builder::put
        );

        var preparedTags = new ArrayList<>(merge.values());
        preparedTags.forEach(x -> {
            builder.put(x.getKey(), x.getValue());
        });

        return builder
                .tags(new ArrayList<>(merge.values()))
                .build();
    }

    protected static String key(OpenLineage.Dataset v) {
        return v.getNamespace() + v.getName();
    }

    public static <T extends OpenLineage.Dataset> List<T> mergeDatasetList(List<T> left, List<T> right, BinaryOperator<T> op) {
        return merge(left, right, (l, r) -> {
            if (l.isEmpty()) return r;
            if (r.isEmpty()) return l;

            Map<String, List<T>> outputs = new HashMap<>();
            left.forEach(v -> outputs.computeIfAbsent(key(v), (k) -> new ArrayList<>()).add(v));
            right.forEach(v -> outputs.computeIfAbsent(key(v), (k) -> new ArrayList<>()).add(v));

            return outputs.values()
                    .stream()
                    .flatMap(v -> v.stream().reduce(op).stream())
                    .collect(Collectors.toList());
        });
    }

    public static List<OpenLineage.OutputDataset> mergeOutputs(OpenLineage ol, List<OpenLineage.OutputDataset> left, List<OpenLineage.OutputDataset> right) {
        return mergeDatasetList(left, right, (l, r) -> combine(ol, l, r));
    }

    public static List<OpenLineage.InputDataset> mergeInputs(OpenLineage ol, List<OpenLineage.InputDataset> left, List<OpenLineage.InputDataset> right) {
        return mergeDatasetList(left, right, (l, r) -> combine(ol, l, r));
    }

    public static OpenLineage.InputDataset combine(OpenLineage ol, OpenLineage.InputDataset left, OpenLineage.InputDataset right) {
        return merge(left, right, (l, r) -> {
            var facets = combine(ol, left.getFacets(), right.getFacets());
            var inputFacets = combine(ol, left.getInputFacets(), right.getInputFacets());

            return ol.newInputDatasetBuilder()
                    .namespace(right.getNamespace())
                    .name(right.getName())
                    .facets(facets)
                    .inputFacets(inputFacets)
                    .build();
        });
    }

    public static OpenLineage.OutputDataset combine(OpenLineage ol, OpenLineage.OutputDataset left, OpenLineage.OutputDataset right) {
        return merge(left, right, (l, r) -> {
            assert Objects.equals(left.getName(), right.getName());
            assert Objects.equals(left.getNamespace(), right.getNamespace());

            OpenLineage.DatasetFacets facets = combine(ol, left.getFacets(), right.getFacets());
            OpenLineage.OutputDatasetOutputFacets outputFacets = combine(ol, left.getOutputFacets(), right.getOutputFacets());

            return ol.newOutputDatasetBuilder()
                    .namespace(right.getNamespace())
                    .name(right.getName())
                    .facets(facets)
                    .outputFacets(outputFacets)
                    .build();
        });
    }

    public static OpenLineage.OutputDatasetOutputFacets combine(OpenLineage ol, OpenLineage.OutputDatasetOutputFacets left, OpenLineage.OutputDatasetOutputFacets right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(combine(ol, left.getOutputStatistics(), right.getOutputStatistics()))
                    .icebergCommitReport(combine(ol, left.getIcebergCommitReport(), right.getIcebergCommitReport()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder.build();
        });
    }

    public static OpenLineage.IcebergCommitReportOutputDatasetFacet combine(OpenLineage ol, OpenLineage.IcebergCommitReportOutputDatasetFacet left, OpenLineage.IcebergCommitReportOutputDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.OutputStatisticsOutputDatasetFacet combine(OpenLineage ol, OpenLineage.OutputStatisticsOutputDatasetFacet lft, OpenLineage.OutputStatisticsOutputDatasetFacet rght) {
        return merge(lft, rght, (l, r) -> {
            var builder = ol.newOutputStatisticsOutputDatasetFacetBuilder()
                    .fileCount(l.getFileCount() + r.getFileCount())
                    .rowCount(l.getRowCount() + r.getRowCount())
                    .size(l.getSize() + r.getSize());

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder.build();
        });
    }

    public static OpenLineage.InputDatasetInputFacets combine(OpenLineage ol, OpenLineage.InputDatasetInputFacets left, OpenLineage.InputDatasetInputFacets right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newInputDatasetInputFacetsBuilder()
                    .inputStatistics(combine(ol, l.getInputStatistics(), r.getInputStatistics()))
                    .dataQualityAssertions(combine(ol, l.getDataQualityAssertions(), r.getDataQualityAssertions()))
                    .icebergScanReport(combine(ol, l.getIcebergScanReport(), r.getIcebergScanReport()))
                    .subset(combine(ol, l.getSubset(), r.getSubset()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder.build();
        });
    }

    public static OpenLineage.InputSubsetInputDatasetFacet combine(OpenLineage ol, OpenLineage.InputSubsetInputDatasetFacet left, OpenLineage.InputSubsetInputDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.IcebergScanReportInputDatasetFacet combine(OpenLineage ol, OpenLineage.IcebergScanReportInputDatasetFacet left, OpenLineage.IcebergScanReportInputDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DataQualityAssertionsDatasetFacet combine(OpenLineage ol, OpenLineage.DataQualityAssertionsDatasetFacet left, OpenLineage.DataQualityAssertionsDatasetFacet right) {
        return merge(left, right, (l, r) -> {
            var items = new ArrayList<>(l.getAssertions());
            items.addAll(r.getAssertions());
            var builder = ol.newDataQualityAssertionsDatasetFacetBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder
                    .assertions(items)
                    .build();
        });
    }

    public static OpenLineage.DataQualityMetricsInputDatasetFacet combine(OpenLineage ol, OpenLineage.DataQualityMetricsInputDatasetFacet lft, OpenLineage.DataQualityMetricsInputDatasetFacet rght) {
        return merge(lft, rght, (l, r) -> {
            var builder = ol.newDataQualityMetricsInputDatasetFacetBuilder()
                    .fileCount(combine(ol, l.getFileCount(), r.getFileCount()))
                    .rowCount(combine(ol, l.getRowCount(), r.getRowCount()))
                    .bytes(combine(ol, l.getBytes(), r.getBytes()))
                    .lastUpdated(r.getLastUpdated())
                    .columnMetrics(combine(ol, l.getColumnMetrics(), r.getColumnMetrics()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );
            return builder.build();
        });
    }

    public static Long combine(OpenLineage ol, Long lft, Long rght) {
        return merge(lft, rght, Long::sum);
    }

    public static OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics combine(OpenLineage ol, OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics lft, OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics rght) {
        return merge(lft, rght, (l, r) -> {
            var builder = ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder
                    .build();
        });
    }


    public static OpenLineage.InputStatisticsInputDatasetFacet combine(OpenLineage ol, OpenLineage.InputStatisticsInputDatasetFacet lft, OpenLineage.InputStatisticsInputDatasetFacet rght) {
        return merge(lft, rght, (l, r) -> {
            var builder = ol.newInputStatisticsInputDatasetFacetBuilder();

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder
                    .fileCount(merge(l.getFileCount(), r.getFileCount(), Long::sum))
                    .rowCount(merge(l.getRowCount(), r.getRowCount(), Long::sum))
                    .size(merge(l.getSize(), r.getSize(), Long::sum))
                    .build();
        });
    }


    public static OpenLineage.DatasetFacets combine(OpenLineage ol, OpenLineage.DatasetFacets left, OpenLineage.DatasetFacets right) {
        return merge(left, right, (l, r) -> {
            OpenLineage.DatasetFacetsBuilder builder = ol.newDatasetFacetsBuilder()
                    .dataSource(combine(l.getDataSource(), r.getDataSource()))
                    .version(combine(l.getVersion(), r.getVersion()))
                    .datasetType(combine(l.getDatasetType(), r.getDatasetType()))
                    .storage(combine(l.getStorage(), r.getStorage()))
                    .columnLineage(combine(ol, l.getColumnLineage(), r.getColumnLineage()))
                    .lifecycleStateChange(combine(l.getLifecycleStateChange(), r.getLifecycleStateChange()))
                    .tags(combine(ol, l.getTags(), r.getTags()))
                    .documentation(combine(ol, l.getDocumentation(), r.getDocumentation()))
                    .schema(combine(ol, l.getSchema(), r.getSchema()))
                    .ownership(combine(ol, l.getOwnership(), r.getOwnership()))
                    .symlinks(combine(ol, l.getSymlinks(), r.getSymlinks()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder.build();
        });
    }

    public static OpenLineage.DocumentationDatasetFacet combine(OpenLineage ol, OpenLineage.DocumentationDatasetFacet left, OpenLineage.DocumentationDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.OwnershipDatasetFacet combine(OpenLineage ol, OpenLineage.OwnershipDatasetFacet left, OpenLineage.OwnershipDatasetFacet right) {
        return merge(left, right, (l, r) -> {
            Map<String, OpenLineage.OwnershipDatasetFacetOwners> owners = new HashMap<>();
            l.getOwners().forEach(v -> owners.put(v.getName(), v));
            r.getOwners().forEach(v -> owners.put(v.getName(), v));
            var builder = ol.newOwnershipDatasetFacetBuilder().owners(new ArrayList<>(owners.values()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );
            return builder.build();
        });
    }

    public static OpenLineage.LifecycleStateChangeDatasetFacet combine(OpenLineage.LifecycleStateChangeDatasetFacet left, OpenLineage.LifecycleStateChangeDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.StorageDatasetFacet combine(OpenLineage.StorageDatasetFacet left, OpenLineage.StorageDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DatasetTypeDatasetFacet combine(OpenLineage.DatasetTypeDatasetFacet left, OpenLineage.DatasetTypeDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DatasetVersionDatasetFacet combine(OpenLineage.DatasetVersionDatasetFacet left, OpenLineage.DatasetVersionDatasetFacet right) {
        return right != null ? right : left;
    }

    public static OpenLineage.DatasourceDatasetFacet combine(OpenLineage.DatasourceDatasetFacet left, OpenLineage.DatasourceDatasetFacet right) {
        return right != null ? right : left;
    }


    public static OpenLineage.SchemaDatasetFacet combine(OpenLineage ol, OpenLineage.SchemaDatasetFacet left, OpenLineage.SchemaDatasetFacet right) {
        return merge(left, right, (l, r) -> {
            Map<String, OpenLineage.SchemaDatasetFacetFields> fields = new HashMap<>();
            l.getFields().forEach(v -> fields.put(v.getName(), v));
            r.getFields().forEach(v -> fields.put(v.getName(), v));
            var builder = ol.newSchemaDatasetFacetBuilder()
                    .fields(new ArrayList<>(fields.values()));

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder.build();
        });
    }

    public static OpenLineage.ColumnLineageDatasetFacet combine(OpenLineage ol, OpenLineage.ColumnLineageDatasetFacet left, OpenLineage.ColumnLineageDatasetFacet right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newColumnLineageDatasetFacetBuilder();

            var datasets = new ArrayList<OpenLineage.InputField>();
            if (l.getDataset() != null) datasets.addAll(l.getDataset());
            if (r.getDataset() != null) datasets.addAll(r.getDataset());


            var merge = new HashMap<>(l.getFields().getAdditionalProperties());
            r.getFields().getAdditionalProperties().forEach((k, v) -> {
                if (merge.containsKey(k)) {
                    merge.put(k, combine(ol, merge.get(k), v));
                } else {
                    merge.put(k, v);
                }
            });

            var fields = ol.newColumnLineageDatasetFacetFieldsBuilder();
            merge.forEach(fields::put);

            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            return builder
                    .dataset(datasets.isEmpty() ? null : datasets)
                    .fields(fields.build())
                    .build();
        });
    }

    public static OpenLineage.ColumnLineageDatasetFacetFieldsAdditional combine(OpenLineage ol, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional left, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional right) {
        if (left == null || left.getInputFields() == null || left.getInputFields().isEmpty()) return right;
        if (right == null || right.getInputFields() == null || right.getInputFields().isEmpty()) return left;

        List<OpenLineage.InputField> fields = new ArrayList<>(left.getInputFields());
        fields.addAll(right.getInputFields());

        return ol.newColumnLineageDatasetFacetFieldsAdditional(fields, right.getTransformationDescription(), right.getTransformationType());
    }

    public static OpenLineage.SymlinksDatasetFacet combine(OpenLineage ol, OpenLineage.SymlinksDatasetFacet left, OpenLineage.SymlinksDatasetFacet right) {
        return merge(left, right, (l, r) -> {
            var builder = ol.newSymlinksDatasetFacetBuilder();

            Map<String, OpenLineage.SymlinksDatasetFacetIdentifiers> merge = new HashMap<>();
            if (l != null && l.getIdentifiers() != null) {
                l.getIdentifiers().forEach(v -> merge.put(v.getName(), v));
            }
            if (r != null && r.getIdentifiers() != null) {
                r.getIdentifiers().forEach(v -> merge.put(v.getName(), v));
            }
            merge(
                    l.getAdditionalProperties(),
                    r.getAdditionalProperties(),
                    builder::put
            );

            builder.identifiers(new ArrayList<>(merge.values()));
            return builder.build();
        });
    }

    private static <T> void merge(Map<String, T> left, Map<String, T> right, BiConsumer<String, T> put) {
        if (left != null) {
            left.forEach(put);
        }
        if (right != null) {
            right.forEach(put);
        }
    }

    private static <T> void merge(List<TagField> tags, BiConsumer<String, Object> put) {
        if (tags != null) {
            tags.forEach(x -> put.accept(x.getKey(), x.getValue()));
        }
    }


    private static <T> T merge(T left, T right, BinaryOperator<T> merge) {
        if (left != null && right != null) {
            return merge.apply(left, right);
        } else if (right != null) {
            return right;
        } else {
            return left;
        }
    }
}
