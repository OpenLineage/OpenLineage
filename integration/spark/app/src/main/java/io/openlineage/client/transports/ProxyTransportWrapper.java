package io.openlineage.client.transports;

import com.google.common.collect.MapMaker;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.openlineage.spark.agent.ArgumentParser.SPARK_CONF_PARENT_RUN_ID;

@Slf4j
@ToString
public final class ProxyTransportWrapper extends Transport implements ProxyTransportControllable {

    private final Transport underlying;
    private final SparkOpenLineageConfig config;
    private final Map<String, List<OpenLineage.DatasetEvent>> datasetEvents;
    private final Map<String, List<OpenLineage.RunEvent>> runEvents;
    private final Map<String, List<OpenLineage.JobEvent>> jobEvents;
    private final OpenLineage ol;
    private final boolean mergeEventsByType = Objects.equals(System.getenv().getOrDefault("OPENLINEAGE__MERGE_EVENTS_BY_TYPE", "true"), "true");
    private final boolean useWeakRef = Objects.equals(System.getenv().getOrDefault("OPENLINEAGE__PROXY_USE_WEAK_REF", "false"), "true");

    private OpenLineage.TagsJobFacet jobTagFacet;
    private OpenLineage.TagsRunFacet runTagFacet;
    private String parentRunID;

    public ProxyTransportWrapper(@NonNull SparkOpenLineageConfig config, @NonNull Transport underlying) {
        this.underlying = underlying;
        this.config = config;
        if (useWeakRef) {
            this.runEvents = new MapMaker().weakValues().makeMap();
            this.jobEvents = new MapMaker().weakValues().makeMap();
        } else {
            this.runEvents = new MapMaker().makeMap();
            this.jobEvents = new MapMaker().makeMap();
        }
        this.datasetEvents = new MapMaker().weakValues().makeMap();
        this.ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
        ProxyTransportRemote.setControllable(this);
    }


    @Override
    public void emit(@NonNull OpenLineage.RunEvent event) {
        putOrAppend(
                runEvents,
                getParentRunID(),
                EventMergerV2.combine(
                        ol,
                        event,
                        Optional.of(getJobTagsFromConfig(config)),
                        Optional.of(getRunTagsFromConfig(config))
                )
        );
    }

    @Override
    public void emit(@NonNull OpenLineage.DatasetEvent event) {
        // TODO: Ignored
//    putOrAppend(datasetEvents, calculateRunID(), event, this::mergeDatasets);
    }

    @Override
    public void emit(@NonNull OpenLineage.JobEvent event) {
        putOrAppend(
                jobEvents,
                getParentRunID(),
                EventMergerV2.combine(
                        ol,
                        event,
                        Optional.of(getJobTagsFromConfig(config))
                )
        );
    }

    @Override
    public void close() throws Exception {
        underlying.close();
    }

    /**
     * @return an new {@link Builder} object for building {@link ProxyTransportWrapper}s.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void emitAll() {
        emitAll(getParentRunID());
    }

    @Override
    public void emitAll(String runId) {

        var runRecords = runEvents.remove(runId);
        if (runRecords == null) {
            runRecords = new ArrayList<>();
        }

        var jobRecords = jobEvents.remove(runId);
        if (jobRecords == null) {
            jobRecords = new ArrayList<>();
        }

        if (!runRecords.isEmpty()) {
            if (mergeEventsByType) {
                var groupedRunEvents = runRecords
                        .stream()
                        .collect(Collectors.toMap(
                                OpenLineage.RunEvent::getEventType,
                                Function.identity(),
                                (l, r) -> ProxyTransportWrapper.mergeRuns(ol, l, r)
                        ));

                if (config == null) {
                    log.debug(config.getClass().getCanonicalName() + " is null");
                }

                var jobTags = Stream.concat(
                        jobRecords.stream().map(x -> x.getJob().getFacets().getTags()),
                        groupedRunEvents.values().stream().flatMap(x -> getJobTagsFacets(x).stream())
                ).reduce((l, r) -> EventMergerV2.combineWithTags(ol, l, r, getJobTagsFromConfig(config)));

                if (jobTags.isPresent()) {
                    log.debug("jobTags merged: {}", jobTags.get().toString());
                } else {
                    log.debug("jobTags are null");
                }

                var runTags = groupedRunEvents.values().stream()
                        .flatMap(x -> getRunTagFacets(x).stream())
                        .reduce((l, r) -> EventMergerV2.combineWithTags(ol, l, r, getRunTagsFromConfig(config)));

                if (runTags.isPresent()) {
                    log.debug("runTags merged: {}", runTags.get().toString());
                } else {
                    log.debug("runTags are null");
                }

                Consumer<OpenLineage.RunEvent> runEventWithJobTagsF = (x) -> {
                    var evt = EventMergerV2.combine(ol, x, jobTags, runTags);
                    if (underlying != null) {
                        logEmittingEvent(evt);
                        underlying.emit(evt);
                    } else {
                        logEmittingEvent(evt);
                    }
                };

                emitForKey(OpenLineage.RunEvent.EventType.START, groupedRunEvents, runEventWithJobTagsF);
                emitForKey(OpenLineage.RunEvent.EventType.RUNNING, groupedRunEvents, runEventWithJobTagsF);
                emitForKey(OpenLineage.RunEvent.EventType.ABORT, groupedRunEvents, runEventWithJobTagsF);
                emitForKey(OpenLineage.RunEvent.EventType.FAIL, groupedRunEvents, runEventWithJobTagsF);
                emitForKey(OpenLineage.RunEvent.EventType.COMPLETE, groupedRunEvents, runEventWithJobTagsF);
                emitForKey(OpenLineage.RunEvent.EventType.OTHER, groupedRunEvents, runEventWithJobTagsF);

                Consumer<OpenLineage.JobEvent> jobEventWithJobTagsF = (x) -> {
                    logEmittingEvent(x);
                    underlying.emit(EventMergerV2.combine(ol, x, jobTags));
                };

                jobRecords.forEach(jobEventWithJobTagsF);
            } else {
                runRecords.forEach(x -> {
                    if (underlying != null) {
                        logEmittingEvent(x);
                        underlying.emit(x);
                    } else {
                        logEmittingEvent(x);
                    }
                });
            }
        }
    }

    private Optional<OpenLineage.TagsRunFacet> getRunTagFacets(OpenLineage.RunEvent x) {
        var xTags = Optional.ofNullable(x)
                .map(OpenLineage.RunEvent::getRun)
                .map(OpenLineage.Run::getFacets)
                .map(OpenLineage.RunFacets::getTags);
        if (xTags.isPresent()) {
            log.debug("xTags.Run for " + x.getEventType() + " merged: {}", xTags.get());
        } else {
            log.debug("xTags.Run for " + x.getEventType() + " is null");
        }
        return xTags;
    }

    private Optional<OpenLineage.TagsJobFacet> getJobTagsFacets(OpenLineage.RunEvent x) {
        var xTags = Optional.ofNullable(x.getJob())
                .map(OpenLineage.Job::getFacets)
                .map(OpenLineage.JobFacets::getTags);
        if (xTags.isPresent()) {
            log.debug("xTags.Job for " + x.getEventType() + " merged: {}", xTags.get());
        } else {
            log.debug("xTags.Job for " + x.getEventType() + " is null");
        }
        return xTags;
    }

    private OpenLineage.TagsRunFacet getRunTagsFromConfig(SparkOpenLineageConfig config) {
        if (runTagFacet == null) {
            var fields = parseRunTagsFromConfig(config);
            if (fields.isEmpty()) {
                log.debug("runTagFields are empty");
            }
            var b = ol.newTagsRunFacetBuilder();
            fields.forEach(x -> {
                b.put(x.getKey(), x.getValue());
            });
            var items = fields
                    .stream()
                    .map(x -> ol
                            .newTagsRunFacetFieldsBuilder()
                            .key(x.getKey())
                            .value(x.getValue())
                            .source(x.getSource())
                            .build())
                    .collect(Collectors.toList());

            fields.forEach(x -> log.debug("runTagFields merged: {}", x.toString()));

            runTagFacet = b
                    .tags(items)
                    .build();

        }
        return runTagFacet;
    }

    private OpenLineage.TagsJobFacet getJobTagsFromConfig(SparkOpenLineageConfig config) {
        if (jobTagFacet == null) {
            var fields = parseJobTagsFromConfig(config);
            if (fields.isEmpty()) {
                log.debug("jobTagFields are empty");
            }
            var b = ol.newTagsJobFacetBuilder();
            fields.forEach(x -> {
                b.put(x.getKey(), x.getValue());
            });

            var items = fields
                    .stream()
                    .map(x -> ol
                            .newTagsJobFacetFieldsBuilder()
                            .key(x.getKey())
                            .value(x.getValue())
                            .source(x.getSource())
                            .build())
                    .collect(Collectors.toList());

            items.forEach(x -> log.debug("jobTagFields merged: {}", x.toString()));

            jobTagFacet = b
                    .tags(items)
                    .build();

        }
        return jobTagFacet;
    }

    private void emitForKey(OpenLineage.RunEvent.EventType key, Map<OpenLineage.RunEvent.EventType, OpenLineage.RunEvent> dict, Consumer<OpenLineage.RunEvent> emitF) {
        Optional.ofNullable(dict.get(key)).ifPresent(emitF);
    }

    private OpenLineage.DatasetEvent mergeDatasets(OpenLineage.DatasetEvent left, OpenLineage.DatasetEvent right) {
        // TODO: implement method
        return left;
    }

    private String getParentRunID() {
        if (parentRunID == null) {
            var conf = SparkSessionUtils.activeSession().map(SparkSession::conf);
            var notFound = "undefined";
            if (conf.isPresent()) {
                var entry = conf.flatMap(x -> {
                    if (x.contains(SPARK_CONF_PARENT_RUN_ID))
                        return Optional.ofNullable(x.get(SPARK_CONF_PARENT_RUN_ID));
                    else
                        return Optional.empty();
                });
                if (entry.isPresent()) {
                    parentRunID = entry.get();
                } else {
                    log.error("spark config does not contain key: " + SPARK_CONF_PARENT_RUN_ID);
                    parentRunID = notFound;
                }
            } else {
                log.error("no spark session or config is present");
                parentRunID = notFound;
            }
        }
        return parentRunID;
    }

    public static List<TagField> parseJobTagsFromConfig(SparkOpenLineageConfig config) {
        var items = Optional.ofNullable(config)
                .map(OpenLineageConfig::getJobConfig)
                .map(JobConfig::getTags)
                .orElseGet(ArrayList::new);
        items.addAll(getTagsFromSparkSession("spark.openlineage.job.tags"));
        return items;
    }

    public static List<TagField> parseRunTagsFromConfig(SparkOpenLineageConfig config) {
        var items = Optional.ofNullable(config)
                .map(OpenLineageConfig::getRunConfig)
                .map(RunConfig::getTags)
                .orElseGet(ArrayList::new);
        items.addAll(getTagsFromSparkSession("spark.openlineage.run.tags"));
        return items;
    }

    public static OpenLineage.JobEvent mergeJobs(OpenLineage ol, OpenLineage.JobEvent left, OpenLineage.JobEvent right) {
        // TODO: implement method
        log.debug("merging {} events: ", OpenLineage.JobEvent.class.getCanonicalName());
        log.debug("left: [ {} ]", OpenLineageClientUtils.toJson(left));
        log.debug("right: [ {} ]", OpenLineageClientUtils.toJson(right));
        var r = EventMergerV2.combine(ol, left, right);
        log.debug("merging events, product: [ {} ]", OpenLineageClientUtils.toJson(r));
        return r;
    }

    public static OpenLineage.RunEvent mergeRuns(OpenLineage ol, OpenLineage.RunEvent left, OpenLineage.RunEvent right) {
        log.debug("merging {} events: ", OpenLineage.RunEvent.class.getCanonicalName());
        log.debug("left: [ {} ]", OpenLineageClientUtils.toJson(left));
        log.debug("right: [ {} ]", OpenLineageClientUtils.toJson(right));
        var r = EventMergerV2.combine(ol, left, right);
        log.debug("merging events, product: [ {} ]", OpenLineageClientUtils.toJson(r));
        return r;
    }

    public static <T extends OpenLineage.BaseEvent> void logEmittingEvent(T event) {
        var json = OpenLineageClientUtils.toJson(event);
        log.debug("emitting event json: [ {} }", json);
    }

    public static <T extends OpenLineage.BaseEvent> List<T> putOrAppend(Map<String, List<T>> dict, String key, T event) {
        log.debug("storing event {} before emitting: [ {} ]", event.getClass().getCanonicalName(), OpenLineageClientUtils.toJson(event));
        if (dict.containsKey(key)) {
            dict.get(key).add(event);
        } else {
            dict.put(key, new LinkedList<>() {{
                add(event);
            }});
        }
        return dict.get(key);
    }

    public static List<TagField> getTagsFromSparkSession(String key) {
        return SparkSessionUtils.activeSession()
                .map(session -> session.conf().get(key))
                .map(ProxyTransportWrapper::parseTagFields)
                .orElseGet(ArrayList::new);
    }

    public static List<TagField> parseTagFields(String input) {
        return Stream.of(input)
                .flatMap(x -> {
                    if (x.contains(";")) {
                        return Arrays.stream(x.split(";"));
                    } else {
                        return Stream.of(x);
                    }
                })
                .map(x -> {
                    var idx = x.indexOf(":");
                    var key = x.substring(0, idx);
                    var value = x.substring(idx + 1);

                    return new TagField(key, value);
                })
                .collect(Collectors.toList());
    }

    /**
     * Builder for {@link ProxyTransportWrapper} instances.
     *
     */
    @Deprecated
    public static final class Builder {

        @Delegate
        private Transport transport;

        @Delegate
        private SparkOpenLineageConfig config;

        private Builder() {

        }

        public Builder underlying(@NonNull Transport transport) {
            this.transport = transport;
            return this;
        }

        public Builder config(@NonNull SparkOpenLineageConfig config) {
            this.config = config;
            return this;
        }

        /**
         * @return an {@link ProxyTransportWrapper} object with the properties of this {@link
         * Builder}.
         */
        public ProxyTransportWrapper build() {

            return new ProxyTransportWrapper(config, transport);
        }
    }

}
