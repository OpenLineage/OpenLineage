<!-- SPDX-License-Identifier: Apache-2.0 -->

---
Author: Ziyoiddin Yusupov
Created: February 17, 2022
Issue: https://github.com/OpenLineage/OpenLineage/issues/79
---

# Purpose
Users need get lineage data from Flink and generate OpenLineage events.<br/>
Flink lineage may include job start time, datasets (source/sink) which are being read and being produced.<br/>
The notion of datasets is in the sense of [OpenLineage datasets](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md) and we need to be able to identify them following OpenLineage dataset [naming convention](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).<br/>
We should cover both DataStream API jobs and Table/SQL API jobs (DataSet API is deprecated).<br/>
Furthermore, we should have information about particular datasets - volume (rows, bytes) of data written.<br/>

[Original proposition](https://docs.google.com/document/d/1AGbGv-BsSnJLyg6adDSLn2CMgTdw3ThSFDImvLaPxJk) was given by Julien Le Dem.<br/>
Apache Atlas Flink integration discussions in Apache Flink [mailing archive](https://lists.apache.org/thread/1wnyn8cgskkhsz669kp72mgh0x5s5bbv) - originator Gyula Fóra. Discussion wasn't concluded.<br/>
[Design document](https://docs.google.com/document/d/1wSgzPdhcwt-SlNBBqL-Zb7g8fY6bN8JwHEg7GCdsBG8) for Atlas and Flink integration.<br/>
OpenLineage internal [discussion document](https://docs.google.com/document/d/1Mog-DWzCYAmln3rKwde88lQWkFWO8mebbarlbhU6YtM) describing first steps and investigations.

# Implementation ideas

First good idea would be to integrate over [JobListener](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/core/execution/JobListener.html) functionality from Apache Flink.<br/>
We can try to implement custom `OpenLineageJobListener` with those two methods (onJobSubmitted/onJobExecuted):
```java
/**
 * A listener that is notified on specific job status changed, which should be firstly registered by
 * {@code #registerJobListener} of execution environments.
 *
 * <p>It is highly recommended NOT to perform any blocking operation inside the callbacks. If you
 * block the thread the invoker of environment execute methods is possibly blocked.
 */
@PublicEvolving
public interface JobListener {

    /**
     * Callback on job submission. This is called when {@code execute()} or {@code executeAsync()}
     * is called.
     *
     * <p>Exactly one of the passed parameters is null, respectively for failure or success.
     *
     * @param jobClient a {@link JobClient} for the submitted Flink job
     * @param throwable the cause if submission failed
     */
    void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable);

    /**
     * Callback on job execution finished, successfully or unsuccessfully. It is only called back
     * when you call {@code execute()} instead of {@code executeAsync()} methods of execution
     * environments.
     *
     * <p>Exactly one of the passed parameters is null, respectively for failure or success.
     */
    void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable);
}
```

**Problem** here is that neither `JobClient` nor `JobExecutionResult` has [StreamGraph](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/graph/StreamGraph.html) internally.<br/>
Original [suggestion from Atlas team](https://github.com/gyfora/flink/commit/66b094e9dba7d72e53f1b0ba61688e63060eaad8#diff-acce77aad974b744913a2e6ff56982ea31695d4a0a2d59ccfa8c3c5baac17b47R46)
for this case was to introduce `Pipeline getPipeline()` method in `JobClient`,
which can be [casted](https://github.com/gyfora/atlas/commit/f0f8b94db5c86d9f424e5a8c4dfde94c1ceef352#diff-4d627133118ed277a4b243b36f049e543c015d1a3bb62d6bbe381e05890d42e3R68)
into `StreamGraph` easily in custom `JobListener` implementation.<br/>
The changes suggested from Apache Atlas team, has to be brought back again to discussion, and we can improve it for further movement into the Apache Flink repository.

# Work in progress

Based on the idea above, we are trying to achieve first desired steps from our side before moving forward into Apache Flink community with changes for `JobListener`.<br/>
Our plan was to get `Pipeline` from Flink, which will include all the sources/sinks as well as operators.<br/>
To achieve it we are introducing `OpenLineageJobListener` which implements `JobListener` from Flink.
Once user executes/submits the job, our custom logic for OpenLineage hook under `JobListener` gets triggered.<br/> 
Due to the missing `Pipeline/StreamGraph` in `JobClient` as of now, 
we are trying to use `ExecutionEnvironment.getExecutionEnvironment()/StreamExecutionEnvironment.getExecutionEnvironment()` parameter for `OpenLineageJobListener` constructor. (which is not a good approach, but temporary solution)<br/>
ExecutionEnvironment has `StreamGraph`. When user runs [execute()](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java#L1965-#L1970) it uses this graph to prepare pipeline for Flink run.<br/>
Inside `execute()` method JobListener's `onJobExecuted` method gets triggered, with our hook logic there.<br/>

There is one caveat, inside `execute()` method call, there is a
call: `final StreamGraph streamGraph = getStreamGraph();`, which is calling `getStreamGraph(true)` internally. 
That method with parameter `getStreamGraph(boolean clearTransformations)`:

```java
@Public
public class StreamExecutionEnvironment {
    /*
     * ......
     * .....
     */
    /**
     * Getter of the {@link StreamGraph} of the streaming job with the option to clear previously
     * registered {@link Transformation transformations}. Clearing the transformations allows, for
     * example, to not re-execute the same operations when calling {@link #execute()} multiple
     * times.
     *
     * @param clearTransformations Whether or not to clear previously registered transformations
     * @return The stream graph representing the transformations
     */
    @Internal
    public StreamGraph getStreamGraph(boolean clearTransformations) {
        final StreamGraph streamGraph = getStreamGraphGenerator(transformations).generate();
        if (clearTransformations) {
            transformations.clear();
        }
        return streamGraph;
    }
}
```
clears out the `List<Transformation<?>> transformations = new ArrayList<>()`.<br/>
As this call is happening before `JobListener.onJobSubmitted()` method call, we can't get pipeline/streamGraph out of Flink.<br/>
`StreamGraph` is constructed based on this list of `transformation`s.<br/>

To address this issue we are now tweaking temporarily with reflection the field `transformations` inside `StreamExecutionEnvironment` class:
```java
public class OpenLineageFlinkJobListener implements JobListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageFlinkJobListener.class);
    private final StreamExecutionEnvironment executionEnvironment;

    static class UnclearableList<T> extends ArrayList<T> {
        public UnclearableList(Collection<T> collection) {super(collection);}
        @Override
        public void clear() {LOGGER.info("Ignore clearing the transformations ArrayList");}
    }

    OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
        try {
            Field transformations = FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
            ArrayList<Transformation<?>> previousTransformationList = (ArrayList<Transformation<?>>) FieldUtils.readField(transformations, executionEnvironment, true);
            List<Transformation<?>> transformationList = new UnclearableList<>(previousTransformationList);
            FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
        } catch (IllegalAccessException e) {
            LOGGER.error("Failed to rewrite transformations");
        }
    }
}
```
We managed to get the StreamGraph this way.

## Next steps
We should be able to construct the pipeline from `StreamGraph` and get pipeline view.<br/>
We have to find a way to read Source/Sink related metadate (e.g. Kafka - topic, partition, brokers etc)<br/>





