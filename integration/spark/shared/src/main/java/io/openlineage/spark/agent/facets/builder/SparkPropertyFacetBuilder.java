package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkPropertyFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import org.apache.spark.scheduler.SparkListenerJobStart;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkPropertyFacetBuilder extends CustomFacetBuilder<SparkListenerJobStart, SparkPropertyFacet> {
    @Override
    protected void build(SparkListenerJobStart event, BiConsumer<String, ? super SparkPropertyFacet> consumer) {
        Stream<Map.Entry<Object, Object>> stream = event.properties().entrySet().stream();
        Map<String, Object> collect = stream.collect(Collectors.toMap(k -> k.getKey().toString(), Map.Entry::getValue));
        consumer.accept("spark-properties", new SparkPropertyFacet(collect));
    }


}
