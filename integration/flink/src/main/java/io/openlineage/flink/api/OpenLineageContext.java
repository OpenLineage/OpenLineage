package io.openlineage.flink.api;

import io.openlineage.client.OpenLineage;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;
import java.util.UUID;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link io.openlineage.client.OpenLineage.RunEvent}. An {@link OpenLineageContext} should be
 * created once for every detected Flink job execution.
 *
 * <p>It should be assumed that the lists of input and output {@link QueryPlanVisitor} are mutable
 * lists. As {@link QueryPlanVisitor}s require a reference to the {@link OpenLineageContext}, the
 * lists will always be added to after the {@link QueryPlanVisitor}s are constructed. Thus, copies
 * should never be made of the lists, as it should be assumed such copies will be incomplete.
 *
 * <p>This API is evolving and may change in future releases
 *
 * @apiNote This interface is evolving and may change in future releases
 */
@Value
@Builder
public class OpenLineageContext {
    UUID runUuid = UUID.randomUUID();

    /**
     * Optional {@link StreamExecutionEnvironment} instance, that is the context in which a streaming program is executed.
     */
    @Builder.Default
    @NonNull Optional<StreamExecutionEnvironment> streamExecutionEnvironment = Optional.empty();

    /**
     * A non-null, preconfigured {@link OpenLineage} client instance for constructing OpenLineage
     * model objects
     */
    @NonNull OpenLineage openLineage;
}
