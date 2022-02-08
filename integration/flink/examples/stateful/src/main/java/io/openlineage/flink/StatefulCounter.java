package io.openlineage.flink;

import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.avro.event.OutputEvent;
import io.openlineage.flink.avro.infrastructure.state.managed.Counter;
import io.openlineage.util.StateUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatefulCounter extends KeyedProcessFunction<String, InputEvent, OutputEvent> {

    public static final long serialVersionUID = 1;
    public static final ValueStateDescriptor<Counter> COUNTER_DESCRIPTOR;

    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulCounter.class);

    static {
        COUNTER_DESCRIPTOR = new ValueStateDescriptor<>("counter", Counter.class);
        COUNTER_DESCRIPTOR.setQueryable(COUNTER_DESCRIPTOR.getName());
    }

    private transient ValueState<Counter> counterState;

    @Override
    public void open(Configuration parameters) {
        counterState = getRuntimeContext().getState(COUNTER_DESCRIPTOR);
    }

    @Override
    public void processElement(InputEvent inputEvent, Context ctx, Collector<OutputEvent> output) throws Exception {
        LOGGER.info("Processing InputEvent={}", inputEvent);
        Counter counter = StateUtils.value(counterState, new Counter(0L, null));
        counter.setCounter(counter.getCounter() + 1);
        counter.setLastSeen(inputEvent.toString());
        counterState.update(counter);
        OutputEvent outputEvent = new OutputEvent(inputEvent.id, inputEvent.version, counter.getCounter());
        LOGGER.info("Preparing OutputEvent={} to sent.", inputEvent);
        output.collect(outputEvent);
    }
}
