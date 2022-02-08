package io.openlineage.flink

class FlinkStatefulApplicationRunner {
    private static String[] PARAMETERS = ["--input-topic", "io.openlineage.flink.kafka.input", "--output-topic", "io.openlineage.flink.kafka.output"]

    static void main(String[] args) {
        FlinkStatefulApplication.main(PARAMETERS)
    }
}
