package io.openlineage.flink

class FlinkStatefulApplicationRunner {
    private static String[] PARAMETERS = ["--input-topics", "io.openlineage.flink.kafka.input",
                                          "--output-topic", "io.openlineage.flink.kafka.output",
                                          "--flink.openlineage.url", "http://localhost:5000/api/v1/namespaces/flink_integration/"]

    static void main(String[] args) {
        FlinkStatefulApplication.main(PARAMETERS)
    }
}
