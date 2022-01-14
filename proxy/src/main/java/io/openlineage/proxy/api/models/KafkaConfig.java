package io.openlineage.proxy.api.models;

import io.openlineage.proxy.ProxyStreamConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Properties;

@NoArgsConstructor
@ToString
public final class KafkaConfig implements ProxyStreamConfig {
    @Getter @Setter private String topicName;
    @Getter @Setter private String localServerId;
    @Getter @Setter private String bootstrapServerUrl;
    @Getter @Setter private Properties properties;

    public boolean hasLocalServerId() {
        return (localServerId != null);
    }

    public void addProperty(@NonNull String key, @NonNull String value) {
        properties.put(key, value);
    }
}
