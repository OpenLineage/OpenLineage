package io.openlineage.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class ConfigWrapper {
    private final Config config;

    public ConfigWrapper(Config config) {
        this.config = config;
    }

    public static ConfigWrapper fromResource(String resource) {
        return new ConfigWrapper(ConfigFactory.parseResources(resource));
    }

    public static ConfigWrapper fromResource(String resource, ConfigWrapper resolvingConfig) {
        return new ConfigWrapper(ConfigFactory.parseResources(resource).resolveWith(resolvingConfig.config));
    }

    public Map<String, String> toStringsMap() {
        Map<String, String> stringsMap = new HashMap<>();
        for(Entry<String, ConfigValue> entry: config.entrySet()) {
            stringsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }
        return stringsMap;
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.putAll(toStringsMap());
        return properties;
    }
}
