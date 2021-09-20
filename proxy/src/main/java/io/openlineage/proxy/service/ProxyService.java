package io.openlineage.proxy.service;

import com.google.common.collect.ImmutableList;
import io.openlineage.proxy.ProxyConfig;
import io.openlineage.proxy.service.models.Stream;

public class ProxyService {
    private final ImmutableList<Stream> streams;

    public ProxyService(final ProxyConfig config) {
        this.streams = ImmutableList.of();
    }
    public void emit() {

    }
}
