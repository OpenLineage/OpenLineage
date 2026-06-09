/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class ProxyTransportBuilder implements TransportBuilder {

    @Override
    public TransportConfig getConfig() {
        return new HttpConfig();
    }

    @Override
    public Transport build(TransportConfig config) {
        var underlying = new HttpTransport((HttpConfig) config);
        var transport = new ProxyTransport(underlying);
        return transport;
    }

    @Override
    public String getType() {
        return "proxy";
    }
}
