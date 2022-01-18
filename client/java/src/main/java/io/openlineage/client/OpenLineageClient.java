package io.openlineage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class OpenLineageClient {
    private static final URL DEFAULT_BASE_URL = Utils.toUrl("http://localhost:8080");
}
