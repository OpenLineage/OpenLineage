/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.Transport;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** HTTP client used to emit {@link OpenLineage.RunEvent}s to HTTP backend. */
@Slf4j
public final class OpenLineageClient {
  final Transport transport;
  final String[] disabledFacets;

  /** Creates a new {@code OpenLineageClient} object. */
  public OpenLineageClient() {
    this(new ConsoleTransport());
  }

  public OpenLineageClient(@NonNull final Transport transport) {
    this(transport, new String[] {});
  }

  public OpenLineageClient(@NonNull final Transport transport, String[] disabledFacets) {
    this.transport = transport;
    this.disabledFacets = disabledFacets;

    OpenLineageClientUtils.configureObjectMapper(disabledFacets);
  }

  /**
   * Emit the given run event to HTTP backend. The method will return successfully after the run
   * event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param runEvent The run event to emit.
   */
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    transport.emit(runEvent);
  }

  /**
   * Returns an new {@link OpenLineageClient.Builder} object for building {@link
   * OpenLineageClient}s.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link OpenLineageClient} instances.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * OpenLineageClient client = OpenLineageClient().builder()
   *     .url("http://localhost:5000")
   *     .build()
   * }</pre>
   */
  public static final class Builder {
    private static final Transport DEFAULT_TRANSPORT = new ConsoleTransport();
    private Transport transport;
    private String[] disabledFacets;

    private Builder() {
      this.transport = DEFAULT_TRANSPORT;
      disabledFacets = new String[] {};
    }

    public Builder transport(@NonNull Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder disableFacets(@NonNull String[] disabledFacets) {
      this.disabledFacets = disabledFacets;
      return this;
    }

    /**
     * Returns an {@link OpenLineageClient} object with the properties of this {@link
     * OpenLineageClient.Builder}.
     */
    public OpenLineageClient build() {
      return new OpenLineageClient(transport, disabledFacets);
    }
  }
}
