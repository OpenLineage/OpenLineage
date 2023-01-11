/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import io.openlineage.proxy.service.ProxyService;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/api/v1/lineage")
public class ProxyResource {
  private final ProxyService service;

  public ProxyResource(@NonNull final ProxyService service) {
    this.service = service;
  }

  @POST
  @Consumes(APPLICATION_JSON)
  public void proxyEvent(
      @Valid String eventAsString, @Suspended final AsyncResponse asyncResponse) {
    service
        .proxyEventAsync(eventAsString)
        .whenComplete(
            (result, err) -> {
              if (err != null) {
                log.error("Failed to proxy OpenLineage event!", err);
                asyncResponse.resume(Response.status(500).build());
              } else {
                asyncResponse.resume(Response.status(200).build());
              }
            });
  }
}
