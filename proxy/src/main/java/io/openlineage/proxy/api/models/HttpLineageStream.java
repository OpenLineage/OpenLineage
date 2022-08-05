/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.proxy.api.models;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** HttpLineageStream pushes events to http endpoint */
@Slf4j
public class HttpLineageStream extends LineageStream {

  private final String url;
  private final String apikey;

  public HttpLineageStream(@NonNull final HttpConfig httpConfig) {
    super(Type.HTTP);
    this.url = httpConfig.getUrl();
    this.apikey = httpConfig.getApikey();
  }

  @Override
  public void collect(@NonNull String eventAsString) {
    eventAsString = eventAsString.trim();
    try {
      ClientBuilder cb = ClientBuilder.newBuilder();
      Client client = cb.build();
      Invocation.Builder invocation = client.target(this.url).request(MediaType.APPLICATION_JSON);
      if (this.apikey != null && this.apikey.trim().length() > 0) {
        invocation = invocation.header(HttpHeaders.AUTHORIZATION, "Bearer " + this.apikey);
      }
      Response response = invocation.post(Entity.json(eventAsString));
      int status = response.getStatus();
      log.debug("Received lineage event: {} \n response code: {}", eventAsString, status);
    } catch (WebApplicationException ex) {
      log.error("Exception occurred during HttpLineageStream's collect() call.");
      log.error(ex.getMessage());
    }
  }
}
