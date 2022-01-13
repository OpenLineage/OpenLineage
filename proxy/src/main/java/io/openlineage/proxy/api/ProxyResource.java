/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public void emit(@Valid String event, @Suspended final AsyncResponse asyncResponse) {
    service
        .emitAsync(event)
        .whenComplete(
            (result, err) -> {
              if (err != null) {
                log.error("Failed to handle request!", err);
                asyncResponse.resume(Response.status(500).build());
              } else {
                asyncResponse.resume(Response.status(201).build());
              }
            });
  }
}
