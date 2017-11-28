/**
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
package org.apache.aurora.scheduler.http;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import org.apache.aurora.scheduler.offers.OfferManager;

/**
 * Servlet that exposes resource offers that the scheduler is currently retaining.
 */
@Path("/offers")
public class Offers {

  private final OfferManager offerManager;
  private final ObjectMapper mapper;

  @Inject
  Offers(OfferManager offerManager) {
    this.offerManager = Objects.requireNonNull(offerManager);
    mapper = new ObjectMapper()
        .registerModule(new ProtobufModule())
        .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  /**
   * Dumps the offers queued in the scheduler.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers() throws JsonProcessingException {
    return Response.ok(
        mapper.writeValueAsString(
            StreamSupport.stream(offerManager.getAll().spliterator(), false)
                .map(o -> o.getOffer())
                .collect(Collectors.toList())))
        .build();
  }
}
