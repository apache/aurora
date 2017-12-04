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
package org.apache.aurora.scheduler.http.api;

import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;

import org.apache.aurora.gen.AuroraAdmin.Iface;
import org.apache.aurora.scheduler.storage.entities.AuroraAdminMetadata;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.scheduler.http.api.GsonMessageBodyHandler.GSON;

/**
 * A scheduler interface that allows interaction with the thrift API via traditional JSON,
 * rather than thrift's preferred means which uses field IDs.
 */
@Path(ApiBeta.PATH)
public class ApiBeta {
  static final String PATH = "/apibeta";

  private static final Logger LOG = LoggerFactory.getLogger(ApiBeta.class);

  private final Iface api;

  @Inject
  ApiBeta(AnnotatedAuroraAdmin api) {
    this.api = Objects.requireNonNull(api);
  }

  private JsonElement getJsonMember(JsonObject json, String memberName) {
    return (json == null) ? null : json.get(memberName);
  }

  private static Response errorResponse(Status status, String message) {
    return Response.status(status)
        .entity(Responses.error(message))
        .build();
  }

  private static Response badRequest(String message) {
    return errorResponse(Status.BAD_REQUEST, message);
  }

  /**
   * Parses method parameters into the appropriate types.  For a method call to be successful,
   * the elements supplied in the request must match the names of those specified in the thrift
   * method definition.  If a method parameter does not exist in the request object, {@code null}
   * will be substituted.
   *
   * @param json Incoming request data, to translate into method parameters.
   * @param method The thrift method to bind parameters for.
   * @return Parsed method parameters.
   * @throws WebApplicationException If a parameter could not be parsed.
   */
  private Object[] readParams(JsonObject json, Method method)
      throws WebApplicationException {

    List<Object> params = Lists.newArrayList();
    for (Parameter param : method.getParameters()) {
      try {
        params.add(GSON.fromJson(getJsonMember(json, param.getName()), param.getType()));
      } catch (JsonParseException e) {
        throw new WebApplicationException(
            e,
            badRequest("Failed to parse parameter " + param + ": " + e.getMessage()));
      }
    }
    return params.toArray();
  }

  private Method getApiMethod(String name, Class<?>... parameterTypes) {
    try {
      return Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Path("{method}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response invoke(@PathParam("method") String methodName, String postData) {
    LOG.debug("Call to {} with data: {}", methodName, postData);

    // First, verify that this is a valid method on the interface.
    Class<?>[] methodParameterTypes = AuroraAdminMetadata.METHODS.get(methodName);
    if (methodParameterTypes == null) {
      return errorResponse(Status.NOT_FOUND, "Method " + methodName + " does not exist.");
    }

    JsonObject parameters;
    try {
      JsonElement json = GSON.fromJson(postData, JsonElement.class);
      // The parsed object will be null if there was no post data.  This is okay, since that is
      // expected for a zero-parameter method.
      if (json != null && !(json instanceof JsonObject)) {
        throw new WebApplicationException(
            badRequest("Request data must be a JSON object of method parameters."));
      }
      parameters = (JsonObject) json;
    } catch (JsonSyntaxException e) {
      throw new WebApplicationException(e, badRequest("Request must be valid JSON"));
    }

    final Method method = getApiMethod(methodName, methodParameterTypes);
    final Object[] params = readParams(parameters, method);
    return Response.ok((StreamingOutput) output -> {
      try {
        Object response = method.invoke(api, params);
        try (OutputStreamWriter out = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
          GSON.toJson(response, out);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }).build();
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response getIndex() {
    return Response.seeOther(URI.create("/apihelp/index.html")).build();
  }
}
