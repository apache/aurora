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
package org.apache.aurora.common.net.http.handlers;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import org.apache.aurora.common.stats.Stat;

/**
 * A servlet that returns the current value of all variables in JSON format.
 * The format returns a JSON object with string fields and typed values:
 * <pre>
 *   {
 *     "var_a": 1,
 *     "var_b": 126.0,
 *     "var_c": "a string value",
 *   }
 * </pre>
 * If the optional URL parameter 'pretty' is used, the output will be pretty-printed
 * (similar to the above example).
 *
 * TODO(wfarner): Handle this request in VarsHandler.
 */
@Path("/vars.json")
public class VarsJsonHandler {

  private final Supplier<Iterable<Stat<?>>> statSupplier;

  /**
   * Creates a new handler that will report stats from the provided supplier.
   *
   * @param statSupplier Stats supplier.
   */
  @Inject
  public VarsJsonHandler(Supplier<Iterable<Stat<?>>> statSupplier) {
    this.statSupplier = Preconditions.checkNotNull(statSupplier);
  }

  @VisibleForTesting
  String getBody(boolean pretty) {
    Map<String, Object> vars = Maps.newLinkedHashMap();
    for (Stat<?> var : statSupplier.get()) {
      vars.put(var.getName(), var.read());
    }
    // TODO(wfarner): Let the jax-rs provider handle serialization.
    return getGson(pretty).toJson(vars);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public String getVars(@QueryParam("pretty") boolean pretty) {
    return getBody(pretty);
  }

  private Gson getGson(boolean pretty) {
    return pretty ? new GsonBuilder().setPrettyPrinting().create() : new Gson();
  }
}
