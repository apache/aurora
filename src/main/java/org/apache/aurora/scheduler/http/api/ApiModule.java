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

import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.servlet.ServletModule;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.scheduler.http.CorsFilter;
import org.apache.aurora.scheduler.http.LeaderRedirectFilter;
import org.apache.aurora.scheduler.http.api.TContentAwareServlet.ContentFactoryPair;
import org.apache.aurora.scheduler.http.api.TContentAwareServlet.InputConfig;
import org.apache.aurora.scheduler.http.api.TContentAwareServlet.OutputConfig;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.util.resource.Resource;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class ApiModule extends ServletModule {
  public static final String API_PATH = "/api";
  private static final MediaType GENERIC_THRIFT = new MediaType("application", "x-thrift");
  private static final MediaType THRIFT_JSON =
      new MediaType("application", "vnd.apache.thrift.json");
  private static final MediaType THRIFT_BINARY =
      new MediaType("application", "vnd.apache.thrift.binary");

  @Parameters(separators = "=")
  public static class Options {
    /**
     * Set the {@code Access-Control-Allow-Origin} header for API requests. See
     * http://www.w3.org/TR/cors/
     */
    @Parameter(names = "-enable_cors_for",
        description = "List of domains for which CORS support should be enabled.")
    public String enableCorsFor;
  }

  private final Options options;

  public ApiModule(Options options) {
    this.options = options;
  }

  private static final String API_CLIENT_ROOT = Resource
      .newClassPathResource("org/apache/aurora/scheduler/gen/client")
      .toString();

  @Override
  protected void configureServlets() {
    if (options.enableCorsFor != null) {
      filter(API_PATH).through(new CorsFilter(options.enableCorsFor));
    }
    serve(API_PATH).with(TContentAwareServlet.class);

    filter(ApiBeta.PATH, ApiBeta.PATH + "/*").through(LeaderRedirectFilter.class);
    bind(ApiBeta.class);

    serve("/apiclient", "/apiclient/*")
        .with(new DefaultServlet(), ImmutableMap.<String, String>builder()
            .put("resourceBase", API_CLIENT_ROOT)
            .put("pathInfoOnly", "true")
            .put("dirAllowed", "false")
            .build());
  }

  @Provides
  @Singleton
  TContentAwareServlet provideApiThriftServlet(AnnotatedAuroraAdmin schedulerThriftInterface) {
    /*
     * For backwards compatibility the servlet is configured to assume `application/x-thrift` and
     * `application/json` have TJSON bodies.
     *
     * Requests that have the registered MIME type for apache thrift are mapped to their respective
     * protocols. See
     * http://www.iana.org/assignments/media-types/application/vnd.apache.thrift.binary and
     * http://www.iana.org/assignments/media-types/application/vnd.apache.thrift.json for details.
     *
     * Responses have the registered MIME type so the client can decode appropriately.
     *
     * The Accept header is used to determine the response type. By default JSON is sent for any
     * value except for the binary thrift header.
     */

    ContentFactoryPair jsonFactory = new ContentFactoryPair(
        new TJSONProtocol.Factory(),
        THRIFT_JSON);
    ContentFactoryPair binFactory = new ContentFactoryPair(
        new TBinaryProtocol.Factory(),
        THRIFT_BINARY);

    // Which factory to use based on the Content-Type header of the request for reading the request.
    InputConfig inputConfig = new InputConfig(GENERIC_THRIFT, ImmutableMap.of(
        GENERIC_THRIFT, jsonFactory,
        THRIFT_JSON, jsonFactory,
        APPLICATION_JSON_TYPE, jsonFactory,
        THRIFT_BINARY, binFactory
    ));

    // Which factory to use based on the Accept header of the request for the response.
    OutputConfig outputConfig = new OutputConfig(APPLICATION_JSON_TYPE, ImmutableMap.of(
        APPLICATION_JSON_TYPE, jsonFactory,
        GENERIC_THRIFT, jsonFactory,
        THRIFT_JSON, jsonFactory,
        THRIFT_BINARY, binFactory
        ));

    // A request without a Content-Type (like from curl) should be treated as GENERIC_THRIFT
    return new TContentAwareServlet(
        new AuroraAdmin.Processor<>(schedulerThriftInterface),
        inputConfig,
        outputConfig);
  }
}
