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
package org.apache.aurora.common.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParseException;

import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;

import static java.util.Objects.requireNonNull;

class JsonCodec implements Codec<ServiceInstance> {

  private static void assertRequiredField(String fieldName, Object fieldValue) {
    if (fieldValue == null) {
      throw new JsonParseException(String.format("Field %s is required", fieldName));
    }
  }

  private static class EndpointSchema {
    private final String host;
    private final Integer port;

    EndpointSchema(Endpoint endpoint) {
      host = endpoint.getHost();
      port = endpoint.getPort();
    }

    Endpoint asEndpoint() {
      assertRequiredField("host", host);
      assertRequiredField("port", port);

      return new Endpoint(host, port);
    }
  }

  private static class ServiceInstanceSchema {
    private final EndpointSchema serviceEndpoint;
    private final Map<String, EndpointSchema> additionalEndpoints;
    private final Status status;
    private final @Nullable Integer shard;

    ServiceInstanceSchema(ServiceInstance instance) {
      serviceEndpoint = new EndpointSchema(instance.getServiceEndpoint());
      if (instance.isSetAdditionalEndpoints()) {
        additionalEndpoints =
            Maps.transformValues(instance.getAdditionalEndpoints(), EndpointSchema::new);
      } else {
        additionalEndpoints = ImmutableMap.of();
      }
      status  = instance.getStatus();
      shard = instance.isSetShard() ? instance.getShard() : null;
    }

    ServiceInstance asServiceInstance() {
      assertRequiredField("serviceEndpoint", serviceEndpoint);
      assertRequiredField("status", status);

      Map<String, EndpointSchema> extraEndpoints =
          additionalEndpoints == null ? ImmutableMap.of() : additionalEndpoints;

      ServiceInstance instance =
          new ServiceInstance(
              serviceEndpoint.asEndpoint(),
              Maps.transformValues(extraEndpoints, EndpointSchema::asEndpoint),
              status);
      if (shard != null) {
        instance.setShard(shard);
      }
      return instance;
    }
  }

  private static final Charset ENCODING = Charsets.UTF_8;

  private final Gson gson;

  JsonCodec() {
    this(new Gson());
  }

  JsonCodec(Gson gson) {
    this.gson = requireNonNull(gson);
  }

  @Override
  public void serialize(ServiceInstance instance, OutputStream sink) throws IOException {
    Writer writer = new OutputStreamWriter(sink, ENCODING);
    try {
      gson.toJson(new ServiceInstanceSchema(instance), writer);
    } catch (JsonIOException e) {
      throw new IOException(String.format("Problem serializing %s to JSON", instance), e);
    }
    writer.flush();
  }

  @Override
  public ServiceInstance deserialize(InputStream source) throws IOException {
    InputStreamReader reader = new InputStreamReader(source, ENCODING);
    try {
      @Nullable ServiceInstanceSchema schema = gson.fromJson(reader, ServiceInstanceSchema.class);
      if (schema == null) {
        throw new IOException("JSON did not include a ServiceInstance object");
      }
      return schema.asServiceInstance();
    } catch (JsonParseException e) {
      throw new IOException("Problem parsing JSON ServiceInstance.", e);
    }
  }
}
