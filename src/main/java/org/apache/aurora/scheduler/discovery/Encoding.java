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
package org.apache.aurora.scheduler.discovery;

import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;

/**
 * Utility class for encoding and decoding data stored in ZooKeeper nodes.
 */
public final class Encoding {

  private static final Gson GSON = new Gson();

  private Encoding() {
    // Utility class.
  }

  /**
   * Returns a serialized Thrift service instance object, with given endpoints and codec.
   *
   * @param serviceInstance the Thrift service instance object to be serialized
   * @return byte array that contains a serialized Thrift service instance
   */
  public static byte[] encode(ServiceInstance serviceInstance) throws IOException {
    return GSON.toJson(serviceInstance).getBytes(Charsets.UTF_8);
  }

  /**
   * Creates a service instance object deserialized from byte array.
   *
   * @param data the byte array contains a serialized Thrift service instance
   */
  public static ServiceInstance decode(byte[] data) throws JsonSyntaxException {
    ServiceInstance instance =
        GSON.fromJson(new String(data, Charsets.UTF_8), ServiceInstance.class);
    assertRequiredField("serviceInstance", instance);
    assertRequiredField("serviceEndpoint", instance.getServiceEndpoint());
    assertRequiredFields(instance.getServiceEndpoint());
    if (instance.getAdditionalEndpoints() != null) {
      instance.getAdditionalEndpoints().values().forEach(Encoding::assertRequiredFields);
    }

    return instance;
  }

  private static void assertRequiredFields(ServiceInstance.Endpoint endpoint) {
    assertRequiredField("host", endpoint.getHost());
  }

  private static void assertRequiredField(String fieldName, Object fieldValue) {
    if (fieldValue == null) {
      throw new JsonParseException(String.format("Field %s is required", fieldName));
    }
  }
}
