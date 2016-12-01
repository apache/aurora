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

import java.net.InetSocketAddress;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServerSetsTest {
  @Test
  public void testSimpleSerialization() throws Exception {
    InetSocketAddress endpoint = new InetSocketAddress(12345);
    Map<String, Endpoint > additionalEndpoints = ImmutableMap.of();
    Status status = Status.ALIVE;

    byte[] data = ServerSets.serializeServiceInstance(
        endpoint, additionalEndpoints, status, ServerSet.JSON_CODEC);

    ServiceInstance instance = ServerSets.deserializeServiceInstance(data, ServerSet.JSON_CODEC);

    assertEquals(endpoint.getPort(), instance.getServiceEndpoint().getPort());
    assertEquals(additionalEndpoints, instance.getAdditionalEndpoints());
    assertEquals(Status.ALIVE, instance.getStatus());
  }
}
