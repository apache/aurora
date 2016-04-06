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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ServerSetTest {

  @Test
  public void testJsonCodecRoundtrip() throws Exception {
    Codec<ServiceInstance> codec = ServerSet.JSON_CODEC;
    ServiceInstance instance1 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http", new Endpoint("foo", 8080)),
        Status.ALIVE)
        .setShard(0);
    byte[] data = ServerSets.serializeServiceInstance(instance1, codec);
    assertTrue(ServerSets.deserializeServiceInstance(data, codec).getServiceEndpoint().isSetPort());
    assertTrue(ServerSets.deserializeServiceInstance(data, codec).isSetShard());

    ServiceInstance instance2 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http-admin1", new Endpoint("foo", 8080)),
        Status.ALIVE);
    data = ServerSets.serializeServiceInstance(instance2, codec);
    assertTrue(ServerSets.deserializeServiceInstance(data, codec).getServiceEndpoint().isSetPort());
    assertFalse(ServerSets.deserializeServiceInstance(data, codec).isSetShard());

    ServiceInstance instance3 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.<String, Endpoint>of(),
        Status.ALIVE);
    data = ServerSets.serializeServiceInstance(instance3, codec);
    assertTrue(ServerSets.deserializeServiceInstance(data, codec).getServiceEndpoint().isSetPort());
    assertFalse(ServerSets.deserializeServiceInstance(data, codec).isSetShard());
  }

  @Test
  public void testJsonCompatibility() throws IOException {
    ServiceInstance instance = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http", new Endpoint("foo", 8080)),
        Status.ALIVE).setShard(42);

    ByteArrayOutputStream results = new ByteArrayOutputStream();
    ServerSet.JSON_CODEC.serialize(instance, results);
    assertEquals(
        "{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000},"
            + "\"additionalEndpoints\":{\"http\":{\"host\":\"foo\",\"port\":8080}},"
            + "\"status\":\"ALIVE\","
            + "\"shard\":42}",
        results.toString());
  }
}
