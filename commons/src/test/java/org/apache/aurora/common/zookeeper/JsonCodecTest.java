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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;

import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Gson.class)
public class JsonCodecTest {

  private static final Codec<ServiceInstance> STANDARD_JSON_CODEC = new JsonCodec();

  @Test
  public void testJsonCodecRoundtrip() throws Exception {
    Codec<ServiceInstance> codec = STANDARD_JSON_CODEC;
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
    STANDARD_JSON_CODEC.serialize(instance, results);
    assertEquals(
        "{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000},"
            + "\"additionalEndpoints\":{\"http\":{\"host\":\"foo\",\"port\":8080}},"
            + "\"status\":\"ALIVE\","
            + "\"shard\":42}",
        results.toString());
  }

  @Test
  public void testInvalidSerialize() {
    // Gson is final so we need to call on PowerMock here.
    Gson gson = PowerMock.createMock(Gson.class);
    gson.toJson(EasyMock.isA(Object.class), EasyMock.isA(Appendable.class));
    EasyMock.expectLastCall().andThrow(new JsonIOException("error"));
    PowerMock.replay(gson);

    ServiceInstance instance =
        new ServiceInstance(new Endpoint("foo", 1000), ImmutableMap.of(), Status.ALIVE);

    try {
      new JsonCodec(gson).serialize(instance, new ByteArrayOutputStream());
      fail();
    } catch (IOException e) {
      // Expected.
    }

    PowerMock.verify(gson);
  }

  @Test
  public void testDeserializeMinimal() throws IOException {
    String minimal = "{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000},\"status\":\"ALIVE\"}";
    ByteArrayInputStream source = new ByteArrayInputStream(minimal.getBytes(Charsets.UTF_8));
    ServiceInstance actual = STANDARD_JSON_CODEC.deserialize(source);
    ServiceInstance expected =
        new ServiceInstance(new Endpoint("foo", 1000), ImmutableMap.of(), Status.ALIVE);
    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidDeserialize() {
    // Not JSON.
    assertInvalidDeserialize(new byte[] {0xC, 0xA, 0xF, 0xE});

    // No JSON object.
    assertInvalidDeserialize("");
    assertInvalidDeserialize("[]");

    // Missing required fields.
    assertInvalidDeserialize("{}");
    assertInvalidDeserialize("{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000}}");
    assertInvalidDeserialize("{\"status\":\"ALIVE\"}");
  }

  private void assertInvalidDeserialize(String data) {
    assertInvalidDeserialize(data.getBytes(Charsets.UTF_8));
  }

  private void assertInvalidDeserialize(byte[] data) {
    try {
      STANDARD_JSON_CODEC.deserialize(new ByteArrayInputStream(data));
      fail();
    } catch (IOException e) {
      // Expected.
    }
  }
}
