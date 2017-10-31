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
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParseException;

import org.apache.aurora.scheduler.discovery.ServiceInstance.Endpoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EncodingTest {

  @Test
  public void testEncodingRoundTrip() throws Exception {
    ServiceInstance instance1 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http", new Endpoint("foo", 8080)));
    byte[] data = Encoding.encode(instance1);
    assertEquals(instance1, Encoding.decode(data));

    ServiceInstance instance2 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http-admin1", new Endpoint("foo", 8080)));
    data = Encoding.encode(instance2);
    assertEquals(instance2, Encoding.decode(data));

    ServiceInstance instance3 = new ServiceInstance(new Endpoint("foo", 1000), ImmutableMap.of());
    data = Encoding.encode(instance3);
    assertEquals(instance3, Encoding.decode(data));
  }

  @Test
  public void testJsonCompatibility() throws IOException {
    ServiceInstance instance = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http", new Endpoint("foo", 8080)));

    byte[] encoded = Encoding.encode(instance);
    assertEquals(
        "{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000},"
            + "\"additionalEndpoints\":{\"http\":{\"host\":\"foo\",\"port\":8080}},"
            + "\"status\":\"ALIVE\"}",
        new String(encoded, Charsets.UTF_8));
  }

  @Test
  public void testDeserializeMinimal() throws IOException {
    String minimal = "{\"serviceEndpoint\":{\"host\":\"foo\",\"port\":1000},\"status\":\"ALIVE\"}";
    ServiceInstance actual = Encoding.decode(minimal.getBytes(Charsets.UTF_8));
    ServiceInstance expected =
        new ServiceInstance(new Endpoint("foo", 1000), ImmutableMap.of());
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
    assertInvalidDeserialize("{\"status\":\"ALIVE\"}");
  }

  private void assertInvalidDeserialize(String data) {
    assertInvalidDeserialize(data.getBytes(Charsets.UTF_8));
  }

  private void assertInvalidDeserialize(byte[] data) {
    try {
      Encoding.decode(data);
      fail();
    } catch (JsonParseException e) {
      // Expected.
    }
  }
}
