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
package org.apache.aurora.codec;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.Identity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ThriftBinaryCodecTest {

  @Test
  public void testRoundTrip() throws CodingException {
    Identity original = new Identity("mesos", "jack");
    assertEquals(original,
        ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(original)));
  }

  @Test
  public void testRoundTripNull() throws CodingException {
    assertNull(ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(null)));
  }

  @Test
  public void testRoundTripNonNull() throws CodingException {
    Identity original = new Identity("mesos", "jill");
    assertEquals(original,
        ThriftBinaryCodec.decodeNonNull(Identity.class, ThriftBinaryCodec.encodeNonNull(original)));
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNonNull() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNonNull() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(Identity.class, null);
  }
}
