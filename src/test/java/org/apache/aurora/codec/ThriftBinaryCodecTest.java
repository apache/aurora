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
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ThriftBinaryCodecTest {

  @Test
  public void testRoundTrip() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();
    assertEquals(original,
        ThriftBinaryCodec.decode(ScheduledTask.class, ThriftBinaryCodec.encode(original)));
  }

  @Test
  public void testRoundTripNull() throws CodingException {
    assertNull(ThriftBinaryCodec.decode(ScheduledTask.class, ThriftBinaryCodec.encode(null)));
  }

  @Test
  public void testRoundTripNonNull() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();
    assertEquals(original,
        ThriftBinaryCodec.decodeNonNull(
            ScheduledTask.class,
            ThriftBinaryCodec.encodeNonNull(original)));
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNonNull() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNonNull() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(ScheduledTask.class, null);
  }

  @Test
  public void testInflateDeflateRoundTrip() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();

    byte[] deflated = ThriftBinaryCodec.deflateNonNull(original);

    ScheduledTask inflated = ThriftBinaryCodec.inflateNonNull(ScheduledTask.class, deflated);

    assertEquals(original, inflated);
  }
}
