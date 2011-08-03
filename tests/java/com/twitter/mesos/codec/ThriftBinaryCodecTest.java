package com.twitter.mesos.codec;

import org.junit.Test;

import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.Identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author John Sirois
 */
public class ThriftBinaryCodecTest {

  @Test
  public void testRoundTrip() throws CodingException {
    Identity original = new Identity("mesos", "jack");
    assertEquals(original,
        ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(original)));
  }

  @Test
  public void testRoundTrip_null() throws CodingException {
    assertNull(ThriftBinaryCodec.decode(Identity.class, ThriftBinaryCodec.encode(null)));
  }

  @Test
  public void testRoundTripNonNull() throws CodingException {
    Identity original = new Identity("mesos", "jill");
    assertEquals(original,
        ThriftBinaryCodec.decodeNonNull(Identity.class, ThriftBinaryCodec.encodeNonNull(original)));
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNonNull_null() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNonNull_null() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(Identity.class, null);
  }
}
