package com.twitter.mesos;

import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import mesos.FrameworkMessage;
import org.apache.thrift.TBase;

/**
 * Codec to handle wrapping/unwrapping and serialization/deserialization of thrift messages from
 * the binary payload of a framework message.
 *
 * @author wfarner
 */
public class FrameworkMessageCodec<T extends TBase>
    implements Codec<T, FrameworkMessage> {

  private final Codec<T, byte[]> codec;

  public FrameworkMessageCodec(Class<T> messageClass) {
    codec = new ThriftBinaryCodec<T>(messageClass);
  }

  @Override
  public FrameworkMessage encode(T b) throws CodingException {
    FrameworkMessage frameworkMsg = new FrameworkMessage();
    frameworkMsg.setData(codec.encode(b));
    return frameworkMsg;
  }

  @Override
  public T decode(FrameworkMessage msg) throws CodingException {
    return codec.decode(msg.getData());
  }
}
