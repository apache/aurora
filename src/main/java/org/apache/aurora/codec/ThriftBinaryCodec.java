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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nullable;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import static java.util.Objects.requireNonNull;

/**
 * Codec that works for thrift objects.
 */
public final class ThriftBinaryCodec {

  /**
   * Protocol factory used for all thrift encoding and decoding.
   */
  public static final TProtocolFactory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

  private ThriftBinaryCodec() {
    // Utility class.
  }

  /**
   * Identical to {@link #decodeNonNull(Class, byte[])}, but allows for a null buffer.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message, or {@code null} if the buffer was {@code null}.
   * @throws CodingException If the message could not be decoded.
   */
  @Nullable
  public static <T extends TBase<T, ?>> T decode(Class<T> clazz, @Nullable byte[] buffer)
      throws CodingException {

    if (buffer == null) {
      return null;
    }
    return decodeNonNull(clazz, buffer);
  }

  /**
   * Decodes a binary-encoded byte array into a target type.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends TBase<T, ?>> T decodeNonNull(Class<T> clazz, byte[] buffer)
      throws CodingException {

    requireNonNull(clazz);
    requireNonNull(buffer);

    try {
      T t = newInstance(clazz);
      new TDeserializer(PROTOCOL_FACTORY).deserialize(t, buffer);
      return t;
    } catch (TException e) {
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  /**
   * Identical to {@link #encodeNonNull(TBase)}, but allows for a null input.
   *
   * @param tBase Object to encode.
   * @return Encoded object, or {@code null} if the argument was {@code null}.
   * @throws CodingException If the object could not be encoded.
   */
  @Nullable
  public static byte[] encode(@Nullable TBase<?, ?> tBase) throws CodingException {
    if (tBase == null) {
      return null;
    }
    return encodeNonNull(tBase);
  }

  /**
   * Encodes a thrift object into a binary array.
   *
   * @param tBase Object to encode.
   * @return Encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static byte[] encodeNonNull(TBase<?, ?> tBase) throws CodingException {
    requireNonNull(tBase);

    try {
      return new TSerializer(PROTOCOL_FACTORY).serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  // See http://www.zlib.net/zlib_how.html
  // "If the memory is available, buffers sizes on the order of 128K or 256K bytes should be used."
  private static final int DEFLATER_BUFFER_SIZE = Amount.of(256, Data.KB).as(Data.BYTES);

  // Empirical from microbenchmarks (assuming 20MiB/s writes to the replicated log and a large
  // de-duplicated Snapshot from a production environment).
  // TODO(ksweeney): Consider making this configurable.
  private static final int DEFLATE_LEVEL = 3;

  /**
   * Encodes a thrift object into a DEFLATE-compressed binary array.
   *
   * @param tBase Object to encode.
   * @return Deflated, encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static byte[] deflateNonNull(TBase<?, ?> tBase) throws CodingException {
    requireNonNull(tBase);

    // NOTE: Buffering is needed here for performance.
    // There are actually 2 buffers in play here - the BufferedOutputStream prevents thrift from
    // causing a call to deflate() on every encoded primitive. The DeflaterOutputStream buffer
    // allows the underlying Deflater to operate on a larger chunk at a time without stopping to
    // copy the intermediate compressed output to outBytes.
    // See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4986239
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    TTransport transport = new TIOStreamTransport(
        new BufferedOutputStream(
            new DeflaterOutputStream(outBytes, new Deflater(DEFLATE_LEVEL), DEFLATER_BUFFER_SIZE),
            DEFLATER_BUFFER_SIZE));
    try {
      TProtocol protocol = PROTOCOL_FACTORY.getProtocol(transport);
      tBase.write(protocol);
      transport.close(); // calls finish() on the underlying stream, completing the compression
      return outBytes.toByteArray();
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    } finally {
      transport.close();
    }
  }

  /**
   * Decodes a thrift object from a DEFLATE-compressed byte array into a target type.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Compressed buffer to decode.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends TBase<T, ?>> T inflateNonNull(Class<T> clazz, byte[] buffer)
      throws CodingException {

    requireNonNull(clazz);
    requireNonNull(buffer);

    T tBase = newInstance(clazz);
    TTransport transport = new TIOStreamTransport(
          new InflaterInputStream(new ByteArrayInputStream(buffer)));
    try {
      TProtocol protocol = PROTOCOL_FACTORY.getProtocol(transport);
      tBase.read(protocol);
      return tBase;
    } catch (TException e) {
      throw new CodingException("Failed to deserialize: " + e, e);
    } finally {
      transport.close();
    }
  }

  private static <T extends TBase<T, ?>> T newInstance(Class<T> clazz) throws CodingException {
    try {
      return clazz.getConstructor().newInstance();
    } catch (InvocationTargetException e) {
      throw new CodingException("Exception in constructor for target type: " + e, e);
    } catch (NoSuchMethodException e) {
      throw new CodingException(
          "No no-args constructor for target type: "
              + clazz
              + ". Did the thrift code generator change?",
          e);
    } catch (InstantiationException e) {
      throw new CodingException("Failed to instantiate target type.", e);
    } catch (IllegalAccessException e) {
      throw new CodingException("Failed to access constructor for target type.", e);
    }
  }

  /**
   * Thrown when serialization or deserialization failed.
   */
  public static class CodingException extends RuntimeException {
    public CodingException(String message) {
      super(message);
    }
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
