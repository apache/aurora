/*
 * Copyright 2013 Twitter, Inc.
 *
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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

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

    Preconditions.checkNotNull(clazz);
    Preconditions.checkNotNull(buffer);

    try {
      T t = clazz.newInstance();
      new TDeserializer(PROTOCOL_FACTORY).deserialize(t, buffer);
      return t;
    } catch (IllegalAccessException e) {
      throw new CodingException("Failed to access constructor for target type.", e);
    } catch (InstantiationException e) {
      throw new CodingException("Failed to instantiate target type.", e);
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
    Preconditions.checkNotNull(tBase);

    try {
      return new TSerializer(PROTOCOL_FACTORY).serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  /**
   * Thrown when serialization or deserialization failed.
   */
  public static class CodingException extends Exception {
    public CodingException(String message) {
      super(message);
    }
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
