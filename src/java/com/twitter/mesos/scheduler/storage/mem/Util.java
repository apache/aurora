package com.twitter.mesos.scheduler.storage.mem;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import org.apache.thrift.TBase;

import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;

/**
 * Utility class for common operations amongst in-memory store implementations.
 */
final class Util {

  private Util() {
    // Utility class.
  }

  /**
   * Creates a function that performs a 'deep copy' on a thrift struct of a specific type.  The
   * resulting copied objects will be exactly identical to the original.  Mutations to the original
   * object will not be reflected in the copy, and vice versa.
   *
   * @param cls Concrete class of the copier type.
   * @param <T> Copier type parameter.
   * @return A copier for the provided type of thrift structs.
   */
  static <T extends TBase<T, ?>> Function<T, T> deepCopier(final Class<T> cls) {
    return new Function<T, T>() {
      @Override public T apply(@Nullable T input) {
        // TODO(William Farner): Use thrift's deepCopy() here instead, as it is likely to be much
        // more efficient.  Initial attempts to do this resulted in test failures which is
        // rooted in inconsistent results between deepCopy() and encode/decode.  This appears to
        // be caused by incorrect restoration of the isset bit vector when encoding/decoding.
        // Though deepCopy() appears to have the correct behavior, proceed with the existing
        // behavior for now.
        try {
          return ThriftBinaryCodec.decode(cls, ThriftBinaryCodec.encode(input));
        } catch (CodingException e) {
          throw new IllegalStateException("Failed to deep copy struct " + input, e);
        }
      }
    };
  }
}
