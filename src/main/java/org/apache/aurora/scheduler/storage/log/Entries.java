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
package org.apache.aurora.scheduler.storage.log;

import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.LogEntry._Fields;

/**
 * Utility class for working with log entries.
 */
public final class Entries {

  private static final Logger LOG = Logger.getLogger(Entries.class.getName());

  private Entries() {
    // Utility class.
  }

  /**
   * Deflates a log entry and wraps it in a deflated entry.
   * <p>
   * This will encode the entry using the thrift binary codec, and will apply deflate compression to
   * the resulting encoded data.
   * <p>
   * This operation is symmetric with {@link #inflate(LogEntry)}.
   *
   * @param entry Entry to deflate.
   * @return An entry with the {@code deflatedEntry} field set with the deflated serialized value
   *         of the original entry.
   * @throws CodingException If the value could not be encoded or deflated.
   */
  public static LogEntry deflate(LogEntry entry) throws CodingException {
    return LogEntry.deflatedEntry(ThriftBinaryCodec.deflateNonNull(entry));
  }

  /**
   * Inflates and deserializes a deflated log entry.
   * <p>
   * This requires that the {@code deflatedEntry} field is set on the provided {@code entry}.
   * The encoded value will be inflated and deserialized as a {@link LogEntry}.
   *
   * @param entry Entry to inflate, which must be a deflated entry.
   * @return The inflated entry.
   * @throws CodingException If the value could not be inflated or decoded.
   */
  static LogEntry inflate(LogEntry entry) throws CodingException {
    Preconditions.checkArgument(entry.isSet(_Fields.DEFLATED_ENTRY));

    byte[] data = entry.getDeflatedEntry();
    LOG.info("Inflating deflated log entry of size " + data.length);
    return ThriftBinaryCodec.inflateNonNull(LogEntry.class, entry.getDeflatedEntry());
  }

  /**
   * Thrift binary-encodes a log entry.
   *
   * @param entry The entry to encode.
   * @return The serialized entry value.
   * @throws CodingException If the entry could not be encoded.
   */
  static byte[] thriftBinaryEncode(LogEntry entry) throws CodingException {
    return ThriftBinaryCodec.encodeNonNull(entry);
  }

  /**
   * Decodes a byte array containing thrift binary-encoded data.
   *
   * @param contents The data to decode.
   * @return The deserialized entry.
   * @throws CodingException If the entry could not be deserialized.
   */
  static LogEntry thriftBinaryDecode(byte[] contents) throws CodingException {
    return ThriftBinaryCodec.decodeNonNull(LogEntry.class, contents);
  }
}
