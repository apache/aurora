/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.storage.log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.twitter.common.stats.Stats;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.LogEntry._Fields;

/**
 * Utility class for working with log entries.
 */
final class Entries {

  private static final Logger LOG = Logger.getLogger(Entries.class.getName());

  private static final AtomicLong COMPRESSION_BYTES_SAVED =
      Stats.exportLong("log_compressed_entry_bytes_saved");

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
  static LogEntry deflate(LogEntry entry) throws CodingException {
    byte[] data = thriftBinaryEncode(entry);
    int initialLength = data.length;
    LOG.info("Deflating log entry of size " + initialLength);
    ByteArrayOutputStream deflated = new ByteArrayOutputStream();
    DeflaterOutputStream deflater = new DeflaterOutputStream(deflated);
    try {
      deflater.write(data);
      deflater.flush();
      deflater.close();
      byte[] deflatedData = deflated.toByteArray();
      int bytesSaved = initialLength - deflatedData.length;
      if (bytesSaved < 0) {
        LOG.warning("Deflated entry is larger than original by " + (bytesSaved * -1) + " bytes");
      } else {
        LOG.info("Deflated log entry size: " + deflatedData.length + " (saved " + bytesSaved + ")");
      }

      COMPRESSION_BYTES_SAVED.addAndGet(bytesSaved);
      return LogEntry.deflatedEntry(ByteBuffer.wrap(deflatedData));
    } catch (IOException e) {
      throw new CodingException("Failed to deflate snapshot: " + e, e);
    }
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

    ByteArrayOutputStream inflated = new ByteArrayOutputStream();
    ByteBuffer data = entry.bufferForDeflatedEntry();
    LOG.info("Inflating deflated log entry of size " + data.remaining());
    InflaterInputStream inflater = new InflaterInputStream(
        new ByteArrayInputStream(data.array(), data.position(), data.remaining()));
    try {
      ByteStreams.copy(inflater, inflated);
      byte[] inflatedData = inflated.toByteArray();
      LOG.info("Inflated log entry size: " + inflatedData.length);
      return thriftBinaryDecode(inflatedData);
    } catch (IOException e) {
      throw new CodingException("Failed to inflate compressed log entry.", e);
    }
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
