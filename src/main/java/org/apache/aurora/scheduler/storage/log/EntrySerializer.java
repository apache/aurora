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

import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.storage.Frame;
import org.apache.aurora.gen.storage.FrameChunk;
import org.apache.aurora.gen.storage.FrameHeader;
import org.apache.aurora.gen.storage.LogEntry;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import static org.apache.aurora.scheduler.storage.log.LogManager.LogEntryHashFunction;
import static org.apache.aurora.scheduler.storage.log.LogManager.MaxEntrySize;

/**
 * Logic for serializing distributed log entries.
 */
public interface EntrySerializer {
  /**
   * Serializes a log entry and splits it into chunks no larger than {@code maxEntrySizeBytes}. The
   * returned iterable's iterator is not thread-safe.
   *
   * @param logEntry The log entry to serialize.
   * @return Serialized and chunked log entry.
   * @throws CodingException If the entry could not be serialized.
   */
  Iterable<byte[]> serialize(LogEntry logEntry) throws CodingException;

  @VisibleForTesting
  class EntrySerializerImpl implements EntrySerializer {
    private final HashFunction hashFunction;
    private final int maxEntrySizeBytes;

    @Inject
    @VisibleForTesting
    public EntrySerializerImpl(
        @MaxEntrySize Amount<Integer, Data> maxEntrySize,
        @LogEntryHashFunction HashFunction hashFunction) {

      this.hashFunction = requireNonNull(hashFunction);
      maxEntrySizeBytes = maxEntrySize.as(Data.BYTES);
    }

    @Override
    @Timed("log_entry_serialize")
    public Iterable<byte[]> serialize(LogEntry logEntry) throws CodingException {
      final byte[] entry = Entries.thriftBinaryEncode(logEntry);
      if (entry.length <= maxEntrySizeBytes) {
        return ImmutableList.of(entry);
      }

      final int chunks = (int) Math.ceil(entry.length / (double) maxEntrySizeBytes);

      final byte[] header = encode(
          Frame.header(new FrameHeader(chunks, ByteBuffer.wrap(checksum(entry)))));

      return new Iterable<byte[]>() {
        @Override
        public Iterator<byte[]> iterator() {
          return streamFrames(header, chunks, entry);
        }
      };
    }

    Iterator<byte[]> streamFrames(final byte[] header, final int chunks, final byte[] entry) {
      return new AbstractIterator<byte[]>() {
        private int i = -1;

        @Override
        protected byte[] computeNext() {
          byte[] result;
          if (i == -1) {
            result = header;
          } else if (i < chunks) {
            int offset = i * maxEntrySizeBytes;
            ByteBuffer chunk =
                ByteBuffer.wrap(entry, offset, Math.min(maxEntrySizeBytes, entry.length - offset));
            try {
              result = encode(Frame.chunk(new FrameChunk(chunk)));
            } catch (CodingException e) {
              throw Throwables.propagate(e);
            }
          } else {
            return endOfData();
          }

          i++;
          return result;
        };
      };
    }

    @Timed("log_entry_checksum")
    protected byte[] checksum(byte[] data) {
      // TODO(ksweeney): Use the streaming API here.
      return hashFunction.hashBytes(data).asBytes();
    }

    @Timed("log_entry_encode")
    protected byte[] encode(Frame frame) throws CodingException {
      return Entries.thriftBinaryEncode(LogEntry.frame(frame));
    }
  }
}
