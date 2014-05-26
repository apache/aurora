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
package org.apache.aurora.scheduler.log.testing;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import com.twitter.common.base.Closure;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.test.FileLogContents;
import org.apache.aurora.gen.test.LogRecord;
import org.apache.aurora.scheduler.log.Log;

/**
 * A log implementation that reads from and writes to a local file.
 * <p>
 * This should never be used in a production setting, it is only intended for local testing.
 * TODO(wfarner): Bind/inject a settable flag that indicates we are running with test settings.
 * Surface this in a banner on the web UI.
 */
class FileLog implements Log {

  private final File logFile;

  @Inject
  FileLog(File logFile) {
    this.logFile = Preconditions.checkNotNull(logFile);
  }

  @Override
  public Stream open() throws IOException {
    try {
      FileLogContents logContents;
      // Treat an empty file as a new file.
      // NOTE: we can't use logFile.length == 0 to test for an empty file, since empty unicode files
      // contain a byte order mark (BOM) header that is 255 bytes in length.
      if (logFile.createNewFile()
          || Files.readFirstLine(logFile, Charset.defaultCharset()) == null) {
        logContents = new FileLogContents(Maps.<Long, LogRecord>newHashMap());
      } else {
        logContents = ThriftBinaryCodec.decode(FileLogContents.class, Files.toByteArray(logFile));
      }
      Closure<FileLogContents> logWriter = new Closure<FileLogContents>() {
        @Override
        public void execute(FileLogContents logContents) {
          try {
            Files.write(ThriftBinaryCodec.encode(logContents), logFile);
          } catch (IOException | CodingException e) {
            throw Throwables.propagate(e);
          }
        }
      };
      return new FileStream(logContents, logWriter);
    } catch (CodingException e) {
      throw new IOException("Failed to interpret log contents: " + e, e);
    }
  }

  private static class FileStream implements Stream {
    private final FileLogContents logContents;
    private final Closure<FileLogContents> logWriter;
    private long nextPosition;

    FileStream(FileLogContents logContents, Closure<FileLogContents> logWriter) {
      this.logContents = logContents;
      this.logWriter = logWriter;
      nextPosition = logContents.getRecords().isEmpty()
          ? 1
          : Ordering.natural().max(logContents.getRecords().keySet()) + 1;
    }

    @Override
    public Position append(byte[] contents) throws StreamAccessException {
      logContents.getRecords().put(nextPosition, new LogRecord(ByteBuffer.wrap(contents)));
      Position position = new CounterPosition(nextPosition);
      logWriter.execute(logContents);
      nextPosition++;
      return position;
    }

    private static final Function<LogRecord, Entry> TO_ENTRY = new Function<LogRecord, Entry>() {
      @Override
      public Entry apply(final LogRecord record) {
        return new Entry() {
          @Override
          public byte[] contents() {
            return record.getContents();
          }
        };
      }
    };

    @Override
    public Iterator<Entry> readAll() throws InvalidPositionException, StreamAccessException {
      return FluentIterable.from(Ordering.natural().sortedCopy(logContents.getRecords().keySet()))
          .transform(Functions.forMap(logContents.getRecords()))
          .transform(TO_ENTRY)
          .iterator();
    }

    @Override
    public void truncateBefore(Position position)
        throws InvalidPositionException, StreamAccessException {

      if (!(position instanceof CounterPosition)) {
        throw new InvalidPositionException("Unrecognized position " + position);
      }

      final long truncateBefore = ((CounterPosition) position).value;
      Iterables.removeIf(logContents.getRecords().keySet(), new Predicate<Long>() {
        @Override
        public boolean apply(Long recordPosition) {
          return recordPosition < truncateBefore;
        }
      });
      logWriter.execute(logContents);
    }

    private static class CounterPosition implements Position {
      private final long value;

      CounterPosition(long value) {
        this.value = value;
      }

      @Override
      public int compareTo(Position position) {
        return Longs.compare(value, ((CounterPosition) position).value);
      }
    }
  }
}
