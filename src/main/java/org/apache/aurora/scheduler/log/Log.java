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
package org.apache.aurora.scheduler.log;

import java.io.IOException;
import java.util.Iterator;

/**
 * Represents an append only log that can be read after and truncated before a known
 * {@link Position}.
 *
 * <p>Logs are accessed by {@link #open() opening} a {@link Stream}.  All stream
 * access occurs with references to log entry {@link Position positions} in the stream.
 */
public interface Log {

  /**
   * An opaque ordered handle to a log entry's position in the log stream.
   */
  interface Position {
  }

  /**
   * Represents a single entry in the log stream.
   */
  interface Entry {

    /**
     * Gets the contents of the log entry.
     *
     * @return the data stored in this log entry
     */
    byte[] contents();
  }

  /**
   * An interface to the live {@link Log} stream that allows for appending, reading and writing
   * entries.
   */
  interface Stream {

    /**
     * Indicates a {@link Position} that is not (currently) contained in this log stream.
     * This might indicate the position id from a different log or that the position was from this
     * log but has been truncated.
     */
    class InvalidPositionException extends RuntimeException {
      public InvalidPositionException(String message) {
        super(message);
      }
      public InvalidPositionException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    /**
     * Indicates a {@link Stream} could not be read from, written to or truncated due to some
     * underlying IO error.
     */
    class StreamAccessException extends RuntimeException {
      public StreamAccessException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    /**
     * Appends an {@link Entry} to the end of the log stream.
     *
     * @param contents the data to store in the appended entry
     * @return the posiiton of the appended entry
     * @throws StreamAccessException if contents could not be appended to the stream
     */
    Position append(byte[] contents) throws StreamAccessException;

    /**
     * Allows reading all entries from the log stream.  Implementations may
     * materialize all entries at once or the may provide some form of streaming to back the
     * returned entry iterator.  If the implementation does use some form of streaming or batching,
     * it may throw a
     * {@code StreamAccessException} on any call to {@link Iterator#hasNext()} or
     * {@link Iterator#next()}.
     *
     * @return an iterator that ranges from the entry from the given {@code position} to the last
     *     entry in the log.
     * @throws InvalidPositionException if the specified position does not exist in this log
     * @throws StreamAccessException if the stream could not be read from
     */
    Iterator<Entry> readAll() throws InvalidPositionException, StreamAccessException;

    /**
     * Removes all log entries preceding the log entry at the given {@code position}.
     *
     * @param position the position of the latest entry to remove
     * @throws InvalidPositionException if the specified position does not exist in this log
     * @throws StreamAccessException if the stream could not be truncated
     */
    void truncateBefore(Position position) throws InvalidPositionException, StreamAccessException;
  }

  /**
   * Opens the log stream for reading writing and truncation.  Clients should ensure the stream is
   * closed when they are done using it.
   *
   * @return the log stream
   * @throws IOException if there was a problem opening the log stream
   */
  Stream open() throws IOException;
}
