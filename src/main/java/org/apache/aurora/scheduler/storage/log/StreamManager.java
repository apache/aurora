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

import java.util.Iterator;

import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.log.Log;

import static org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import static org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import static org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;

/**
 * Manages interaction with the log stream.  Log entries can be
 * {@link #readFromBeginning() read from} the beginning,
 * a {@link #startTransaction() transaction} consisting of one or more local storage
 * operations can be committed atomically, or the log can be compacted by
 * {@link #snapshot(org.apache.aurora.gen.storage.Snapshot) snapshotting}.
 */
public interface StreamManager {
  /**
   * Reads all entries in the log stream.
   *
   * @return All stored log entries.
   * @throws CodingException if there was a problem decoding a log entry from the stream.
   * @throws InvalidPositionException if the given position is not found in the log.
   * @throws StreamAccessException if there is a problem reading from the log.
   */
  Iterator<LogEntry> readFromBeginning() throws CodingException, StreamAccessException;

  /**
   * Truncates all entries in the log stream occuring before the given position.  The entry at the
   * given position becomes the first entry in the stream when this call completes.
   *
   * @param position The last position to keep in the stream.
   * @throws InvalidPositionException if the specified position does not exist in this log.
   * @throws StreamAccessException if the stream could not be truncated.
   */
  void truncateBefore(Log.Position position);

  /**
   * Starts a transaction that can be used to commit a series of ops to the log stream atomically.
   *
   * @return StreamTransaction A transaction manager to handle batching up commits to the
   *    underlying stream.
   */
  StreamTransaction startTransaction();

  /**
   * Adds a snapshot to the log and if successful, truncates the log entries preceding the
   * snapshot.
   *
   * @param snapshot The snapshot to add.
   * @throws CodingException if the was a problem encoding the snapshot into a log entry.
   * @throws InvalidPositionException if there was a problem truncating before the snapshot.
   * @throws StreamAccessException if there was a problem appending the snapshot to the log.
   */
  void snapshot(Snapshot snapshot)
      throws CodingException, InvalidPositionException, StreamAccessException;
}
