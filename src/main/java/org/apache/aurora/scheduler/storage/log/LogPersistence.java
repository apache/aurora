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

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Persistence layer that uses a replicated log.
 */
class LogPersistence implements Persistence {

  private static final Logger LOG = LoggerFactory.getLogger(LogPersistence.class);

  private final LogManager logManager;
  private final Snapshotter snapshotter;
  private StreamManager streamManager;

  @Inject
  LogPersistence(LogManager logManager, Snapshotter snapshotter) {
    this.logManager = requireNonNull(logManager);
    this.snapshotter = requireNonNull(snapshotter);
  }

  @Override
  public void prepare() {
    // Open the log to make a log replica available to the scheduler group.
    try {
      streamManager = logManager.open();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open the log, cannot continue", e);
    }
  }

  /**
   * Saves a snapshot to the log stream.
   *
   * @param snapshot Snapshot to save.
   */
  void persist(Snapshot snapshot) {
    streamManager.snapshot(snapshot);
  }

  @Override
  public void persist(Stream<Op> mutations) throws PersistenceException {
    try {
      streamManager.commit(mutations.collect(Collectors.toList()));
    } catch (CodingException e) {
      throw new PersistenceException(e);
    }
  }

  @Override
  public Stream<Edit> recover() throws PersistenceException {
    try {
      Iterator<LogEntry> entries = streamManager.readFromBeginning();
      Iterable<LogEntry> iterableEntries = () -> entries;
      Stream<LogEntry> entryStream = StreamSupport.stream(iterableEntries.spliterator(), false);

      return entryStream
          .filter(entry -> entry.getSetField() != LogEntry._Fields.NOOP)
          .flatMap(entry -> {
            switch (entry.getSetField()) {
              case SNAPSHOT:
                Snapshot snapshot = entry.getSnapshot();
                LOG.info("Applying snapshot taken on " + new Date(snapshot.getTimestamp()));
                return Stream.concat(
                    Stream.of(Edit.deleteAll()),
                    snapshotter.asStream(snapshot)
                        .map(Edit::op));

              case TRANSACTION:
                return entry.getTransaction().getOps().stream()
                    .map(Edit::op);

              default:
                throw new IllegalStateException("Unknown log entry type: " + entry.getSetField());
            }
          });
    } catch (CodingException | InvalidPositionException | StreamAccessException e) {
      throw new PersistenceException(e);
    }
  }
}
