package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.Transaction;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Entry;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream;
import com.twitter.mesos.scheduler.log.Log.Stream.InvalidPositionException;
import com.twitter.mesos.scheduler.log.Log.Stream.StreamAccessException;

/**
 * Manages opening, reading from and writing to a {@link Log}.
 *
 * @author John Sirois
 */
final class LogManager {

  private static final Logger LOG = Logger.getLogger(LogManager.class.getName());

  private final Log log;
  private final ShutdownRegistry shutdownRegistry;

  @Inject
  LogManager(Log log, ShutdownRegistry shutdownRegistry) {
    this.log = Preconditions.checkNotNull(log);
    this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
  }

  StreamManager open() throws IOException {
    final Stream stream = log.open();
    shutdownRegistry.addAction(new ExceptionalCommand<IOException>() {
      @Override public void execute() throws IOException {
        stream.close();
      }
    });
    return new StreamManager(stream);
  }

  /**
   * Manages interaction with the log stream.  Log entries can be
   * {@link #readAfter(byte[], com.twitter.common.base.Closure) read after} a last known good
   * position, a {@link #startTransaction() transaction} consisting of one or more local storage
   * operations can be committed atomically, or the log can be compacted by
   * {@link #snapshot(com.twitter.mesos.gen.storage.Snapshot) snapshotting}.
   */
  static class StreamManager {

    private static class Vars {
      final AtomicInteger unSnapshottedTransactions =
          Stats.exportInt("scheduler_log_un_snapshotted_transactions");
      final AtomicLong logBytesWritten = Stats.exportLong("scheduler_log_bytes_written");
      final AtomicLong logEntriesWritten = Stats.exportLong("scheduler_log_entries_written");
      final AtomicLong logBytesRead = Stats.exportLong("scheduler_log_bytes_read");
      final AtomicLong logEntriesRead = Stats.exportLong("scheduler_log_entries_read");
      final AtomicLong logSnapshots = Stats.exportLong("scheduler_log_snapshots");
    }
    private final Vars vars = new Vars();

    private final Stream stream;

    StreamManager(Stream stream) {
      this.stream = Preconditions.checkNotNull(stream);
    }

    /**
     * Reads all entries in the log stream after the given position.  If the position
     * supplied is {@code null} then all log entries in the stream will be read.
     *
     * @param position The position immediately before the first position to read.
     * @param reader A reader that will be handed log entries decoded from the stream.
     * @return The position of the last LogEntry that was read.
     * @throws CodingException if there was a problem decoding a log entry from the stream.
     * @throws InvalidPositionException if the given position is not found in the log.
     * @throws StreamAccessException if there is a problem reading from the log.
     */
    @Nullable
    Position readAfter(@Nullable byte[] position, Closure<LogEntry> reader)
        throws CodingException, InvalidPositionException, StreamAccessException {

      Iterator<Entry> entries =
          stream.readFrom(position == null ? stream.beginning() : stream.position(position));

      if (position != null && entries.hasNext()) {
        // Discard the known position.
        entries.next();
      }

      Position last = null;
      while (entries.hasNext()) {
        Entry entry = entries.next();
        last = entry.position();
        byte[] contents = entry.contents();
        reader.execute(ThriftBinaryCodec.decodeNonNull(LogEntry.class, contents));
        vars.logBytesRead.addAndGet(contents.length);
        vars.logEntriesRead.incrementAndGet();
      }
      return last;
    }

    /**
     * Truncates all entries in the log stream occuring before the given position.  The entry at the
     * given position becomes the first entry in the stream when this call completes.
     *
     * @param position The last position to keep in the stream.
     * @throws InvalidPositionException if the specified position does not exist in this log.
     * @throws StreamAccessException if the stream could not be truncated.
     */
    void truncateBefore(Position position) {
      stream.truncateBefore(position);
    }

    /**
     * Starts a transaction that can be used to commit a series of {@link Op}s to the log stream
     * atomically.
     *
     * @return StreamTransaction A transaction manager to handle batching up commits to the
     *    underlying stream.
     */
    StreamTransaction startTransaction() {
      return new StreamTransaction();
    }

    /**
     * Adds a snapshot to the log and if successful, truncates the log entries preceding the
     * snapshot.
     *
     * @param snapshot The snapshot to add.
     * @return The position of the snapshot log entry.
     * @throws CodingException if the was a problem encoding the snapshot into a log entry.
     * @throws InvalidPositionException if there was a problem truncating before the snapshot.
     * @throws StreamAccessException if there was a problem appending the snapshot to the log.
     */
    @Timed("scheduler_log_snapshot")
    Position snapshot(Snapshot snapshot)
        throws CodingException, InvalidPositionException, StreamAccessException {

      Position position = append(LogEntry.snapshot(snapshot));
      vars.logSnapshots.incrementAndGet();
      vars.unSnapshottedTransactions.set(0);
      stream.truncateBefore(position);
      return position;
    }

    /**
     * Appends an entry to the log and returns the entry's position in the log if successful.
     *
     * @param logEntry The entry to append to the log.
     * @return The position of the appended log entry.
     * @throws CodingException if there was a problem encoding the log entry.
     * @throws StreamAccessException if there was a problem appending to the log.
     */
    @Timed("scheduler_log_append")
    Position append(LogEntry logEntry) throws CodingException, StreamAccessException {
      byte[] entry = ThriftBinaryCodec.encodeNonNull(logEntry);
      Position position = stream.append(entry);
      vars.logBytesWritten.addAndGet(entry.length);
      vars.logEntriesWritten.incrementAndGet();
      return position;
    }

    /**
     * Manages a single log stream append transaction.  Local storage ops can be added to the
     * transaction and then later committed as an atomic unit.
     */
    class StreamTransaction {
      private final Transaction transaction = new Transaction();
      private final AtomicBoolean committed = new AtomicBoolean(false);

      private StreamTransaction() {
        // supplied by factory method
      }

      /**
       * Appends any ops that have been added to this transaction to the log stream in a single
       * atomic record.
       *
       * @return The position of the log entry committed in this transaction, if any.
       * @throws CodingException If there was a problem encoding a log entry for commit.
       */
      Position commit() throws CodingException, StreamAccessException {
        Preconditions.checkState(!committed.getAndSet(true),
            "Can only call commit once per transaction.");

        if (!transaction.isSetOps()) {
          return null;
        }

        Position position = append(LogEntry.transaction(transaction));
        vars.unSnapshottedTransactions.incrementAndGet();
        return position;
      }

      /**
       * Adds a local storage operation to this transaction.
       *
       * @param op The local storage op to add.
       */
      void add(Op op) {
        Preconditions.checkState(!committed.get());

        Op prior = transaction.isSetOps() ? Iterables.getFirst(transaction.getOps(), null) : null;
        if (prior == null || !coalesce(prior, op)) {
          transaction.addToOps(op);
        }
      }

      /**
       * Tries to coalesce a new op into the prior to compact the binary representation and increase
       * batching.
       *
       * <p>Its recommended that as new {@code Op}s are added, they be treated here although they
       * need not be</p>
       *
       * @param prior The previous op.
       * @param next The next op to be added.
       * @return {@code true} if the next op was coalesced into the prior, {@code false} otherwise.
       */
      private boolean coalesce(Op prior, Op next) {
        if (!prior.isSet() && !next.isSet()) {
          return false;
        }

        Op._Fields priorType = prior.getSetField();
        if (!priorType.equals(next.getSetField())) {
          return false;
        }

        switch (priorType) {
          case SAVE_FRAMEWORK_ID:
            prior.setSaveFrameworkId(next.getSaveFrameworkId());
            return true;

          case SAVE_ACCEPTED_JOB:
          case SAVE_JOB_UPDATE:
          case REMOVE_JOB_UPDATE:
          case REMOVE_JOB:
          case SAVE_QUOTA:
          case REMOVE_QUOTA:
            return false;

          case SAVE_TASKS:
            coalesce(prior.getSaveTasks(), next.getSaveTasks());
            return true;
          case REMOVE_TASKS:
            coalesce(prior.getRemoveTasks(), next.getRemoveTasks());
            return true;

          default:
            LOG.warning("Unoptimized op: " + priorType);
            return false;
        }
      }

      private void coalesce(SaveTasks prior, SaveTasks next) {
        if (next.isSetTasks()) {
          if (prior.isSetTasks()) {
            prior.getTasks().addAll(next.getTasks());
          } else {
            prior.setTasks(next.getTasks());
          }
        }
      }

      private void coalesce(RemoveTasks prior, RemoveTasks next) {
        if (next.isSetTaskIds()) {
          if (prior.isSetTaskIds()) {
            prior.getTaskIds().addAll(next.getTaskIds());
          } else {
            prior.setTaskIds(next.getTaskIds());
          }
        }
      }
    }
  }
}
