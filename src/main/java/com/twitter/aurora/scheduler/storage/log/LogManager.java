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
package com.twitter.aurora.scheduler.storage.log;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.google.inject.BindingAnnotation;

import com.twitter.aurora.codec.ThriftBinaryCodec.CodingException;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.storage.Constants;
import com.twitter.aurora.gen.storage.Frame;
import com.twitter.aurora.gen.storage.FrameChunk;
import com.twitter.aurora.gen.storage.FrameHeader;
import com.twitter.aurora.gen.storage.LogEntry;
import com.twitter.aurora.gen.storage.LogEntry._Fields;
import com.twitter.aurora.gen.storage.Op;
import com.twitter.aurora.gen.storage.RemoveTasks;
import com.twitter.aurora.gen.storage.SaveHostAttributes;
import com.twitter.aurora.gen.storage.SaveTasks;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.Transaction;
import com.twitter.aurora.scheduler.log.Log;
import com.twitter.aurora.scheduler.log.Log.Entry;
import com.twitter.aurora.scheduler.log.Log.Position;
import com.twitter.aurora.scheduler.log.Log.Stream;
import com.twitter.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import com.twitter.aurora.scheduler.log.Log.Stream.StreamAccessException;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.stats.Stats;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages opening, reading from and writing to a {@link Log}.
 */
public final class LogManager {

  /**
   * Identifies the maximum log entry size to permit before chunking entries into frames.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface MaxEntrySize { }

  /**
   * Binding annotation for settings regarding the way snapshots are written.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface SnapshotSetting { }

  private static final Logger LOG = Logger.getLogger(LogManager.class.getName());

  private final Log log;
  private final Amount<Integer, Data> maxEntrySize;
  private final boolean deflateSnapshots;
  private final ShutdownRegistry shutdownRegistry;

  @Inject
  LogManager(
      Log log,
      @MaxEntrySize Amount<Integer, Data> maxEntrySize,
      @SnapshotSetting boolean deflateSnapshots,
      ShutdownRegistry shutdownRegistry) {

    this.log = checkNotNull(log);
    this.maxEntrySize = checkNotNull(maxEntrySize);
    this.deflateSnapshots = deflateSnapshots;
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
  }

  /**
   * Opens the log for reading and writing.
   *
   * @return A stream manager that can be used to manipulate the log stream.
   * @throws IOException If there is a problem opening the log.
   */
  public StreamManager open() throws IOException {
    final Stream stream = log.open();
    shutdownRegistry.addAction(new ExceptionalCommand<IOException>() {
      @Override public void execute() throws IOException {
        stream.close();
      }
    });
    return new StreamManager(stream, deflateSnapshots, maxEntrySize);
  }

  /**
   * Manages interaction with the log stream.  Log entries can be
   * {@link #readFromBeginning(com.twitter.common.base.Closure) read from} the beginning,
   * a {@link #startTransaction() transaction} consisting of one or more local storage
   * operations can be committed atomically, or the log can be compacted by
   * {@link #snapshot(com.twitter.aurora.gen.storage.Snapshot) snapshotting}.
   */
  public static class StreamManager {

    private static MessageDigest createDigest() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("Could not find provider for standard algorithm 'MD5'", e);
      }
    }

    private static class Vars {
      private final AtomicInteger unSnapshottedTransactions =
          Stats.exportInt("scheduler_log_un_snapshotted_transactions");
      private final AtomicLong bytesWritten = Stats.exportLong("scheduler_log_bytes_written");
      private final AtomicLong entriesWritten = Stats.exportLong("scheduler_log_entries_written");
      private final AtomicLong badFramesRead = Stats.exportLong("scheduler_log_bad_frames_read");
      private final AtomicLong bytesRead = Stats.exportLong("scheduler_log_bytes_read");
      private final AtomicLong entriesRead = Stats.exportLong("scheduler_log_entries_read");
      private final AtomicLong deflatedEntriesRead =
          Stats.exportLong("scheduler_log_deflated_entries_read");
      private final AtomicLong snapshots = Stats.exportLong("scheduler_log_snapshots");
    }
    private final Vars vars = new Vars();

    private final Object writeMutex = new Object();
    private final Stream stream;
    private final boolean deflateSnapshots;
    private final MessageDigest digest;
    private final EntrySerializer entrySerializer;

    StreamManager(Stream stream, boolean deflateSnapshots, Amount<Integer, Data> maxEntrySize) {
      this.stream = checkNotNull(stream);
      this.deflateSnapshots = deflateSnapshots;
      digest = createDigest();
      entrySerializer = new EntrySerializer(digest, maxEntrySize);
    }

    /**
     * Reads all entries in the log stream after the given position.  If the position
     * supplied is {@code null} then all log entries in the stream will be read.
     *
     * @param reader A reader that will be handed log entries decoded from the stream.
     * @throws CodingException if there was a problem decoding a log entry from the stream.
     * @throws InvalidPositionException if the given position is not found in the log.
     * @throws StreamAccessException if there is a problem reading from the log.
     */
    public void readFromBeginning(Closure<LogEntry> reader)
        throws CodingException, InvalidPositionException, StreamAccessException {

      Iterator<Entry> entries = stream.readAll();

      while (entries.hasNext()) {
        LogEntry logEntry = decodeLogEntry(entries.next());
        while (logEntry != null && isFrame(logEntry)) {
          logEntry = tryDecodeFrame(logEntry.getFrame(), entries);
        }
        if (logEntry != null) {
          if (logEntry.isSet(_Fields.DEFLATED_ENTRY)) {
            logEntry = Entries.inflate(logEntry);
            vars.deflatedEntriesRead.incrementAndGet();
          }

          reader.execute(logEntry);
          vars.entriesRead.incrementAndGet();
        }
      }
    }

    @Nullable
    private LogEntry tryDecodeFrame(Frame frame, Iterator<Entry> entries) throws CodingException {
      if (!isHeader(frame)) {
        LOG.warning("Found a frame with no preceding header, skipping.");
        return null;
      }
      FrameHeader header = frame.getHeader();
      byte[][] chunks = new byte[header.chunkCount][];

      digest.reset();
      for (int i = 0; i < header.chunkCount; i++) {
        if (!entries.hasNext()) {
          logBadFrame(header, i);
          return null;
        }
        LogEntry logEntry = decodeLogEntry(entries.next());
        if (!isFrame(logEntry)) {
          logBadFrame(header, i);
          return logEntry;
        }
        Frame chunkFrame = logEntry.getFrame();
        if (!isChunk(chunkFrame)) {
          logBadFrame(header, i);
          return logEntry;
        }
        byte[] chunkData = chunkFrame.getChunk().getData();
        digest.update(chunkData);
        chunks[i] = chunkData;
      }
      if (!Arrays.equals(header.getChecksum(), digest.digest())) {
        throw new CodingException("Read back a framed log entry that failed its checksum");
      }
      return Entries.thriftBinaryDecode(Bytes.concat(chunks));
    }

    private static boolean isFrame(LogEntry logEntry) {
      return logEntry.getSetField() == LogEntry._Fields.FRAME;
    }

    private static boolean isChunk(Frame frame) {
      return frame.getSetField() == Frame._Fields.CHUNK;
    }

    private static boolean isHeader(Frame frame) {
      return frame.getSetField() == Frame._Fields.HEADER;
    }

    private void logBadFrame(FrameHeader header, int chunkIndex) {
      LOG.info(String.format("Found an aborted transaction, required %d frames and found %d",
          header.chunkCount, chunkIndex));
      vars.badFramesRead.incrementAndGet();
    }

    private LogEntry decodeLogEntry(Entry entry) throws CodingException {
      byte[] contents = entry.contents();
      vars.bytesRead.addAndGet(contents.length);
      return Entries.thriftBinaryDecode(contents);
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
     * @throws CodingException if the was a problem encoding the snapshot into a log entry.
     * @throws InvalidPositionException if there was a problem truncating before the snapshot.
     * @throws StreamAccessException if there was a problem appending the snapshot to the log.
     */
    @Timed("log_manager_snapshot")
    void snapshot(Snapshot snapshot)
        throws CodingException, InvalidPositionException, StreamAccessException {

      LogEntry entry = LogEntry.snapshot(snapshot);
      if (deflateSnapshots) {
        entry = Entries.deflate(entry);
      }

      Position position = appendAndGetPosition(entry);
      vars.snapshots.incrementAndGet();
      vars.unSnapshottedTransactions.set(0);
      stream.truncateBefore(position);
    }

    @Timed("log_manager_append")
    private Position appendAndGetPosition(LogEntry logEntry) throws CodingException {
      Position firstPosition = null;
      byte[][] entries = entrySerializer.serialize(logEntry);
      synchronized (writeMutex) { // ensure all sub-entries are written as a unit
        for (byte[] entry : entries) {
          Position position = stream.append(entry);
          if (firstPosition == null) {
            firstPosition = position;
          }
          vars.bytesWritten.addAndGet(entry.length);
        }
      }
      vars.entriesWritten.incrementAndGet();
      return firstPosition;
    }

    @VisibleForTesting
    public static class EntrySerializer {
      private final MessageDigest digest;
      private final int maxEntrySizeBytes;

      private EntrySerializer(MessageDigest digest, Amount<Integer, Data> maxEntrySize) {
        this.digest = checkNotNull(digest);
        maxEntrySizeBytes = maxEntrySize.as(Data.BYTES);
      }

      public EntrySerializer(Amount<Integer, Data> maxEntrySize) {
        this(createDigest(), maxEntrySize);
      }

      /**
       * Serializes a log entry and splits it into chunks no larger than {@code maxEntrySizeBytes}.
       *
       * @param logEntry The log entry to serialize.
       * @return Serialized and chunked log entry.
       * @throws CodingException If the entry could not be serialized.
       */
      @VisibleForTesting
      public byte[][] serialize(LogEntry logEntry) throws CodingException {
        byte[] entry = Entries.thriftBinaryEncode(logEntry);
        if (entry.length <= maxEntrySizeBytes) {
          return new byte[][] {entry};
        }

        int chunks = (int) Math.ceil(entry.length / (double) maxEntrySizeBytes);
        byte[][] frames = new byte[chunks + 1][];

        frames[0] = encode(Frame.header(new FrameHeader(chunks, ByteBuffer.wrap(checksum(entry)))));
        for (int i = 0; i < chunks; i++) {
          int offset = i * maxEntrySizeBytes;
          ByteBuffer chunk =
              ByteBuffer.wrap(entry, offset, Math.min(maxEntrySizeBytes, entry.length - offset));
          frames[i + 1] = encode(Frame.chunk(new FrameChunk(chunk)));
        }
        return frames;
      }

      private byte[] checksum(byte[] data) {
        digest.reset();
        return digest.digest(data);
      }

      private static byte[] encode(Frame frame) throws CodingException {
        return Entries.thriftBinaryEncode(LogEntry.frame(frame));
      }
    }

    /**
     * Manages a single log stream append transaction.  Local storage ops can be added to the
     * transaction and then later committed as an atomic unit.
     */
    final class StreamTransaction {
      private final Transaction transaction =
          new Transaction().setSchemaVersion(Constants.CURRENT_SCHEMA_VERSION);
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
      Position commit() throws CodingException {
        Preconditions.checkState(!committed.getAndSet(true),
            "Can only call commit once per transaction.");

        if (!transaction.isSetOps()) {
          return null;
        }

        Position position = appendAndGetPosition(LogEntry.transaction(transaction));
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

        Op prior = transaction.isSetOps() ? Iterables.getLast(transaction.getOps(), null) : null;
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
          case SAVE_HOST_ATTRIBUTES:
            return coalesce(prior.getSaveHostAttributes(), next.getSaveHostAttributes());
          default:
            LOG.warning("Unoptimized op: " + priorType);
            return false;
        }
      }

      private void coalesce(SaveTasks prior, SaveTasks next) {
        if (next.isSetTasks()) {
          if (prior.isSetTasks()) {
            // It is an expected invariant that an operation may reference a task (identified by
            // task ID) no more than one time.  Therefore, to coalesce two SaveTasks operations,
            // the most recent task definition overrides the prior operation.
            Map<String, ScheduledTask> coalesced = Maps.newHashMap();
            for (ScheduledTask task : prior.getTasks()) {
              coalesced.put(task.getAssignedTask().getTaskId(), task);
            }
            for (ScheduledTask task : next.getTasks()) {
              coalesced.put(task.getAssignedTask().getTaskId(), task);
            }
            prior.setTasks(ImmutableSet.copyOf(coalesced.values()));
          } else {
            prior.setTasks(next.getTasks());
          }
        }
      }

      private void coalesce(RemoveTasks prior, RemoveTasks next) {
        if (next.isSetTaskIds()) {
          if (prior.isSetTaskIds()) {
            prior.setTaskIds(ImmutableSet.<String>builder()
                .addAll(prior.getTaskIds())
                .addAll(next.getTaskIds())
                .build());
          } else {
            prior.setTaskIds(next.getTaskIds());
          }
        }
      }

      private boolean coalesce(SaveHostAttributes prior, SaveHostAttributes next) {
        if (prior.getHostAttributes().getHost().equals(next.getHostAttributes().getHost())) {
          prior.getHostAttributes().setAttributes(next.getHostAttributes().getAttributes());
          return true;
        }
        return false;
      }
    }
  }
}
