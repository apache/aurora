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
import java.security.MessageDigest;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.Frame;
import org.apache.aurora.gen.storage.FrameChunk;
import org.apache.aurora.gen.storage.FrameHeader;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.gen.storage.storageConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.storage.log.SnapshotDeduplicator.SnapshotDeduplicatorImpl;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class LogManagerTest extends EasyMockTest {

  private static final Amount<Integer, Data> NO_FRAMES_EVER_SIZE =
      Amount.of(Integer.MAX_VALUE, Data.GB);

  private static final Function<LogEntry, byte[]> ENCODER = entry -> {
    try {
      return encode(entry);
    } catch (CodingException e) {
      throw new RuntimeException(e);
    }
  };

  private Stream stream;
  private Position position1;
  private Position position2;

  @Before
  public void setUp() {
    stream = createMock(Stream.class);
    position1 = createMock(Position.class);
    position2 = createMock(Position.class);
  }

  private StreamManager createNoMessagesStreamManager() {
    return createStreamManager(NO_FRAMES_EVER_SIZE);
  }

  private StreamManager createStreamManager(final Amount<Integer, Data> maxEntrySize) {
    return new StreamManagerImpl(
        stream,
        new EntrySerializer.EntrySerializerImpl(maxEntrySize, Hashing.md5()),
        Hashing.md5(),
        new SnapshotDeduplicatorImpl());
  }

  @Test
  public void testStreamManagerReadFromUnknownNone() throws CodingException {
    expect(stream.readAll()).andReturn(Iterators.emptyIterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() { });

    control.replay();

    createNoMessagesStreamManager().readFromBeginning(reader);
  }

  @Test
  public void testStreamManagerReadFromUnknownSome() throws CodingException {
    LogEntry transaction1 = createLogEntry(
        Op.removeJob(new RemoveJob(JobKeys.from("role", "env", "job").newBuilder())));
    Entry entry1 = createMock(Entry.class);
    expect(entry1.contents()).andReturn(encode(transaction1));
    expect(stream.readAll()).andReturn(Iterators.singletonIterator(entry1));

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() { });
    reader.execute(transaction1);

    control.replay();

    createNoMessagesStreamManager().readFromBeginning(reader);
  }

  @Test
  public void testStreamManagerTruncateBefore() {
    stream.truncateBefore(position2);

    control.replay();

    createNoMessagesStreamManager().truncateBefore(position2);
  }

  @Test
  public void testStreamManagerSuccessiveCommits() throws CodingException {
    control.replay();

    StreamManager streamManager = createNoMessagesStreamManager();
    StreamTransaction streamTransaction = streamManager.startTransaction();
    streamTransaction.commit();

    assertNotSame("Expected a new transaction to be started after a commit",
        streamTransaction, streamManager.startTransaction());
  }

  @Test
  public void testTransactionEmpty() throws CodingException {
    control.replay();

    Position position = createNoMessagesStreamManager().startTransaction().commit();
    assertNull(position);
  }

  @Test(expected = IllegalStateException.class)
  public void testTransactionDoubleCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = createNoMessagesStreamManager().startTransaction();
    streamTransaction.commit();
    streamTransaction.commit();
  }

  @Test(expected = IllegalStateException.class)
  public void testTransactionAddAfterCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = createNoMessagesStreamManager().startTransaction();
    streamTransaction.commit();
    streamTransaction.add(Op.saveFrameworkId(new SaveFrameworkId("don't allow this")));
  }

  private static class LogEntryMatcher implements IArgumentMatcher {
    private final LogEntry expected;

    LogEntryMatcher(LogEntry expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object argument) {
      if (!(argument instanceof byte[])) {
        return false;
      }

      try {
        return expected.equals(ThriftBinaryCodec.decode(LogEntry.class, (byte[]) argument));
      } catch (CodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void appendTo(StringBuffer buffer) {
      buffer.append(expected.toString());
    }
  }

  private static byte[] entryEq(LogEntry expected) {
    EasyMock.reportMatcher(new LogEntryMatcher(expected));
    return new byte[] {};
  }

  @Test
  public void testCoalesce() throws CodingException {
    SaveTasks saveTasks1 = createSaveTasks("1", "2");
    createSaveTasks("2");
    SaveTasks saveTasks2 = createSaveTasks("1", "3");
    SaveTasks saveTasks3 = createSaveTasks("4", "5");

    // saveTasks1 is unrepresented because both of its operations were trumped.
    // saveTasks3 is unrepresented because its operations were deleted.
    SaveTasks coalescedSaves = createSaveTasks("3", "2", "1");

    RemoveTasks removeTasks1 = createRemoveTasks("1", "2");
    RemoveTasks removeTasks2 = createRemoveTasks("3");
    RemoveTasks removeTasks3 = createRemoveTasks("4", "5");

    RemoveTasks coalescedRemoves =
        new RemoveTasks(ImmutableSet.copyOf(Iterables.concat(removeTasks2.getTaskIds(),
            removeTasks3.getTaskIds())));

    expectAppend(position1,
        createLogEntry(
            Op.saveTasks(coalescedSaves),
            Op.removeTasks(removeTasks1),
            Op.saveTasks(saveTasks3),
            Op.removeTasks(coalescedRemoves)));

    control.replay();

    StreamTransaction streamTransaction = createNoMessagesStreamManager().startTransaction();

    // The next 2 saves should coalesce
    streamTransaction.add(Op.saveTasks(saveTasks1));
    streamTransaction.add(Op.saveTasks(saveTasks2));

    streamTransaction.add(Op.removeTasks(removeTasks1));
    streamTransaction.add(Op.saveTasks(saveTasks3));

    // The next 2 removes should coalesce
    streamTransaction.add(Op.removeTasks(removeTasks2));
    streamTransaction.add(Op.removeTasks(removeTasks3));

    assertEquals(position1, streamTransaction.commit());
  }

  @Test
  public void testTransactionSnapshot() throws CodingException {
    Snapshot snapshot = createSnapshot();
    DeduplicatedSnapshot deduplicated = new SnapshotDeduplicatorImpl().deduplicate(snapshot);
    expectAppend(position1, Entries.deflate(LogEntry.deduplicatedSnapshot(deduplicated)));
    stream.truncateBefore(position1);

    control.replay();

    createNoMessagesStreamManager().snapshot(snapshot);
  }

  @Test
  public void testTransactionOps() throws CodingException {
    Op saveFrameworkId = Op.saveFrameworkId(new SaveFrameworkId("jake"));
    Op deleteJob = Op.removeJob(new RemoveJob(JobKeys.from("role", "env", "name").newBuilder()));
    expectTransaction(position1, saveFrameworkId, deleteJob);

    StreamManager streamManager = createNoMessagesStreamManager();
    control.replay();

    StreamTransaction transaction = streamManager.startTransaction();
    transaction.add(saveFrameworkId);
    transaction.add(deleteJob);

    Position position = transaction.commit();
    assertSame(position1, position);
  }

  static class Message {
    private final Amount<Integer, Data> chunkSize;
    private final LogEntry header;
    private final ImmutableList<LogEntry> chunks;

    Message(Amount<Integer, Data> chunkSize, Frame header, Iterable<Frame> chunks) {
      this.chunkSize = chunkSize;
      this.header = LogEntry.frame(header);
      this.chunks = ImmutableList.copyOf(Iterables.transform(chunks,
          LogEntry::frame));
    }
  }

  static Message frame(LogEntry logEntry) throws Exception {
    byte[] entry = encode(logEntry);

    double chunkBytes = entry.length / 2.0;
    Amount<Integer, Data> chunkSize = Amount.of((int) Math.floor(chunkBytes), Data.BYTES);
    int chunkLength = chunkSize.getValue();
    int chunkCount = (int) Math.ceil(entry.length / (double) chunkSize.getValue());

    Frame header = Frame.header(new FrameHeader(chunkCount,
        ByteBuffer.wrap(MessageDigest.getInstance("MD5").digest(entry))));

    List<Frame> chunks = Lists.newArrayList();
    for (int i = 0; i < chunkCount; i++) {
      int offset = i * chunkLength;
      ByteBuffer data =
          ByteBuffer.wrap(entry, offset, Math.min(chunkLength, entry.length - offset));
      chunks.add(Frame.chunk(new FrameChunk(data)));
    }

    return new Message(chunkSize, header, chunks);
  }

  @Test
  public void testTransactionFrames() throws Exception {
    Op saveFrameworkId = Op.saveFrameworkId(new SaveFrameworkId("jake"));

    Message message = frame(createLogEntry(saveFrameworkId));
    expectFrames(position1, message);

    StreamManager streamManager = createStreamManager(message.chunkSize);
    control.replay();

    StreamTransaction transaction = streamManager.startTransaction();
    transaction.add(saveFrameworkId);

    Position position = transaction.commit();
    assertSame(position1, position);
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    control.replay(); // No easymock expectations used here

    Op op1 = Op.removeJob(new RemoveJob(JobKeys.from("r1", "env", "name").newBuilder()));
    final Op op2 = Op.removeJob(new RemoveJob(JobKeys.from("r2", "env", "name").newBuilder()));

    LogEntry transaction1 = createLogEntry(op1);
    LogEntry transaction2 = createLogEntry(op2);

    final CountDownLatch message1Started = new CountDownLatch(1);

    Message message1 = frame(transaction1);
    Message message2 = frame(transaction2);

    List<byte[]> expectedAppends =
        ImmutableList.<byte[]>builder()
            .add(encode(message1.header))
            .addAll(Iterables.transform(message1.chunks, ENCODER))
            .add(encode(message2.header))
            .addAll(Iterables.transform(message2.chunks, ENCODER))
            .build();

    final Deque<byte[]> actualAppends = new LinkedBlockingDeque<>();

    Stream mockStream = new Stream() {
      @Override
      public Position append(byte[] contents) throws StreamAccessException {
        actualAppends.addLast(contents);
        message1Started.countDown();
        try {
          // If a chunked message is not properly serialized to the log, this sleep all but ensures
          // interleaved chunk writes and a test failure.
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override
      public Iterator<Entry> readAll() throws InvalidPositionException, StreamAccessException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void truncateBefore(Position position)
          throws InvalidPositionException, StreamAccessException {
        throw new UnsupportedOperationException();
      }
    };

    final StreamManagerImpl streamManager = new StreamManagerImpl(
        mockStream,
        new EntrySerializer.EntrySerializerImpl(message1.chunkSize, Hashing.md5()),
        Hashing.md5(),
        new SnapshotDeduplicatorImpl());
    StreamTransaction tr1 = streamManager.startTransaction();
    tr1.add(op1);

    Thread snapshotThread = new Thread() {
      @Override
      public void run() {
        StreamTransaction tr2 = streamManager.startTransaction();
        tr2.add(op2);
        try {
          message1Started.await();
          tr2.commit();
        } catch (CodingException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    snapshotThread.setDaemon(true);
    snapshotThread.start();

    tr1.commit();

    snapshotThread.join();

    assertEquals(expectedAppends.size(), actualAppends.size());
    for (byte[] expectedData : expectedAppends) {
      assertArrayEquals(expectedData, actualAppends.removeFirst());
    }
  }

  @Test
  public void testStreamManagerReadFrames() throws Exception {
    LogEntry transaction1 = createLogEntry(
        Op.removeJob(new RemoveJob(JobKeys.from("r1", "env", "name").newBuilder())));
    LogEntry transaction2 = createLogEntry(
        Op.removeJob(new RemoveJob(JobKeys.from("r2", "env", "name").newBuilder())));

    Message message = frame(transaction1);

    List<Entry> entries = Lists.newArrayList();

    // Should be read and skipped.
    Entry orphanChunkEntry = createMock(Entry.class);
    expect(orphanChunkEntry.contents()).andReturn(encode(message.chunks.get(0)));
    entries.add(orphanChunkEntry);

    // Should be read and skipped.
    Entry headerEntry = createMock(Entry.class);
    expect(headerEntry.contents()).andReturn(encode(message.header));
    entries.add(headerEntry);

    // We start a valid message, these frames should be read as 1 entry.
    expect(headerEntry.contents()).andReturn(encode(message.header));
    entries.add(headerEntry);
    for (LogEntry chunk : message.chunks) {
      Entry chunkEntry = createMock(Entry.class);
      expect(chunkEntry.contents()).andReturn(encode(chunk));
      entries.add(chunkEntry);
    }

    // Should be read and skipped.
    expect(orphanChunkEntry.contents()).andReturn(encode(message.chunks.get(0)));
    entries.add(orphanChunkEntry);

    // Should be read and skipped.
    expect(headerEntry.contents()).andReturn(encode(message.header));
    entries.add(headerEntry);

    // Should be read as 1 entry.
    Entry standardEntry = createMock(Entry.class);
    expect(standardEntry.contents()).andReturn(encode(transaction2));
    entries.add(standardEntry);

    expect(stream.readAll()).andReturn(entries.iterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() { });
    reader.execute(transaction1);
    reader.execute(transaction2);

    StreamManager streamManager = createStreamManager(message.chunkSize);
    control.replay();

    streamManager.readFromBeginning(reader);
  }

  @Test
  public void testWriteAndReadDeflatedEntry() throws Exception {
    Snapshot snapshot = createSnapshot();
    LogEntry snapshotLogEntry = LogEntry.snapshot(snapshot);
    LogEntry deflatedSnapshotEntry = Entries.deflate(
        LogEntry.deduplicatedSnapshot(new SnapshotDeduplicatorImpl().deduplicate(snapshot)));

    Entry snapshotEntry = createMock(Entry.class);
    expect(stream.append(entryEq(deflatedSnapshotEntry))).andReturn(position1);
    stream.truncateBefore(position1);

    expect(snapshotEntry.contents()).andReturn(encode(deflatedSnapshotEntry));

    expect(stream.readAll()).andReturn(ImmutableList.of(snapshotEntry).iterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() { });
    reader.execute(snapshotLogEntry);

    control.replay();

    HashFunction md5 = Hashing.md5();
    StreamManagerImpl streamManager = new StreamManagerImpl(
        stream,
        new EntrySerializer.EntrySerializerImpl(NO_FRAMES_EVER_SIZE, md5),
        md5,
        new SnapshotDeduplicatorImpl());
    streamManager.snapshot(snapshot);
    streamManager.readFromBeginning(reader);
  }

  private Snapshot createSnapshot() {
    return new Snapshot()
        .setTimestamp(1L)
        .setHostAttributes(ImmutableSet.of(new HostAttributes("host",
            ImmutableSet.of(new Attribute("hostname", ImmutableSet.of("abc"))))))
        .setTasks(ImmutableSet.of(
            new ScheduledTask().setStatus(ScheduleStatus.RUNNING)
                .setAssignedTask(new AssignedTask().setTaskId("task_id")
                    .setTask(new TaskConfig().setJobName("job_name")))));
  }

  private SaveTasks createSaveTasks(String... taskIds) {
    return new SaveTasks(ImmutableSet.copyOf(Iterables.transform(ImmutableList.copyOf(taskIds),
        taskId -> new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId(taskId)))));
  }

  private RemoveTasks createRemoveTasks(String... taskIds) {
    return new RemoveTasks(ImmutableSet.copyOf(taskIds));
  }

  private void expectFrames(Position position, Message message) throws CodingException {
    expect(stream.append(entryEq(message.header))).andReturn(position);
    for (LogEntry chunk : message.chunks) {
      // Only return a valid position for the header.
      expect(stream.append(entryEq(chunk))).andReturn(null);
    }
  }

  private void expectTransaction(Position position, Op... ops) throws CodingException {
    expectAppend(position, createLogEntry(ops));
  }

  private LogEntry createLogEntry(Op... ops) {
    return LogEntry.transaction(
        new Transaction(ImmutableList.copyOf(ops), storageConstants.CURRENT_SCHEMA_VERSION));
  }

  private void expectAppend(Position position, LogEntry logEntry) throws CodingException {
    expect(stream.append(entryEq(logEntry))).andReturn(position);
  }

  private static byte[] encode(LogEntry logEntry) throws CodingException {
    return ThriftBinaryCodec.encode(logEntry);
  }
}
