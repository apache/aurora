package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.Frame;
import com.twitter.mesos.gen.storage.FrameChunk;
import com.twitter.mesos.gen.storage.FrameHeader;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveJob;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.Transaction;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Entry;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager.StreamTransaction;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class LogManagerTest extends EasyMockTest {

  private static final Amount<Integer, Data> NO_FRAMES_EVER_SIZE =
      Amount.of(Integer.MAX_VALUE, Data.GB);

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

  private StreamManager createStreamManager(Amount<Integer, Data> maxEntrySize) {
    return new StreamManager(stream, maxEntrySize);
  }

  @Test
  public void testLogManager() throws IOException, CodingException {
    Log log = createMock(Log.class);
    expect(log.open()).andReturn(stream);

    ShutdownRegistry shutdownRegistry = createMock(ShutdownRegistry.class);
    Capture<ExceptionalCommand<IOException>> shutdownAction =
        new Capture<ExceptionalCommand<IOException>>();
    shutdownRegistry.addAction(capture(shutdownAction));

    // The registered shutdown command should close the stream
    stream.close();

    control.replay();

    new LogManager(log, NO_FRAMES_EVER_SIZE, shutdownRegistry).open();

    assertTrue(shutdownAction.hasCaptured());
    shutdownAction.getValue().execute();
  }

  @Test
  public void testStreamManager_readFromUnknown_none() throws CodingException {
    expect(stream.readAll()).andReturn(Iterators.<Log.Entry>emptyIterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});

    control.replay();

    createNoMessagesStreamManager().readFromBeginning(reader);
  }

  @Test
  public void testStreamManager_readFromUnknown_some() throws CodingException {
    LogEntry transaction1 = createLogEntry(Op.removeJob(new RemoveJob("job1")));
    Entry entry1 = createMock(Entry.class);
    expect(entry1.contents()).andReturn(encode(transaction1));
    expect(stream.readAll()).andReturn(Iterators.singletonIterator(entry1));

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});
    reader.execute(transaction1);

    control.replay();

    createNoMessagesStreamManager().readFromBeginning(reader);
  }

  @Test
  public void testStreamManager_truncateBefore() {
    stream.truncateBefore(position2);

    control.replay();

    createNoMessagesStreamManager().truncateBefore(position2);
  }

  @Test
  public void testStreamManager_successiveCommits() throws CodingException {
    control.replay();

    StreamManager streamManager = createNoMessagesStreamManager();
    StreamTransaction streamTransaction = streamManager.startTransaction();
    streamTransaction.commit();

    assertNotSame("Expected a new transaction to be started after a commit",
        streamTransaction, streamManager.startTransaction());
  }

  @Test
  public void testTransaction_empty() throws CodingException {
    control.replay();

    Position position = createNoMessagesStreamManager().startTransaction().commit();
    assertNull(position);
  }

  @Test(expected = IllegalStateException.class)
  public void testTransaction_doubleCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = createNoMessagesStreamManager().startTransaction();
    streamTransaction.commit();
    streamTransaction.commit();
  }

  @Test(expected = IllegalStateException.class)
  public void testTransaction_addAfterCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = createNoMessagesStreamManager().startTransaction();
    streamTransaction.commit();
    streamTransaction.add(Op.saveFrameworkId(new SaveFrameworkId("don't allow this")));
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
  public void testTransaction_snapshot() throws CodingException {
    Snapshot snapshot = createSnapshot("snapshot-data");
    expectAppend(position1, LogEntry.snapshot(snapshot));
    stream.truncateBefore(position1);

    control.replay();

    createNoMessagesStreamManager().snapshot(snapshot);
  }

  @Test
  public void testTransaction_ops() throws CodingException {
    Op saveFrameworkId = Op.saveFrameworkId(new SaveFrameworkId("jake"));
    Op deleteJob = Op.removeJob(new RemoveJob("jane"));
    expectTransaction(position1, saveFrameworkId, deleteJob);

    control.replay();

    StreamTransaction transaction = createNoMessagesStreamManager().startTransaction();
    transaction.add(saveFrameworkId);
    transaction.add(deleteJob);

    Position position = transaction.commit();
    assertSame(position1, position);
  }

  static class Message {
    final Amount<Integer, Data> chunkSize;
    final LogEntry header;
    final ImmutableList<LogEntry> chunks;

    public Message(Amount<Integer, Data> chunkSize, Frame header, Iterable<Frame> chunks) {
      this.chunkSize = chunkSize;
      this.header = LogEntry.frame(header);
      this.chunks = ImmutableList.copyOf(Iterables.transform(chunks,
          new Function<Frame, LogEntry>() {
            @Override public LogEntry apply(Frame frame) {
              return LogEntry.frame(frame);
            }
          }));
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
  public void testTransaction_frames() throws Exception {
    Op saveFrameworkId = Op.saveFrameworkId(new SaveFrameworkId("jake"));

    Message message = frame(createLogEntry(saveFrameworkId));
    expectFrames(position1, message);

    control.replay();

    StreamManager streamManager = createStreamManager(message.chunkSize);
    StreamTransaction transaction = streamManager.startTransaction();
    transaction.add(saveFrameworkId);

    Position position = transaction.commit();
    assertSame(position1, position);
  }

  @Test
  public void testStreamManager_readFrames() throws Exception {
    LogEntry transaction1 = createLogEntry(Op.removeJob(new RemoveJob("job1")));
    LogEntry transaction2 = createLogEntry(Op.removeJob(new RemoveJob("job2")));

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

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});
    reader.execute(transaction1);
    reader.execute(transaction2);

    control.replay();

    createStreamManager(message.chunkSize).readFromBeginning(reader);
  }

  private Snapshot createSnapshot(String snapshotData) {
    return new Snapshot()
        .setTimestamp(1L)
        .setDataDEPRECATED(ByteBuffer.wrap(snapshotData.getBytes()));
  }

  private SaveTasks createSaveTasks(String... taskIds) {
    return new SaveTasks(ImmutableSet.copyOf(Iterables.transform(ImmutableList.copyOf(taskIds),
        new Function<String, ScheduledTask>() {
          @Override
          public ScheduledTask apply(String taskId) {
            return new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId(taskId));
          }
        })));
  }

  private RemoveTasks createRemoveTasks(String... taskIds) {
    return new RemoveTasks(ImmutableSet.copyOf(taskIds));
  }

  private void expectFrames(Position position, Message message) throws CodingException {
    expect(stream.append(aryEq(encode(message.header)))).andReturn(position);
    for (LogEntry chunk : message.chunks) {
      // Only return a valid position for the header.
      expect(stream.append(aryEq(encode(chunk)))).andReturn(null);
    }
  }

  private void expectTransaction(Position position, Op... ops) throws CodingException {
    expectAppend(position, createLogEntry(ops));
  }

  private LogEntry createLogEntry(Op... ops) {
    return LogEntry.transaction(new Transaction(ImmutableList.copyOf(ops)));
  }

  private void expectAppend(Position position, LogEntry logEntry) throws CodingException {
    expect(stream.append(aryEq(encode(logEntry)))).andReturn(position);
  }

  private static byte[] encode(LogEntry logEntry) throws CodingException {
    return ThriftBinaryCodec.encode(logEntry);
  }
}
