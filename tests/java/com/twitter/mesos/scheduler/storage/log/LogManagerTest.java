package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.base.Function;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
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

  private Stream stream;
  private Position position1;
  private Position position2;
  private Position position3;
  private StreamManager streamManager;

  @Before
  public void setUp() {
    stream = createMock(Stream.class);
    position1 = createMock(Position.class);
    position2 = createMock(Position.class);
    position3 = createMock(Position.class);

    streamManager = new StreamManager(stream);
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

    new LogManager(log, shutdownRegistry).open();

    assertTrue(shutdownAction.hasCaptured());
    shutdownAction.getValue().execute();
  }

  @Test
  public void testStreamManager_readFromUnknown_none() throws CodingException {
    expect(stream.beginning()).andReturn(position1);
    expect(stream.readFrom(position1)).andReturn(Iterators.<Log.Entry>emptyIterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});

    control.replay();

    streamManager.readAfter(null /* unknown */, reader);
  }

  @Test
  public void testStreamManager_readFromUnknown_some() throws CodingException {
    expect(stream.beginning()).andReturn(position1);

    LogEntry transaction1 = createLogEntry(Op.removeJob(new RemoveJob("job1")));
    Entry entry1 = createMock(Entry.class);
    expect(entry1.position()).andReturn(position2);
    expect(entry1.contents()).andReturn(ThriftBinaryCodec.encode(transaction1));
    expect(stream.readFrom(position1)).andReturn(Iterators.singletonIterator(entry1));

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});
    reader.execute(transaction1);

    control.replay();

    assertSame(position2, streamManager.readAfter(null /* unknown */, reader));
  }

  @Test
  public void testStreamManager_readFromKnown_none() throws CodingException {
    byte[] identity2 = identity("position2");
    expect(stream.position(identity2)).andReturn(position2);
    expect(stream.readFrom(position2)).andReturn(Iterators.<Log.Entry>emptyIterator());

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});

    control.replay();

    assertNull(streamManager.readAfter(identity2, reader));
  }

  @Test
  public void testStreamManager_readFromKnown_one() throws CodingException {
    byte[] identity2 = identity("position2");
    expect(stream.position(identity2)).andReturn(position2);

    Entry entry2 = createMock(Entry.class);
    expect(stream.readFrom(position2)).andReturn(Iterators.singletonIterator(entry2));

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});

    control.replay();

    assertNull(streamManager.readAfter(identity2, reader));
  }

  @Test
  public void testStreamManager_readFromKnown_some() throws CodingException {
    byte[] identity2 = identity("position2");
    expect(stream.position(identity2)).andReturn(position2);

    Entry entry2 = createMock(Entry.class);

    LogEntry snapshot3 = LogEntry.snapshot(createSnapshot("snapshot3"));
    Entry entry3 = createMock(Entry.class);
    expect(entry3.position()).andReturn(position3);
    expect(entry3.contents()).andReturn(ThriftBinaryCodec.encode(snapshot3));

    expect(stream.readFrom(position2)).andReturn(Iterators.forArray(entry2, entry3));

    Closure<LogEntry> reader = createMock(new Clazz<Closure<LogEntry>>() {});
    reader.execute(snapshot3);

    control.replay();

    assertSame(position3, streamManager.readAfter(identity2, reader));
  }

  @Test
  public void testStreamManager_truncateBefore() {
    stream.truncateBefore(position2);

    control.replay();

    streamManager.truncateBefore(position2);
  }

  @Test
  public void testStreamManager_successiveCommits() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = streamManager.startTransaction();
    streamTransaction.commit();

    assertNotSame("Expected a new transaction to be started after a commit",
        streamTransaction, streamManager.startTransaction());
  }

  @Test
  public void testTransaction_empty() throws CodingException {
    control.replay();

    Position position = streamManager.startTransaction().commit();
    assertNull(position);
  }

  @Test(expected = IllegalStateException.class)
  public void testTransaction_doubleCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = streamManager.startTransaction();
    streamTransaction.commit();
    streamTransaction.commit();
  }

  @Test(expected = IllegalStateException.class)
  public void testTransaction_addAfterCommit() throws CodingException {
    control.replay();

    StreamTransaction streamTransaction = streamManager.startTransaction();
    streamTransaction.commit();
    streamTransaction.add(Op.saveFrameworkId(new SaveFrameworkId("don't allow this")));
  }

  @Test
  public void testCoalesce() throws CodingException {
    SaveTasks saveTasks1 = createSaveTasks("1", "2");
    SaveTasks nonTrumpedSaveTasks1 = createSaveTasks("2");
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

    StreamTransaction streamTransaction = streamManager.startTransaction();

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

    Position position = streamManager.snapshot(snapshot);

    assertSame(position1, position);
  }

  @Test
  public void testTransaction_ops() throws CodingException {
    Op saveFrameworkId = Op.saveFrameworkId(new SaveFrameworkId("jake"));
    Op deleteJob = Op.removeJob(new RemoveJob("jane"));
    expectTransaction(position1, saveFrameworkId, deleteJob);

    control.replay();

    StreamTransaction transaction = streamManager.startTransaction();
    transaction.add(saveFrameworkId);
    transaction.add(deleteJob);

    Position position = transaction.commit();
    assertSame(position1, position);
  }

  private byte[] identity(String name) {
    return name.getBytes();
  }

  private Snapshot createSnapshot(String snapshotData) {
    return new Snapshot(1L, ByteBuffer.wrap(snapshotData.getBytes()));
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

  private void expectTransaction(Position position, Op... ops) throws CodingException {
    expectAppend(position, createLogEntry(ops));
  }

  private LogEntry createLogEntry(Op... ops) {
    return LogEntry.transaction(new Transaction(ImmutableList.copyOf(ops)));
  }

  private void expectAppend(Position position, LogEntry logEntry) throws CodingException {
    expect(stream.append(aryEq(ThriftBinaryCodec.encode(logEntry)))).andReturn(position);
  }
}
