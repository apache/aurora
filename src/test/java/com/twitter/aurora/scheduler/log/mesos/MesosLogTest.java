package com.twitter.aurora.scheduler.log.mesos;

import java.util.concurrent.TimeoutException;

import javax.inject.Provider;

import com.google.inject.util.Providers;

import org.apache.mesos.Log;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.scheduler.log.Log.Stream.StreamAccessException;
import com.twitter.aurora.scheduler.log.mesos.LogInterface.ReaderInterface;
import com.twitter.aurora.scheduler.log.mesos.LogInterface.WriterInterface;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;

public class MesosLogTest extends EasyMockTest {

  private static final Amount<Long, Time> READ_TIMEOUT = Amount.of(5L, Time.SECONDS);
  private static final Amount<Long, Time> WRITE_TIMEOUT = Amount.of(3L, Time.SECONDS);
  private static final byte[] DUMMY_CONTENT = "test data".getBytes();

  private LogInterface logInterface;
  private ReaderInterface reader;
  private Provider<WriterInterface> writerFactory;
  private MesosLog.LogStream logStream;
  private MesosLog.LogStream.Mutation dummyMutation;
  private MesosLog.LogStream.OpStats stats;

  @Before
  public void setUp() {
    logInterface = createMock(LogInterface.class);
    reader = createMock(ReaderInterface.class);
    writerFactory = Providers.of(createMock(WriterInterface.class));

    dummyMutation = createMock(MesosLog.LogStream.Mutation.class);
    stats = new MesosLog.LogStream.OpStats("test");
    logStream = new MesosLog.LogStream(logInterface, reader, READ_TIMEOUT,
        writerFactory, WRITE_TIMEOUT, DUMMY_CONTENT);
  }

  @Test(expected = StreamAccessException.class)
  public void testLogStreamTimeout() throws TimeoutException, Log.WriterFailedException {
    testMutationFailure(new TimeoutException("Task timed out"));
  }

  @Test(expected = StreamAccessException.class)
  public void testLogStreamWriteFailure() throws TimeoutException, Log.WriterFailedException {
    testMutationFailure(new Log.WriterFailedException("Failed to write to log"));
  }

  private void testMutationFailure(Exception e) throws TimeoutException, Log.WriterFailedException {
    expect(dummyMutation.apply(EasyMock.<WriterInterface>anyObject())).andThrow(e);

    control.replay();
    logStream.mutate(stats, dummyMutation);
  }
}
