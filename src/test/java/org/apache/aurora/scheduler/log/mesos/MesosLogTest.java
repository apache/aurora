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
package org.apache.aurora.scheduler.log.mesos;

import java.util.concurrent.TimeoutException;

import com.google.inject.util.Providers;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.log.mesos.LogInterface.ReaderInterface;
import org.apache.aurora.scheduler.log.mesos.LogInterface.WriterInterface;
import org.apache.mesos.Log;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.fail;

public class MesosLogTest extends EasyMockTest {

  private static final Amount<Long, Time> READ_TIMEOUT = Amount.of(5L, Time.SECONDS);
  private static final Amount<Long, Time> WRITE_TIMEOUT = Amount.of(3L, Time.SECONDS);
  private static final byte[] DUMMY_CONTENT = "test data".getBytes();

  private Command shutdownHooks;
  private MesosLog.LogStream logStream;
  private MesosLog.LogStream.Mutation<String> dummyMutation;
  private MesosLog.LogStream.OpStats stats;

  @Before
  public void setUp() {
    shutdownHooks = createMock(Command.class);
    dummyMutation = createMock(new Clazz<MesosLog.LogStream.Mutation<String>>() { });
    stats = new MesosLog.LogStream.OpStats("test");
    logStream = new MesosLog.LogStream(
        createMock(LogInterface.class),
        createMock(ReaderInterface.class),
        READ_TIMEOUT,
        Providers.of(createMock(WriterInterface.class)),
        WRITE_TIMEOUT,
        DUMMY_CONTENT,
        new Lifecycle(shutdownHooks, null));
  }

  @Test
  public void testLogStreamTimeout() throws TimeoutException, Log.WriterFailedException {
    try {
      testMutationFailure(new TimeoutException("Task timed out"));
      fail();
    } catch (StreamAccessException e) {
      // Expected.
    }

    expectStreamUnusable();
  }

  @Test
  public void testLogStreamWriteFailure() throws TimeoutException, Log.WriterFailedException {
    try {
      testMutationFailure(new Log.WriterFailedException("Failed to write to log"));
      fail();
    } catch (StreamAccessException e) {
      // Expected.
    }

    expectStreamUnusable();
  }

  private void testMutationFailure(Exception e) throws TimeoutException, Log.WriterFailedException {
    shutdownHooks.execute();
    expect(dummyMutation.apply(EasyMock.<WriterInterface>anyObject())).andThrow(e);

    control.replay();
    logStream.mutate(stats, dummyMutation);
  }

  private void expectStreamUnusable() {
    try {
      logStream.mutate(stats, dummyMutation);
      fail();
    } catch (IllegalStateException e) {
      // Expected.
    }
  }
}
