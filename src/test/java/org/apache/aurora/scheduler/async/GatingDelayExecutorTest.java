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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class GatingDelayExecutorTest extends EasyMockTest {

  private static final Amount<Long, Time> ONE_SECOND = Amount.of(1L, Time.SECONDS);

  private ScheduledExecutorService gatedExecutor;
  private Runnable runnable;
  private GatingDelayExecutor gatingExecutor;

  @Before
  public void setUp() {
    gatedExecutor = createMock(ScheduledExecutorService.class);
    runnable = createMock(Runnable.class);
    gatingExecutor = new GatingDelayExecutor(gatedExecutor);
  }

  @Test
  public void testGateOpen() {
    gatedExecutor.execute(runnable);

    control.replay();

    // The gate was not closed, so the work is executed immediately.
    gatingExecutor.execute(runnable);
  }

  private IExpectationSetters<?> invokeWorkWhenSubmitted() {
    return expectLastCall().andAnswer(() -> {
      ((Runnable) EasyMock.getCurrentArguments()[0]).run();
      return null;
    });
  }

  @Test
  public void testGateIsThreadSpecific() throws InterruptedException {
    gatedExecutor.execute(runnable);

    control.replay();

    CountDownLatch gateClosed = new CountDownLatch(1);
    CountDownLatch unblock = new CountDownLatch(1);
    Runnable closer = () -> gatingExecutor.closeDuring(() -> {
      gateClosed.countDown();
      try {
        unblock.await();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
      return "hi";
    });
    new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("GateTest")
        .build()
        .newThread(closer)
        .start();

    gateClosed.await();
    gatingExecutor.execute(runnable);
    assertQueueSize(0);
    unblock.countDown();
  }

  private void assertQueueSize(int size) {
    assertEquals(size, gatingExecutor.getQueueSize());
  }

  @Test
  public void testReentrantClose() {
    gatedExecutor.execute(runnable);
    expectLastCall().times(3);

    control.replay();

    gatingExecutor.execute(runnable);
    assertQueueSize(0);

    String result = gatingExecutor.closeDuring(() -> {
      gatingExecutor.execute(runnable);
      assertQueueSize(1);

      String result1 = gatingExecutor.closeDuring(() -> {
        gatingExecutor.execute(runnable);
        assertQueueSize(2);
        return "hello";
      });
      assertEquals("hello", result1);

      return "hi";
    });
    assertEquals("hi", result);
    assertQueueSize(0);
  }

  @Test
  public void testExecute() {
    gatedExecutor.execute(runnable);
    invokeWorkWhenSubmitted();
    runnable.run();
    expectLastCall();

    control.replay();

    gatingExecutor.execute(runnable);
  }

  @Test
  public void testExecuteAfterDelay() {
    gatedExecutor.schedule(
        runnable,
        ONE_SECOND.getValue().longValue(),
        ONE_SECOND.getUnit().getTimeUnit());
    invokeWorkWhenSubmitted();
    runnable.run();

    control.replay();

    gatingExecutor.execute(runnable, ONE_SECOND);
  }
}
