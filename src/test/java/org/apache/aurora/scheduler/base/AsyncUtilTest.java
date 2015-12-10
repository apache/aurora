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
package org.apache.aurora.scheduler.base;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.contains;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;

public class AsyncUtilTest extends EasyMockTest {
  private static final String NAME_FORMAT = "Test-%d";
  private Logger logger;
  private CountDownLatch latch;

  @Before
  public void setUp() {
    logger = createMock(Logger.class);
    latch = new CountDownLatch(1);
  }

  @Test
  public void testScheduleLogging() throws Exception {
    expectLogging();

    control.replay();

    scheduledExecutor().schedule(() -> {
      throw new IllegalArgumentException("Expected exception.");
    }, 0, TimeUnit.MILLISECONDS);

    latch.await();
  }

  @Test
  public void testSubmitLogging() throws Exception {
    expectLogging();

    control.replay();

    scheduledExecutor().submit(new Runnable() {
      @Override
      public void run() {
        throw new IllegalArgumentException("Expected exception.");
      }
    });

    latch.await();
  }

  @Test
  public void testExecuteLogging() throws Exception {
    expectLogging();

    control.replay();

    ThreadPoolExecutor executor =
        AsyncUtil.loggingExecutor(1, 1, new LinkedBlockingQueue<>(), NAME_FORMAT, logger);
    executor.execute(() -> {
      throw new IllegalArgumentException("Expected exception.");
    });

    latch.await();
  }

  private void expectLogging() {
    logger.log(
        eq(Level.SEVERE),
        contains("Expected exception."),
        EasyMock.<ExecutionException>anyObject());

    expectLastCall().andAnswer(() -> {
      latch.countDown();
      return null;
    }).once();
  }

  private ScheduledThreadPoolExecutor scheduledExecutor() {
    return AsyncUtil.singleThreadLoggingScheduledExecutor(NAME_FORMAT, logger);
  }
}
