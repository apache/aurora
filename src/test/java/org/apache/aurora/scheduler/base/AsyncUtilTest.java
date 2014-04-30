/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.base;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.contains;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;

public class AsyncUtilTest extends EasyMockTest {
  private Logger logger;
  private ScheduledThreadPoolExecutor executor;

  @Before
  public void setUp() {
    logger = createMock(Logger.class);
    executor = AsyncUtil.singleThreadLoggingScheduledExecutor("Test-%d", logger);
  }

  @Test(expected = ExecutionException.class)
  public void testScheduleLogging() throws Exception {
    logger.log(
        eq(Level.SEVERE),
        contains("Expected exception."),
        EasyMock.<ExecutionException>anyObject());

    expectLastCall().times(1);

    control.replay();

    ScheduledFuture<?> future = executor.schedule(new Runnable() {
      @Override
      public void run() {
        throw new IllegalArgumentException("Expected exception.");
      }
    }, 0, TimeUnit.MILLISECONDS);

    future.get();
  }

  @Test(expected = ExecutionException.class)
  public void testSubmitLogging() throws Exception {
    logger.log(
        eq(Level.SEVERE),
        contains("Expected exception."),
        EasyMock.<ExecutionException>anyObject());

    expectLastCall().times(1);

    control.replay();

    Future<?> future = executor.submit(new Runnable() {
      @Override
      public void run() {
        throw new IllegalArgumentException("Expected exception.");
      }
    });

    future.get();
  }
}
