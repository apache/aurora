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
package org.apache.aurora.common.util;

import java.io.IOException;

import org.apache.aurora.common.base.ExceptionalSupplier;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * @author John Sirois
 */
public class BackoffHelperTest extends EasyMockTest {
  private Clock clock;
  private BackoffStrategy backoffStrategy;
  private BackoffHelper backoffHelper;

  @Before
  public void setUp() {
    clock = createMock(Clock.class);
    backoffStrategy = createMock(BackoffStrategy.class);
    backoffHelper = new BackoffHelper(clock, backoffStrategy);
  }

  @Test
  public void testGetBackoffStrategy() {
    control.replay();

    assertEquals(backoffStrategy, backoffHelper.getBackoffStrategy());
  }

  @Test
  public void testDoUntilSuccess() throws Exception {
    ExceptionalSupplier<Boolean, RuntimeException> task =
        createMock(new Clazz<ExceptionalSupplier<Boolean, RuntimeException>>() { });

    expect(task.get()).andReturn(false);
    expect(backoffStrategy.shouldContinue(0L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(0L)).andReturn(42L);
    clock.waitFor(42L);
    expect(task.get()).andReturn(true);

    control.replay();

    backoffHelper.doUntilSuccess(task);

    control.verify();
  }

  @Test
  public void testDoUntilResult() throws Exception {
    ExceptionalSupplier<String, RuntimeException> task =
        createMock(new Clazz<ExceptionalSupplier<String, RuntimeException>>() { });

    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(0)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(0)).andReturn(42L);
    clock.waitFor(42L);
    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(42L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(42L)).andReturn(37L);
    clock.waitFor(37L);
    expect(task.get()).andReturn("jake");

    control.replay();

    assertEquals("jake", backoffHelper.doUntilResult(task));

    control.verify();
  }

  @Test
  public void testDoUntilResultTransparentException() throws Exception {
    ExceptionalSupplier<String, IOException> task =
        createMock(new Clazz<ExceptionalSupplier<String, IOException>>() { });

    IOException thrown = new IOException();
    expect(task.get()).andThrow(thrown);

    control.replay();

    try {
      backoffHelper.doUntilResult(task);
      fail("Expected exception to be bubbled");
    } catch (IOException e) {
      assertSame(thrown, e);
    }

    control.verify();
  }

  @Test
  public void testDoUntilResultMaxSuccess() throws Exception {
    ExceptionalSupplier<String, RuntimeException> task =
        createMock(new Clazz<ExceptionalSupplier<String, RuntimeException>>() { });

    BackoffHelper maxBackoffHelper = new BackoffHelper(clock, backoffStrategy);

    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(0L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(0)).andReturn(42L);
    clock.waitFor(42L);
    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(42L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(42L)).andReturn(37L);
    clock.waitFor(37L);
    expect(task.get()).andReturn("jake");

    control.replay();

    assertEquals("jake", maxBackoffHelper.doUntilResult(task));

    control.verify();
  }

  @Test
  public void testDoUntilResultMaxReached() throws Exception {
    ExceptionalSupplier<String, RuntimeException> task =
        createMock(new Clazz<ExceptionalSupplier<String, RuntimeException>>() { });

    BackoffHelper maxBackoffHelper = new BackoffHelper(clock, backoffStrategy);

    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(0L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(0)).andReturn(42L);
    clock.waitFor(42L);
    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(42L)).andReturn(true);
    expect(backoffStrategy.calculateBackoffMs(42L)).andReturn(37L);
    clock.waitFor(37L);
    expect(task.get()).andReturn(null);
    expect(backoffStrategy.shouldContinue(37L)).andReturn(false);

    control.replay();

    try {
      maxBackoffHelper.doUntilResult(task);
      fail("Expected max retry failure");
    } catch (BackoffHelper.BackoffStoppedException e) {
      // expected
    }

    control.verify();
  }

  @Test
  public void testDoUntilSuccessTransparentException() throws Exception {
    ExceptionalSupplier<Boolean, RuntimeException> task =
        createMock(new Clazz<ExceptionalSupplier<Boolean, RuntimeException>>() { });

    IllegalArgumentException thrown = new IllegalArgumentException();
    expect(task.get()).andThrow(thrown);

    control.replay();

    try {
      backoffHelper.doUntilSuccess(task);
      fail("Expected exception to be bubbled");
    } catch (IllegalArgumentException e) {
      assertSame(thrown, e);
    }

    control.verify();
  }
}
