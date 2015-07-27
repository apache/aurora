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

import java.util.concurrent.ScheduledExecutorService;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;

public class GatedDelayExecutorTest extends EasyMockTest {

  private static final Amount<Long, Time> ONE_SECOND = Amount.of(1L, Time.SECONDS);

  private ScheduledExecutorService mockExecutor;
  private Runnable runnable;
  private GatedDelayExecutor gatedExecutor;

  @Before
  public void setUp() {
    mockExecutor = createMock(ScheduledExecutorService.class);
    runnable = createMock(Runnable.class);
    gatedExecutor = new GatedDelayExecutor(mockExecutor);
  }

  @Test
  public void testNoFlush() {
    control.replay();

    gatedExecutor.execute(runnable);
    // flush() is not called, so no work is performed.
  }

  private IExpectationSetters<?> invokeWorkWhenSubmitted() {
    return expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() {
        ((Runnable) EasyMock.getCurrentArguments()[0]).run();
        return null;
      }
    });
  }

  @Test
  public void testExecute() {
    mockExecutor.execute(EasyMock.<Runnable>anyObject());
    invokeWorkWhenSubmitted();
    runnable.run();
    expectLastCall();

    control.replay();

    gatedExecutor.execute(runnable);
    gatedExecutor.flush();
  }

  @Test
  public void testExecuteAfterDelay() {
    mockExecutor.schedule(
        EasyMock.<Runnable>anyObject(),
        eq(ONE_SECOND.getValue().longValue()),
        eq(ONE_SECOND.getUnit().getTimeUnit()));
    invokeWorkWhenSubmitted();
    runnable.run();

    control.replay();

    gatedExecutor.execute(runnable, ONE_SECOND);
    gatedExecutor.flush();
  }
}
