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
package org.apache.aurora.scheduler.testing;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;

import org.easymock.EasyMock;
import org.easymock.IAnswer;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

/**
 * A simulated scheduled executor that records scheduled work and executes it when the clock is
 * advanced past their execution time.
 */
public final class FakeScheduledExecutor extends FakeClock {

  private final List<Pair<Long, Runnable>> deferredWork = Lists.newArrayList();

  private FakeScheduledExecutor() { }

  public static FakeScheduledExecutor scheduleExecutor(ScheduledExecutorService mock) {
    FakeScheduledExecutor executor = new FakeScheduledExecutor();
    mock.schedule(
        EasyMock.<Runnable>anyObject(),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS));
    expectLastCall().andAnswer(addExpectations(executor, 1)).anyTimes();

    return executor;
  }

  public static FakeScheduledExecutor scheduleAtFixedRateExecutor(
      ScheduledExecutorService mock,
      int maxInvocations) {

    FakeScheduledExecutor executor = new FakeScheduledExecutor();
    mock.scheduleAtFixedRate(
        EasyMock.<Runnable>anyObject(),
        EasyMock.anyLong(),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS));
    expectLastCall().andAnswer(addExpectations(executor, maxInvocations)).once();

    return executor;
  }

  private static IAnswer<ScheduledFuture<?>> addExpectations(
      final FakeScheduledExecutor executor,
      final int workCount) {

    return new IAnswer<ScheduledFuture<?>>() {
      @Override
      public ScheduledFuture<?> answer() {
        Object[] args = EasyMock.getCurrentArguments();
        Runnable work = (Runnable) args[0];
        long millis = (Long) args[1];
        Preconditions.checkArgument(millis > 0);
        for (int i = 1; i <= workCount; i++) {
          executor.deferredWork.add(Pair.of(executor.nowMillis() + i * millis, work));
        }
        return null;
      }
    };
  }

  @Override
  public void setNowMillis(long nowMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void advance(Amount<Long, Time> period) {
    super.advance(period);
    Iterator<Pair<Long, Runnable>> entries = deferredWork.iterator();
    List<Runnable> toExecute = Lists.newArrayList();
    while (entries.hasNext()) {
      Pair<Long, Runnable> next = entries.next();
      if (next.getFirst() <= nowMillis()) {
        entries.remove();
        toExecute.add(next.getSecond());
      }
    }
    for (Runnable work : toExecute) {
      work.run();
    }
  }

  public void assertEmpty() {
    assertEquals(ImmutableList.<Pair<Long, Runnable>>of(), deferredWork);
  }
}
