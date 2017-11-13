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
package org.apache.aurora.scheduler.offers;

import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;

import com.google.common.base.Supplier;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;

import static java.util.Objects.requireNonNull;

/**
 * Determines if and when a deferred action should be performed.
 */
public interface Deferment {

  /**
   * Defers an action to possibly be performed at some point in the future.
   *
   * @param action Callback to perform the deferred action.
   */
  void defer(Runnable action);

  /**
   * Never performs deferred actions.
   */
  class Noop implements Deferment {
    @Override
    public void defer(Runnable action) {
      // no-op
    }
  }

  /**
   * Performs a deferred action after a dynamic delay.
   */
  class DelayedDeferment implements Deferment {
    private final Supplier<Amount<Long, Time>> delay;
    private final ScheduledExecutorService executor;

    @Inject
    public DelayedDeferment(
        Supplier<Amount<Long, Time>> delay,
        @AsyncExecutor ScheduledExecutorService executor) {

      this.delay = requireNonNull(delay);
      this.executor = requireNonNull(executor);
    }

    @Override
    public void defer(Runnable action) {
      Amount<Long, Time> actionDelay = delay.get();
      executor.schedule(action, actionDelay.getValue(), actionDelay.getUnit().getTimeUnit());
    }
  }
}
