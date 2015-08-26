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

import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;

import static java.util.Objects.requireNonNull;

/**
 * An executor that queues work until flushed.
 */
class GatedDelayExecutor implements DelayExecutor, FlushableWorkQueue {

  private final ScheduledExecutorService executor;
  private final Queue<Runnable> queue = Lists.newLinkedList();

  /**
   * Creates a gated delay executor that will flush work to the provided {@code delegate}.
   *
   * @param delegate Delegate to execute work with when flushed.
   */
  @Inject
  GatedDelayExecutor(ScheduledExecutorService delegate) {
    this.executor = requireNonNull(delegate);
  }

  synchronized int getQueueSize() {
    return queue.size();
  }

  private synchronized void enqueue(Runnable work) {
    queue.add(work);
  }

  @Override
  public synchronized void flush() {
    for (Runnable work : Iterables.consumingIterable(queue)) {
      work.run();
    }
  }

  @Override
  public synchronized void execute(Runnable command) {
    enqueue(() -> executor.execute(command));
  }

  @Override
  public synchronized void execute(Runnable work, Amount<Long, Time> minDelay) {
    enqueue(() -> executor.schedule(work, minDelay.getValue(), minDelay.getUnit().getTimeUnit()));
  }
}
