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
 * An executor that may be temporarily gated with {@link #closeDuring(GatedOperation)}.  When the
 * executor is gated, newly-submitted work will be enqueued and executed once the gate is opened as
 * a result of {@link #closeDuring(GatedOperation)} returning.
 */
class GatingDelayExecutor implements DelayExecutor, GatedWorkQueue {

  private final ScheduledExecutorService gated;
  private final Queue<Runnable> queue = Lists.newLinkedList();

  /**
   * Creates a gating delay executor that will gate work from the provided executor.
   *
   * @param gated Delegate to execute work with when ungated.
   */
  @Inject
  GatingDelayExecutor(ScheduledExecutorService gated) {
    this.gated = requireNonNull(gated);
  }

  private final ThreadLocal<Boolean> isOpen = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return true;
    }
  };

  @Override
  public <T, E extends Exception> T closeDuring(GatedOperation<T, E> operation) throws E {
    boolean startedOpen = isOpen.get();
    isOpen.set(false);

    try {
      return operation.doWithGateClosed();
    } finally {
      if (startedOpen) {
        isOpen.set(true);
        flush();
      }
    }
  }

  synchronized int getQueueSize() {
    return queue.size();
  }

  private synchronized void enqueue(Runnable work) {
    if (isOpen.get()) {
      work.run();
    } else {
      queue.add(work);
    }
  }

  private synchronized void flush() {
    for (Runnable work : Iterables.consumingIterable(queue)) {
      work.run();
    }
  }

  @Override
  public synchronized void execute(Runnable command) {
    enqueue(() -> gated.execute(command));
  }

  @Override
  public synchronized void execute(Runnable work, Amount<Long, Time> minDelay) {
    enqueue(() -> gated.schedule(work, minDelay.getValue(), minDelay.getUnit().getTimeUnit()));
  }
}
