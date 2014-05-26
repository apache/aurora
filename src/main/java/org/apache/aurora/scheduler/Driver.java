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
package org.apache.aurora.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Atomics;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;

import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;

/**
 * Wraps the mesos Scheduler driver to ensure its used in a valid lifecycle; namely:
 * <pre>
 *   (run -> kill*)? -> stop*
 * </pre>
 *
 * Also ensures the driver is only asked for when needed.
 */
public interface Driver {

  /**
   * Launches a task.
   *
   * @param offerId ID of the resource offer to accept with the task.
   * @param task Task to launch.
   */
  void launchTask(OfferID offerId, TaskInfo task);

  /**
   * Declines a resource offer.
   *
   * @param offerId ID of the offer to decline.
   */
  void declineOffer(OfferID offerId);

  /**
   * Sends a kill task request for the given {@code taskId} to the mesos master.
   *
   * @param taskId The id of the task to kill.
   */
  void killTask(String taskId);

  /**
   * Stops the underlying driver if it is running, otherwise does nothing.
   */
  void stop();

  interface SettableDriver extends Driver {
    void initialize(SchedulerDriver driver);
  }

  /**
   * Mesos driver implementation.
   */
  static class DriverImpl implements SettableDriver {
    private static final Logger LOG = Logger.getLogger(Driver.class.getName());

    /**
     * Driver states.
     */
    enum State {
      INIT,
      RUNNING,
      STOPPED
    }

    private final StateMachine<State> stateMachine;
    private final AtomicReference<SchedulerDriver> driverRef = Atomics.newReference();
    private final AtomicLong killFailures = Stats.exportLong("scheduler_driver_kill_failures");

    /**
     * Creates a driver manager that will only ask for the underlying mesos driver when actually
     * needed.
     */
    @Inject
    DriverImpl() {
      this.stateMachine =
          StateMachine.<State>builder("scheduler_driver")
              .initialState(State.INIT)
              .addState(State.INIT, State.RUNNING, State.STOPPED)
              .addState(State.RUNNING, State.STOPPED)
              .logTransitions()
              .throwOnBadTransition(true)
              .build();
    }

    private synchronized SchedulerDriver get(State expected) {
      // TODO(William Farner): Formalize the failure case here by throwing a checked exception.
      stateMachine.checkState(expected);
      return Preconditions.checkNotNull(driverRef.get());
    }

    @Override
    public void launchTask(OfferID offerId, TaskInfo task) {
      get(State.RUNNING).launchTasks(ImmutableList.of(offerId), ImmutableList.of(task));
    }

    @Override
    public void declineOffer(OfferID offerId) {
      get(State.RUNNING).declineOffer(offerId);
    }

    @Override
    public void initialize(SchedulerDriver driver) {
      Preconditions.checkNotNull(driver);
      stateMachine.checkState(State.INIT);
      Preconditions.checkState(driverRef.compareAndSet(null, driver));
      stateMachine.transition(State.RUNNING);
    }

    @Override
    public synchronized void stop() {
      if (stateMachine.getState() == State.RUNNING) {
        SchedulerDriver driver = get(State.RUNNING);
        driver.stop(true /* failover */);
        stateMachine.transition(State.STOPPED);
      }
    }

    @Override
    public void killTask(String taskId) {
      SchedulerDriver driver = get(State.RUNNING);
      Protos.Status status = driver.killTask(Protos.TaskID.newBuilder().setValue(taskId).build());

      if (status != DRIVER_RUNNING) {
        LOG.severe(String.format("Attempt to kill task %s failed with code %s",
            taskId, status));
        killFailures.incrementAndGet();
      }
    }
  }
}
