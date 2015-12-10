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
package org.apache.aurora.scheduler.app.local.simulator;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.scheduler.app.local.FakeMaster;
import org.apache.aurora.scheduler.app.local.simulator.Events.OfferAccepted;
import org.apache.aurora.scheduler.app.local.simulator.Events.Started;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskState;

import static java.util.Objects.requireNonNull;

/**
 * A collection of fake slaves, which advertise resources to the fake master, and react to tasks
 * being scheduled.
 */
class FakeSlaves {
  private final Set<Offer> offers;
  private final FakeMaster master;
  private final ScheduledExecutorService executor;

  @Inject
  FakeSlaves(Set<Offer> offers, FakeMaster master) {
    this.offers = requireNonNull(offers);
    this.master = requireNonNull(master);
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("FakeSlave=%d").build());
  }

  @Subscribe
  public void start(Started event) {
    master.addResources(offers);
  }

  @Subscribe
  public void offerAccepted(OfferAccepted accepted) {
    // Move the task to starting after a delay.
    executor.schedule(
        () -> {
          master.changeState(accepted.task.getTaskId(), TaskState.TASK_STARTING);

          executor.schedule(
              () -> master.changeState(accepted.task.getTaskId(), TaskState.TASK_RUNNING),
              1,
              TimeUnit.SECONDS);
        },
        1,
        TimeUnit.SECONDS);
  }
}
