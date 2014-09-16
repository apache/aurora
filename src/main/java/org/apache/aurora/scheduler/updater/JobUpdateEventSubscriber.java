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
package org.apache.aurora.scheduler.updater;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import static org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;

/**
 * A pubsub event subscriber that forwards status updates to the job update controller.
 */
class JobUpdateEventSubscriber implements PubsubEvent.EventSubscriber {
  private static final Logger LOG = Logger.getLogger(JobUpdateEventSubscriber.class.getName());

  private static final AtomicLong RECOVERY_ERRORS = Stats.exportLong("job_update_recovery_errors");
  private static final AtomicLong DELETE_ERRORS = Stats.exportLong("job_update_delete_errors");
  private static final AtomicLong STATE_CHANGE_ERRORS =
      Stats.exportLong("job_update_state_change_errors");

  private final JobUpdateController controller;

  @Inject
  JobUpdateEventSubscriber(JobUpdateController controller) {
    this.controller = requireNonNull(controller);
  }

  @Subscribe
  public synchronized void taskChangedState(TaskStateChange change) {
    try {
      controller.instanceChangedState(change.getTask());
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to handle state change: " + e, e);
      STATE_CHANGE_ERRORS.incrementAndGet();
    }
  }

  @Subscribe
  public synchronized void tasksDeleted(TasksDeleted event) {
    for (IScheduledTask task : event.getTasks()) {
      // Ignore pruned tasks, since they are irrelevant to updates.
      try {
        if (!Tasks.isTerminated(task.getStatus())) {
          controller.instanceDeleted(
              InstanceKeys.from(
                  Tasks.SCHEDULED_TO_JOB_KEY.apply(task),
                  task.getAssignedTask().getInstanceId()));
        }
      } catch (RuntimeException e) {
        LOG.log(Level.SEVERE, "Failed to handle instance deletion: " + e, e);
        DELETE_ERRORS.incrementAndGet();
      }
    }
  }

  @Subscribe
  public synchronized void schedulerActive(PubsubEvent.SchedulerActive event) {
    try {
      controller.systemResume();
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to resume job updates: " + e, e);
      RECOVERY_ERRORS.incrementAndGet();
    }
  }
}
