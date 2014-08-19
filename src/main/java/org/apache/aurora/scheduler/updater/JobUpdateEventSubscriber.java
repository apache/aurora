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

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import static org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;

/**
 * A pubsub event subscriber that forwards status updates to the job update controller.
 */
class JobUpdateEventSubscriber implements PubsubEvent.EventSubscriber {
  private final JobUpdateController controller;
  private final Storage storage;

  @Inject
  JobUpdateEventSubscriber(JobUpdateController controller, Storage storage) {
    this.controller = requireNonNull(controller);
    this.storage = requireNonNull(storage);
  }

  private static final Function<IScheduledTask, IInstanceKey> TASK_TO_INSTANCE_KEY =
      new Function<IScheduledTask, IInstanceKey>() {
        @Override
        public IInstanceKey apply(IScheduledTask task) {
          return IInstanceKey.build(
              new InstanceKey()
                  .setJobKey(Tasks.SCHEDULED_TO_JOB_KEY.apply(task).newBuilder())
                  .setInstanceId(Tasks.SCHEDULED_TO_INSTANCE_ID.apply(task)));
        }
      };

  @Subscribe
  public synchronized void taskChangedState(TaskStateChange change) {
    controller.instanceChangedState(TASK_TO_INSTANCE_KEY.apply(change.getTask()));
  }

  @Subscribe
  public synchronized void tasksDeleted(TasksDeleted event) {
    Set<IInstanceKey> instances = FluentIterable.from(event.getTasks())
        .transform(TASK_TO_INSTANCE_KEY)
        .toSet();
    for (IInstanceKey instance : instances) {
      controller.instanceChangedState(instance);
    }
  }

  @VisibleForTesting
  static final IJobUpdateQuery ACTIVE_QUERY = IJobUpdateQuery.build(
      new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of(ROLLING_FORWARD, ROLLING_BACK)));

  @Subscribe
  public synchronized void schedulerActive(PubsubEvent.SchedulerActive event)
      throws UpdateStateException {

    storage.write(new Storage.MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider)
          throws UpdateStateException {

        for (IJobUpdateSummary summary
            : storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(ACTIVE_QUERY)) {

          controller.systemResume(summary.getJobKey());
        }
      }
    });
  }
}
