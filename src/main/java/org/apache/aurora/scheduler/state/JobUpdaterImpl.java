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
package org.apache.aurora.scheduler.state;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of the {@link JobUpdater}.
 */
class JobUpdaterImpl implements JobUpdater {

  private final Storage storage;
  private final UUIDGenerator uuidGenerator;
  private final Clock clock;

  private static final Function<Integer, Range<Integer>> INSTANCE_ID_TO_RANGE =
      new Function<Integer, Range<Integer>>() {
        @Override
        public Range<Integer> apply(Integer id) {
          return Range.closed(id, id).canonical(DiscreteDomain.integers());
        }
      };

  private static final Function<Collection<Range<Integer>>, Set<org.apache.aurora.gen.Range>>
      REDUCE_RANGES = new Function<Collection<Range<Integer>>, Set<org.apache.aurora.gen.Range>>() {
        @Override
        public Set<org.apache.aurora.gen.Range> apply(Collection<Range<Integer>> input) {
          RangeSet<Integer> rangeSet = TreeRangeSet.create();
          for (Range<Integer> range : input) {
            rangeSet.add(range);
          }

          ImmutableSet.Builder<org.apache.aurora.gen.Range> builder = ImmutableSet.builder();
          for (Range<Integer> range : rangeSet.asRanges()) {
            // Canonical range of integers is closedOpen, which makes extracting upper bound
            // a problem without resorting to subtraction. The workaround is to convert range
            // into Contiguous set and get first/last.
            ContiguousSet<Integer> set = ContiguousSet.create(range, DiscreteDomain.integers());
            builder.add(new org.apache.aurora.gen.Range(
                set.first(),
                set.last()));
          }

          return builder.build();
        }
      };

  @Inject
  JobUpdaterImpl(Storage storage, Clock clock, UUIDGenerator uuidGenerator) {
    this.storage = requireNonNull(storage);
    this.clock = requireNonNull(clock);
    this.uuidGenerator = requireNonNull(uuidGenerator);
  }

  @Override
  public String startJobUpdate(final IJobUpdateRequest request, final String user)
      throws UpdaterException {

    return storage.write(new MutateWork<String, UpdaterException>() {
      @Override
      public String apply(Storage.MutableStoreProvider storeProvider) throws UpdaterException {
        String updateId = uuidGenerator.createNew().toString();

        IJobUpdate update = IJobUpdate.build(new JobUpdate()
            .setSummary(new JobUpdateSummary()
                .setJobKey(request.getJobKey().newBuilder())
                .setUpdateId(updateId)
                .setUser(user))
            .setConfiguration(new JobUpdateConfiguration()
                .setSettings(request.getSettings().newBuilder())
                .setInstanceCount(request.getInstanceCount())
                .setNewTaskConfig(request.getTaskConfig().newBuilder())
                .setOldTaskConfigs(buildOldTaskConfigs(request.getJobKey(), storeProvider))));

        IJobUpdateEvent event = IJobUpdateEvent.build(new JobUpdateEvent()
            .setStatus(JobUpdateStatus.ROLLING_FORWARD)
            .setTimestampMs(clock.nowMillis()));

        try {
          storeProvider.getJobUpdateStore().saveJobUpdate(update);
          storeProvider.getJobUpdateStore().saveJobUpdateEvent(event, updateId);
        } catch (StorageException e) {
          throw new UpdaterException("Failed to start update.", e);
        }

        // TODO(maxim): wire in updater logic when ready.

        return updateId;
      }
    });
  }

  private Set<InstanceTaskConfig> buildOldTaskConfigs(
      IJobKey jobKey,
      StoreProvider storeProvider) {

    Set<IScheduledTask> tasks =
        storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active());

    Map<ITaskConfig, Set<org.apache.aurora.gen.Range>> rangesByTask = Maps.transformValues(
        Multimaps.transformValues(
            Multimaps.index(tasks, Tasks.SCHEDULED_TO_INFO),
            Functions.compose(INSTANCE_ID_TO_RANGE, Tasks.SCHEDULED_TO_INSTANCE_ID)).asMap(),
        REDUCE_RANGES);

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, Set<org.apache.aurora.gen.Range>> entry : rangesByTask.entrySet()) {
      builder.add(new InstanceTaskConfig()
          .setTask(entry.getKey().newBuilder())
          .setInstances(entry.getValue()));
    }

    return builder.build();
  }
}
