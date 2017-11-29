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
package org.apache.aurora.benchmark;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;

/**
 * Job update factory.
 */
final class JobUpdates {
  private JobUpdates() {
    // Utility class.
  }

  /**
   * Saves job updates into provided storage.
   *
   * @param storage {@link Storage} instance.
   * @param updates updates to save.
   * @return update keys.
   */
  static Set<IJobUpdateKey> saveUpdates(Storage storage, Iterable<IJobUpdateDetails> updates) {
    ImmutableSet.Builder<IJobUpdateKey> keyBuilder = ImmutableSet.builder();
    storage.write((Storage.MutateWork.NoResult.Quiet) store -> {
      JobUpdateStore.Mutable updateStore = store.getJobUpdateStore();
      updateStore.deleteAllUpdates();
      for (IJobUpdateDetails details : updates) {
        IJobUpdateKey key = details.getUpdate().getSummary().getKey();
        keyBuilder.add(key);
        updateStore.saveJobUpdate(details.getUpdate());

        for (IJobUpdateEvent updateEvent : details.getUpdateEvents()) {
          updateStore.saveJobUpdateEvent(key, updateEvent);
        }

        for (IJobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
          updateStore.saveJobInstanceUpdateEvent(key, instanceEvent);
        }
      }
    });
    return keyBuilder.build();
  }

  static final class Builder {
    static final String USER = "user";
    private int numEvents = 1;
    private int numInstanceEvents = 5000;
    private int numInstanceOverrides = 1;
    private int numUpdateMetadata = 10;

    Builder setNumEvents(int newCount) {
      numEvents = newCount;
      return this;
    }

    Builder setNumInstanceEvents(int newCount) {
      numInstanceEvents = newCount;
      return this;
    }

    Builder setNumInstanceOverrides(int newCount) {
      numInstanceOverrides = newCount;
      return this;
    }

    Builder setNumUpdateMetadata(int newCount) {
      numUpdateMetadata = newCount;
      return this;
    }

    Set<IJobUpdateDetails> build(int count) {
      ImmutableSet.Builder<IJobUpdateDetails> result = ImmutableSet.builder();
      for (int i = 0; i < count; i++) {
        JobKey job = new JobKey("role", "env", UUID.randomUUID().toString());
        JobUpdateKey key = new JobUpdateKey().setJob(job).setId(UUID.randomUUID().toString());

        TaskConfig task = TaskTestUtil.makeConfig(IJobKey.build(job)).newBuilder();
        task.getExecutorConfig().setData(string(10000));

        ImmutableSet.Builder<Metadata> metadata = ImmutableSet.builder();
        for (int k = 0; k < numUpdateMetadata; k++) {
          metadata.add(new Metadata("key-" + k, "value=" + k));
        }

        JobUpdate update = new JobUpdate()
            .setSummary(new JobUpdateSummary()
                .setKey(key)
                .setUser(USER)
                .setMetadata(metadata.build()))
            .setInstructions(new JobUpdateInstructions()
                .setSettings(new JobUpdateSettings()
                    .setUpdateGroupSize(100)
                    .setMaxFailedInstances(1)
                    .setMaxPerInstanceFailures(1)
                    .setMinWaitInInstanceRunningMs(1)
                    .setRollbackOnFailure(true)
                    .setWaitForBatchCompletion(false)
                    .setUpdateOnlyTheseInstances(ranges(numInstanceOverrides)))
                .setInitialState(instanceConfigs(task, numInstanceOverrides))
                .setDesiredState(new InstanceTaskConfig()
                    .setTask(task)
                    .setInstances(ImmutableSet.of(new Range(0, numInstanceEvents)))));

        ImmutableList.Builder<JobUpdateEvent> events = ImmutableList.builder();
        for (int j = 0; j < numEvents; j++) {
          events.add(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, j)
              .setUser(USER)
              .setMessage("message"));
        }

        ImmutableList.Builder<JobInstanceUpdateEvent> instances = ImmutableList.builder();
        for (int k = 0; k < numInstanceEvents; k++) {
          instances.add(new JobInstanceUpdateEvent(k, 0L, JobUpdateAction.INSTANCE_UPDATING));
        }

        result.add(IJobUpdateDetails.build(new JobUpdateDetails()
            .setUpdate(update)
            .setUpdateEvents(events.build())
            .setInstanceEvents(instances.build())));
      }

      return result.build();
    }

    private static Set<InstanceTaskConfig> instanceConfigs(TaskConfig task, int count) {
      ImmutableSet.Builder<InstanceTaskConfig> result = ImmutableSet.builder();
      for (int i = 0; i < count; i++) {
        result.add(new InstanceTaskConfig(task, ImmutableSet.of(new Range(i, i))));
      }

      return result.build();
    }

    private static Set<Range> ranges(int count) {
      ImmutableSet.Builder<Range> result = ImmutableSet.builder();
      for (int i = 0; i < count; i++) {
        result.add(new Range(i, i));
      }

      return result.build();
    }

    private static String string(int numChars) {
      char[] chars = new char[numChars];
      Arrays.fill(chars, 'a');
      return new String(chars);
    }
  }
}
