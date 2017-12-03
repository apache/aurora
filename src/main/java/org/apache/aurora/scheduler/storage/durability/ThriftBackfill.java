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
package org.apache.aurora.scheduler.storage.durability;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.Inject;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Helps migrating thrift schema by populating deprecated and/or replacement fields.
 */
public final class ThriftBackfill {

  private final TierManager tierManager;

  @Inject
  public ThriftBackfill(TierManager tierManager) {
    this.tierManager = requireNonNull(tierManager);
  }

  private static Resource getResource(Set<Resource> resources, ResourceType type) {
    return resources.stream()
            .filter(e -> ResourceType.fromResource(IResource.build(e)).equals(type))
            .findFirst()
            .orElseThrow(() ->
                    new IllegalArgumentException("Missing resource definition for " + type));
  }

  /**
   * Ensures TaskConfig.resources and correspondent task-level fields are all populated.
   *
   * @param config TaskConfig to backfill.
   * @return Backfilled TaskConfig.
   */
  public TaskConfig backfillTask(TaskConfig config) {
    backfillTier(config);
    return config;
  }

  private void backfillTier(TaskConfig config) {
    ITaskConfig taskConfig = ITaskConfig.build(config);
    if (config.isSetTier()) {
      TierInfo tier = tierManager.getTier(taskConfig);
      config.setProduction(!tier.isPreemptible() && !tier.isRevocable());
    } else {
      config.setTier(tierManager.getTiers()
          .entrySet()
          .stream()
          .filter(e -> e.getValue().isPreemptible() == !taskConfig.isProduction()
              && !e.getValue().isRevocable())
          .findFirst()
          .orElseThrow(() -> new IllegalStateException(
              format("No matching implicit tier for task of job %s", taskConfig.getJob())))
          .getKey());
    }
  }

  /**
   * Backfills JobConfiguration. See {@link #backfillTask(TaskConfig)}.
   *
   * @param jobConfig JobConfiguration to backfill.
   * @return Backfilled JobConfiguration.
   */
  public IJobConfiguration backfillJobConfiguration(JobConfiguration jobConfig) {
    backfillTask(jobConfig.getTaskConfig());
    return IJobConfiguration.build(jobConfig);
  }

  /**
   * Backfills set of tasks. See {@link #backfillTask(TaskConfig)}.
   *
   * @param tasks Set of tasks to backfill.
   * @return Backfilled set of tasks.
   */
  public Set<IScheduledTask> backfillTasks(Set<ScheduledTask> tasks) {
    return tasks.stream()
        .map(t -> backfillScheduledTask(t))
        .map(IScheduledTask::build)
        .collect(GuavaUtils.toImmutableSet());
  }

  /**
   * Ensures ResourceAggregate.resources and correspondent deprecated fields are all populated.
   *
   * @param aggregate ResourceAggregate to backfill.
   * @return Backfilled IResourceAggregate.
   */
  public static IResourceAggregate backfillResourceAggregate(ResourceAggregate aggregate) {
    if (!aggregate.isSetResources() || aggregate.getResources().isEmpty()) {
      aggregate.addToResources(Resource.numCpus(aggregate.getNumCpus()));
      aggregate.addToResources(Resource.ramMb(aggregate.getRamMb()));
      aggregate.addToResources(Resource.diskMb(aggregate.getDiskMb()));
    } else {
      EnumSet<ResourceType> quotaResources = QuotaManager.QUOTA_RESOURCE_TYPES;
      if (aggregate.getResources().size() > quotaResources.size()) {
        throw new IllegalArgumentException("Too many resource values in quota.");
      }

      if (!quotaResources.equals(aggregate.getResources().stream()
              .map(e -> ResourceType.fromResource(IResource.build(e)))
              .collect(Collectors.toSet()))) {

        throw new IllegalArgumentException("Quota resources must be exactly: " + quotaResources);
      }
      aggregate.setNumCpus(
              getResource(aggregate.getResources(), CPUS).getNumCpus());
      aggregate.setRamMb(
              getResource(aggregate.getResources(), RAM_MB).getRamMb());
      aggregate.setDiskMb(
              getResource(aggregate.getResources(), DISK_MB).getDiskMb());
    }
    return IResourceAggregate.build(aggregate);
  }

  private ScheduledTask backfillScheduledTask(ScheduledTask task) {
    backfillTask(task.getAssignedTask().getTask());
    return task;
  }

  /**
   * Backfills JobUpdate. See {@link #backfillTask(TaskConfig)}.
   *
   * @param update JobUpdate to backfill.
   * @return Backfilled job update.
   */
  public IJobUpdate backFillJobUpdate(JobUpdate update) {
    JobUpdateInstructions instructions = update.getInstructions();
    if (instructions.isSetDesiredState()) {
      backfillTask(instructions.getDesiredState().getTask());
    }

    instructions.getInitialState().forEach(e -> backfillTask(e.getTask()));

    return IJobUpdate.build(update);
  }
}
