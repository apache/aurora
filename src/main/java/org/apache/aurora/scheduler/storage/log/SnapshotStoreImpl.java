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
package org.apache.aurora.scheduler.storage.log;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.SlidingStats.Timeable;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public class SnapshotStoreImpl implements Snapshotter {

  @VisibleForTesting
  static final String SNAPSHOT_SAVE = "snapshot_save_";
  @VisibleForTesting
  static final String SNAPSHOT_RESTORE = "snapshot_restore_";

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotStoreImpl.class);

  private static final String HOST_ATTRIBUTES_FIELD = "hosts";
  private static final String QUOTA_FIELD = "quota";
  private static final String TASK_FIELD = "tasks";
  private static final String CRON_FIELD = "crons";
  private static final String JOB_UPDATE_FIELD = "job_updates";
  private static final String SCHEDULER_METADATA_FIELD = "scheduler_metadata";

  @VisibleForTesting
  Set<String> snapshotFieldNames() {
    return snapshotFields.stream()
        .map(SnapshotField::getName)
        .collect(Collectors.toSet());
  }

  private final List<SnapshotField> snapshotFields = ImmutableList.of(
      new SnapshotField() {
        @Override
        String getName() {
          return HOST_ATTRIBUTES_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setHostAttributes(
              IHostAttributes.toBuildersSet(store.getAttributeStore().getHostAttributes()));
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.getHostAttributesSize() > 0) {
            return snapshot.getHostAttributes().stream()
                .map(attributes -> Op.saveHostAttributes(
                    new SaveHostAttributes().setHostAttributes(attributes)));
          }
          return Stream.empty();
        }
      },
      new SnapshotField() {
        @Override
        String getName() {
          return TASK_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setTasks(
              IScheduledTask.toBuildersSet(store.getTaskStore().fetchTasks(Query.unscoped())));
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.getTasksSize() > 0) {
            return Stream.of(Op.saveTasks(new SaveTasks().setTasks(snapshot.getTasks())));
          }
          return Stream.empty();
        }
      },
      new SnapshotField() {
        @Override
        String getName() {
          return CRON_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredCronJob> jobs = ImmutableSet.builder();

          for (IJobConfiguration config : store.getCronJobStore().fetchJobs()) {
            jobs.add(new StoredCronJob(config.newBuilder()));
          }
          snapshot.setCronJobs(jobs.build());
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.getCronJobsSize() > 0) {
            return snapshot.getCronJobs().stream()
                .map(job -> Op.saveCronJob(
                    new SaveCronJob().setJobConfig(job.getJobConfiguration())));
          }
          return Stream.empty();
        }
      },
      new SnapshotField() {
        @Override
        String getName() {
          return SCHEDULER_METADATA_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          // SchedulerMetadata is updated outside of the static list of SnapshotFields
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.isSetSchedulerMetadata()
              && snapshot.getSchedulerMetadata().isSetFrameworkId()) {
            // No delete necessary here since this is a single value.

            return Stream.of(Op.saveFrameworkId(
                new SaveFrameworkId().setId(snapshot.getSchedulerMetadata().getFrameworkId())));
          }
          return Stream.empty();
        }
      },
      new SnapshotField() {
        @Override
        String getName() {
          return QUOTA_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (Map.Entry<String, IResourceAggregate> entry
              : store.getQuotaStore().fetchQuotas().entrySet()) {

            quotas.add(new QuotaConfiguration(entry.getKey(), entry.getValue().newBuilder()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.getQuotaConfigurationsSize() > 0) {
            return snapshot.getQuotaConfigurations().stream()
                .map(quota -> Op.saveQuota(new SaveQuota()
                    .setRole(quota.getRole())
                    .setQuota(quota.getQuota())));
          }
          return Stream.empty();
        }
      },
      new SnapshotField() {
        @Override
        String getName() {
          return JOB_UPDATE_FIELD;
        }

        @Override
        void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setJobUpdateDetails(
              store.getJobUpdateStore().fetchJobUpdates(JobUpdateStore.MATCH_ALL).stream()
                  .map(u -> new StoredJobUpdateDetails().setDetails(u.newBuilder()))
                  .collect(Collectors.toSet()));
        }

        @Override
        Stream<Op> doStreamFrom(Snapshot snapshot) {
          if (snapshot.getJobUpdateDetailsSize() > 0) {
            return snapshot.getJobUpdateDetails().stream()
                .flatMap(details -> {
                  Stream<Op> parent = Stream.of(Op.saveJobUpdate(
                      new SaveJobUpdate().setJobUpdate(details.getDetails().getUpdate())));
                  Stream<Op> jobEvents;
                  if (details.getDetails().getUpdateEventsSize() > 0) {
                    jobEvents = details.getDetails().getUpdateEvents().stream()
                        .map(event -> Op.saveJobUpdateEvent(
                            new SaveJobUpdateEvent()
                                .setKey(details.getDetails().getUpdate().getSummary().getKey())
                                .setEvent(event)));
                  } else {
                    jobEvents = Stream.empty();
                  }

                  Stream<Op> instanceEvents;
                  if (details.getDetails().getInstanceEventsSize() > 0) {
                    instanceEvents = details.getDetails().getInstanceEvents().stream()
                        .map(event -> Op.saveJobInstanceUpdateEvent(
                            new SaveJobInstanceUpdateEvent()
                                .setKey(details.getDetails().getUpdate().getSummary().getKey())
                                .setEvent(event)));
                  } else {
                    instanceEvents = Stream.empty();
                  }

                  return Streams.concat(parent, jobEvents, instanceEvents);
                });
          }
          return Stream.empty();
        }
      }
  );

  private final BuildInfo buildInfo;
  private final Clock clock;

  @Inject
  public SnapshotStoreImpl(BuildInfo buildInfo, Clock clock) {
    this.buildInfo = requireNonNull(buildInfo);
    this.clock = requireNonNull(clock);
  }

  private Snapshot createSnapshot(StoreProvider storeProvider) {
    Snapshot snapshot = new Snapshot();

    // Capture timestamp to signify the beginning of a snapshot operation, apply after in case
    // one of the field closures is mean and tries to apply a timestamp.
    long timestamp = clock.nowMillis();
    for (SnapshotField field : snapshotFields) {
      field.save(storeProvider, snapshot);
    }

    SchedulerMetadata metadata = new SchedulerMetadata()
        .setFrameworkId(storeProvider.getSchedulerStore().fetchFrameworkId().orNull())
        .setDetails(buildInfo.getProperties());

    snapshot.setSchedulerMetadata(metadata);
    snapshot.setTimestamp(timestamp);
    return snapshot;
  }

  @Timed("snapshot_create")
  @Override
  public Snapshot from(StoreProvider stores) {
    return createSnapshot(stores);
  }

  @Timed("snapshot_apply")
  @Override
  public Stream<Op> asStream(Snapshot snapshot) {
    requireNonNull(snapshot);

    LOG.info("Restoring snapshot.");
    return snapshotFields.stream()
        .flatMap(field -> field.streamFrom(snapshot));
  }

  abstract class SnapshotField {

    abstract String getName();

    abstract void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot);

    abstract Stream<Op> doStreamFrom(Snapshot snapshot);

    void save(StoreProvider storeProvider, Snapshot snapshot) {
      stats.getUnchecked(SNAPSHOT_SAVE + getName())
          .time((Timeable.NoResult.Quiet) () -> saveToSnapshot(storeProvider, snapshot));
    }

    Stream<Op> streamFrom(Snapshot snapshot) {
      return stats.getUnchecked(SNAPSHOT_RESTORE + getName()).time(() -> doStreamFrom(snapshot));
    }
  }

  private final LoadingCache<String, SlidingStats> stats = CacheBuilder.newBuilder().build(
      new CacheLoader<String, SlidingStats>() {
        @Override
        public SlidingStats load(String name) throws Exception {
          return new SlidingStats(name, "nanos");
        }
      });
}
