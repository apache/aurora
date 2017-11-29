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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.SlidingStats.Timeable;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

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
    return FluentIterable.from(snapshotFields)
        .transform(SnapshotField::getName)
        .toSet();
  }

  private final Iterable<SnapshotField> snapshotFields = Arrays.asList(
      new SnapshotField() {
        @Override
        public String getName() {
          return HOST_ATTRIBUTES_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setHostAttributes(
              IHostAttributes.toBuildersSet(store.getAttributeStore().getHostAttributes()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.getHostAttributesSize() > 0) {
            store.getAttributeStore().deleteHostAttributes();
            for (HostAttributes attributes : snapshot.getHostAttributes()) {
              store.getAttributeStore().saveHostAttributes(IHostAttributes.build(attributes));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public String getName() {
          return TASK_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setTasks(
              IScheduledTask.toBuildersSet(store.getTaskStore().fetchTasks(Query.unscoped())));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.getTasksSize() > 0) {
            store.getUnsafeTaskStore().deleteAllTasks();
            store.getUnsafeTaskStore()
                .saveTasks(thriftBackfill.backfillTasks(snapshot.getTasks()));
          }
        }
      },
      new SnapshotField() {
        @Override
        public String getName() {
          return CRON_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredCronJob> jobs = ImmutableSet.builder();

          for (IJobConfiguration config : store.getCronJobStore().fetchJobs()) {
            jobs.add(new StoredCronJob(config.newBuilder()));
          }
          snapshot.setCronJobs(jobs.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.getCronJobsSize() > 0) {
            store.getCronJobStore().deleteJobs();
            for (StoredCronJob job : snapshot.getCronJobs()) {
              store.getCronJobStore().saveAcceptedJob(
                  thriftBackfill.backfillJobConfiguration(job.getJobConfiguration()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public String getName() {
          return SCHEDULER_METADATA_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          // SchedulerMetadata is updated outside of the static list of SnapshotFields
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.isSetSchedulerMetadata()
              && snapshot.getSchedulerMetadata().isSetFrameworkId()) {
            // No delete necessary here since this is a single value.

            store.getSchedulerStore()
                .saveFrameworkId(snapshot.getSchedulerMetadata().getFrameworkId());
          }
        }
      },
      new SnapshotField() {
        @Override
        public String getName() {
          return QUOTA_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (Map.Entry<String, IResourceAggregate> entry
              : store.getQuotaStore().fetchQuotas().entrySet()) {

            quotas.add(new QuotaConfiguration(entry.getKey(), entry.getValue().newBuilder()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.getQuotaConfigurationsSize() > 0) {
            store.getQuotaStore().deleteQuotas();
            for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
              store.getQuotaStore()
                  .saveQuota(quota.getRole(), IResourceAggregate.build(quota.getQuota()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public String getName() {
          return JOB_UPDATE_FIELD;
        }

        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setJobUpdateDetails(
              store.getJobUpdateStore().fetchJobUpdates(JobUpdateStore.MATCH_ALL).stream()
                  .map(u -> new StoredJobUpdateDetails().setDetails(u.newBuilder()))
                  .collect(Collectors.toSet()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.getJobUpdateDetailsSize() > 0) {
            JobUpdateStore.Mutable updateStore = store.getJobUpdateStore();
            updateStore.deleteAllUpdates();
            for (StoredJobUpdateDetails storedDetails : snapshot.getJobUpdateDetails()) {
              JobUpdateDetails details = storedDetails.getDetails();
              updateStore.saveJobUpdate(thriftBackfill.backFillJobUpdate(details.getUpdate()));

              if (details.getUpdateEventsSize() > 0) {
                for (JobUpdateEvent updateEvent : details.getUpdateEvents()) {
                  updateStore.saveJobUpdateEvent(
                      IJobUpdateKey.build(details.getUpdate().getSummary().getKey()),
                      IJobUpdateEvent.build(updateEvent));
                }
              }

              if (details.getInstanceEventsSize() > 0) {
                for (JobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
                  updateStore.saveJobInstanceUpdateEvent(
                      IJobUpdateKey.build(details.getUpdate().getSummary().getKey()),
                      IJobInstanceUpdateEvent.build(instanceEvent));
                }
              }
            }
          }
        }
      }
  );

  private final BuildInfo buildInfo;
  private final Clock clock;
  private final Storage storage;
  private final ThriftBackfill thriftBackfill;

  @Inject
  public SnapshotStoreImpl(
      BuildInfo buildInfo,
      Clock clock,
      @Volatile Storage storage,
      ThriftBackfill thriftBackfill) {

    this.buildInfo = requireNonNull(buildInfo);
    this.clock = requireNonNull(clock);
    this.storage = requireNonNull(storage);
    this.thriftBackfill = requireNonNull(thriftBackfill);
  }

  private Snapshot createSnapshot(Storage anyStorage) {
    // It's important to perform snapshot creation in a write lock to ensure all upstream callers
    // are correctly synchronized (e.g. during backup creation).
    return anyStorage.write(storeProvider -> {
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
    });
  }

  @Timed("snapshot_create")
  @Override
  public Snapshot createSnapshot() {
    return createSnapshot(storage);
  }

  @Timed("snapshot_apply")
  @Override
  public void applySnapshot(final Snapshot snapshot) {
    requireNonNull(snapshot);

    storage.write((NoResult.Quiet) storeProvider -> {
      LOG.info("Restoring snapshot.");

      for (SnapshotField field : snapshotFields) {
        field.restore(storeProvider, snapshot);
      }
    });
  }

  abstract class SnapshotField {

    abstract String getName();

    abstract void saveToSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);

    abstract void restoreFromSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);

    void save(MutableStoreProvider storeProvider, Snapshot snapshot) {
      stats.getUnchecked(SNAPSHOT_SAVE + getName())
          .time((Timeable.NoResult.Quiet) () -> saveToSnapshot(storeProvider, snapshot));
    }

    void restore(MutableStoreProvider storeProvider, Snapshot snapshot) {
      stats.getUnchecked(SNAPSHOT_RESTORE + getName())
          .time((Timeable.NoResult.Quiet) () -> restoreFromSnapshot(storeProvider, snapshot));
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
