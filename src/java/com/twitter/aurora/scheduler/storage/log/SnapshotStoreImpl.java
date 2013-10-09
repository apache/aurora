/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.storage.log;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.storage.QuotaConfiguration;
import com.twitter.aurora.gen.storage.SchedulerMetadata;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.StoredJob;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Volatile;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.util.BuildInfo;
import com.twitter.common.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.Constants.CURRENT_API_VERSION;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private static final Logger LOG = Logger.getLogger(SnapshotStoreImpl.class.getName());

  private static final SnapshotField ATTRIBUTE_FIELD = new SnapshotField() {
    @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
      snapshot.setHostAttributes(storeProvider.getAttributeStore().getHostAttributes());
    }

    @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
      store.getAttributeStore().deleteHostAttributes();

      if (snapshot.isSetHostAttributes()) {
        for (HostAttributes attributes : snapshot.getHostAttributes()) {
          store.getAttributeStore().saveHostAttributes(attributes);
        }
      }
    }
  };

  private static final Iterable<SnapshotField> SNAPSHOT_FIELDS = Arrays.asList(
      ATTRIBUTE_FIELD,
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setTasks(
              IScheduledTask.toBuildersSet(store.getTaskStore().fetchTasks(Query.unscoped())));
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getUnsafeTaskStore().deleteAllTasks();

          if (snapshot.isSetTasks()) {
            store.getUnsafeTaskStore().saveTasks(
                IScheduledTask.setFromBuilders(snapshot.getTasks()));
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredJob> jobs = ImmutableSet.builder();
          for (String managerId : store.getJobStore().fetchManagerIds()) {
            for (IJobConfiguration config : store.getJobStore().fetchJobs(managerId)) {
              jobs.add(new StoredJob(managerId, config.newBuilder()));
            }
          }
          snapshot.setJobs(jobs.build());
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getJobStore().deleteJobs();

          if (snapshot.isSetJobs()) {
            for (StoredJob job : snapshot.getJobs()) {
              store.getJobStore().saveAcceptedJob(
                  job.getJobManagerId(),
                  IJobConfiguration.build(job.getJobConfiguration()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          Properties props = new BuildInfo().getProperties();

          snapshot.setSchedulerMetadata(
                new SchedulerMetadata()
                  .setFrameworkId(store.getSchedulerStore().fetchFrameworkId())
                  .setRevision(props.getProperty(BuildInfo.Key.GIT_REVISION.value))
                  .setTag(props.getProperty(BuildInfo.Key.GIT_TAG.value))
                  .setTimestamp(props.getProperty(BuildInfo.Key.TIMESTAMP.value))
                  .setUser(props.getProperty(BuildInfo.Key.USER.value))
                  .setMachine(props.getProperty(BuildInfo.Key.MACHINE.value))
                  .setVersion(CURRENT_API_VERSION));
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.isSetSchedulerMetadata()) {
            // No delete necessary here since this is a single value.

            store.getSchedulerStore()
                .saveFrameworkId(snapshot.getSchedulerMetadata().getFrameworkId());
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<JobUpdateConfiguration> updates = ImmutableSet.builder();

          for (String updatingRole : store.getUpdateStore().fetchUpdatingRoles()) {
            updates.addAll(store.getUpdateStore().fetchUpdateConfigs(updatingRole));
          }

          snapshot.setUpdateConfigurations(updates.build());
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getUpdateStore().deleteShardUpdateConfigs();

          if (snapshot.isSetUpdateConfigurations()) {
            for (JobUpdateConfiguration config : snapshot.getUpdateConfigurations()) {
              store.getUpdateStore().saveJobUpdateConfig(config);
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (Map.Entry<String, IQuota> entry : store.getQuotaStore().fetchQuotas().entrySet()) {
            quotas.add(new QuotaConfiguration(entry.getKey(), entry.getValue().newBuilder()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getQuotaStore().deleteQuotas();

          if (snapshot.isSetQuotaConfigurations()) {
            for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
              store.getQuotaStore().saveQuota(quota.getRole(), IQuota.build(quota.getQuota()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setLocks(ImmutableSet.copyOf(store.getUpdateStore().fetchLocks()));
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getUpdateStore().deleteLocks();

          if (snapshot.isSetLocks()) {
            for (Lock lock : snapshot.getLocks()) {
              store.getUpdateStore().saveLock(lock);
            }
          }
        }
      }
  );

  private final Clock clock;
  private final Storage storage;

  @Inject
  public SnapshotStoreImpl(Clock clock, @Volatile Storage storage) {
    this.clock = checkNotNull(clock);
    this.storage = checkNotNull(storage);
  }

  @Timed("snapshot_create")
  @Override public Snapshot createSnapshot() {
    return storage.consistentRead(new Work.Quiet<Snapshot>() {
      @Override public Snapshot apply(StoreProvider storeProvider) {
        Snapshot snapshot = new Snapshot();

        // Capture timestamp to signify the beginning of a snapshot operation, apply after in case
        // one of the field closures is mean and tries to apply a timestamp.
        long timestamp = clock.nowMillis();
        for (SnapshotField field : SNAPSHOT_FIELDS) {
          field.saveToSnapshot(storeProvider, snapshot);
        }
        snapshot.setTimestamp(timestamp);
        return snapshot;
      }
    });
  }

  @Timed("snapshot_apply")
  @Override public void applySnapshot(final Snapshot snapshot) {
    checkNotNull(snapshot);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        LOG.info("Restoring snapshot.");

        for (SnapshotField field : SNAPSHOT_FIELDS) {
          field.restoreFromSnapshot(storeProvider, snapshot);
        }
      }
    });
  }

  private interface SnapshotField {
    void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot);

    void restoreFromSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);
  }
}
