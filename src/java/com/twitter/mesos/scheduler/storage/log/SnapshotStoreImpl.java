package com.twitter.mesos.scheduler.storage.log;

import java.util.Arrays;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import com.twitter.common.util.Clock;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Volatile;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public final class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

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
          snapshot.setTasks(store.getTaskStore().fetchTasks(Query.GET_ALL));
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getTaskStore().deleteAllTasks();

          if (snapshot.isSetTasks()) {
            store.getTaskStore().saveTasks(snapshot.getTasks());
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredJob> jobs = ImmutableSet.builder();
          for (String managerId : store.getJobStore().fetchManagerIds()) {
            for (JobConfiguration config : store.getJobStore().fetchJobs(managerId)) {
              jobs.add(new StoredJob(managerId, config));
            }
          }
          snapshot.setJobs(jobs.build());
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getJobStore().deleteJobs();

          if (snapshot.isSetJobs()) {
            for (StoredJob job : snapshot.getJobs()) {
              store.getJobStore()
                  .saveAcceptedJob(job.getJobManagerId(), job.getJobConfiguration());
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setSchedulerMetadata(
              new SchedulerMetadata(store.getSchedulerStore().fetchFrameworkId()));
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
          for (String role : store.getQuotaStore().fetchQuotaRoles()) {
            quotas.add(
                new QuotaConfiguration(role, store.getQuotaStore().fetchQuota(role).get()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getQuotaStore().deleteQuotas();

          if (snapshot.isSetQuotaConfigurations()) {
            for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
              store.getQuotaStore().saveQuota(quota.getRole(), quota.getQuota());
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

  @Override public Snapshot createSnapshot() {
    return storage.doInTransaction(new Work.Quiet<Snapshot>() {
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

  @Override public void applySnapshot(final Snapshot snapshot) {
    checkNotNull(snapshot);

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
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
