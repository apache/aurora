package com.twitter.mesos.scheduler.storage.log;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
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
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public final class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private static final Logger LOG = Logger.getLogger(SnapshotStoreImpl.class.getName());

  private static final Set<Snapshot._Fields> NEW_FIELDS = EnumSet.of(
      Snapshot._Fields.TASKS,
      Snapshot._Fields.JOBS,
      Snapshot._Fields.SCHEDULER_METADATA,
      Snapshot._Fields.UPDATE_CONFIGURATIONS,
      Snapshot._Fields.QUOTA_CONFIGURATIONS
  );

  private static final SnapshotField ATTRIBUTE_FIELD = new SnapshotField() {
    @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
      snapshot.setHostAttributes(storeProvider.getAttributeStore().getHostAttributes());
    }

    @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
      storeProvider.getAttributeStore().deleteHostAttributes();

      if (snapshot.isSetHostAttributes()) {
        for (HostAttributes attributes : snapshot.getHostAttributes()) {
          storeProvider.getAttributeStore().saveHostAttributes(attributes);
        }
      }
    }
  };

  private static final Iterable<SnapshotField> SNAPSHOT_FIELDS = Arrays.asList(
      ATTRIBUTE_FIELD,
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          snapshot.setTasks(storeProvider.getTaskStore().fetchTasks(Query.GET_ALL));
        }

        @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          storeProvider.getTaskStore().removeTasks(Query.GET_ALL);

          if (snapshot.isSetTasks()) {
            storeProvider.getTaskStore().saveTasks(snapshot.getTasks());
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          ImmutableSet.Builder<StoredJob> jobs = ImmutableSet.builder();
          for (String managerId : storeProvider.getJobStore().fetchManagerIds()) {
            for (JobConfiguration config : storeProvider.getJobStore().fetchJobs(managerId)) {
              jobs.add(new StoredJob(managerId, config));
            }
          }
          snapshot.setJobs(jobs.build());
        }

        @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          storeProvider.getJobStore().deleteJobs();

          if (snapshot.isSetJobs()) {
            for (StoredJob job : snapshot.getJobs()) {
              storeProvider.getJobStore()
                  .saveAcceptedJob(job.getJobManagerId(), job.getJobConfiguration());
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          snapshot.setSchedulerMetadata(
              new SchedulerMetadata(storeProvider.getSchedulerStore().fetchFrameworkId()));
        }

        @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          if (snapshot.isSetSchedulerMetadata()) {
            // No delete necessary here since this is a single value.

            storeProvider.getSchedulerStore()
                .saveFrameworkId(snapshot.getSchedulerMetadata().getFrameworkId());
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          ImmutableSet.Builder<JobUpdateConfiguration> updates = ImmutableSet.builder();

          for (String updatingRole : storeProvider.getUpdateStore().fetchUpdatingRoles()) {
            updates.addAll(storeProvider.getUpdateStore().fetchUpdateConfigs(updatingRole));
          }

          snapshot.setUpdateConfigurations(updates.build());
        }

        @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          storeProvider.getUpdateStore().deleteShardUpdateConfigs();

          if (snapshot.isSetUpdateConfigurations()) {
            for (JobUpdateConfiguration config : snapshot.getUpdateConfigurations()) {
              storeProvider.getUpdateStore().saveJobUpdateConfig(config);
            }
          }
        }
      },
      new SnapshotField() {
        @Override public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (String role : storeProvider.getQuotaStore().fetchQuotaRoles()) {
            quotas.add(
                new QuotaConfiguration(role, storeProvider.getQuotaStore().fetchQuota(role).get()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override public void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          storeProvider.getQuotaStore().deleteQuotas();

          if (snapshot.isSetQuotaConfigurations()) {
            for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
              storeProvider.getQuotaStore().saveQuota(quota.getRole(), quota.getQuota());
            }
          }
        }
      }
  );

  private final Clock clock;
  private final SnapshotStore<byte[]> binarySnapshotStore;
  private final Storage storage;

  @Inject
  public SnapshotStoreImpl(Clock clock,
      SnapshotStore<byte[]> binarySnapshotStore,
      Storage storage) {

    this.clock = checkNotNull(clock);
    this.binarySnapshotStore = checkNotNull(binarySnapshotStore);
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

  private static void checkNewFieldsBlank(Snapshot snapshot) {
    for (Snapshot._Fields field : NEW_FIELDS) {
      Preconditions.checkState(!snapshot.isSet(field),
          "Unexpected field set in snapshot: " + field
              + ", snapshot taken at " + snapshot.getTimestamp()
              + ", field value: " + snapshot.getFieldValue(field));
    }
  }

  @Override public void applySnapshot(final Snapshot snapshot) {
    checkNotNull(snapshot);

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {

        if (snapshot.isSetDataDEPRECATED()) {
          // TODO(wfarner): Remove this after rolled forward to all clusters.
          LOG.info("Restoring from old-style snapshot.");
          checkNewFieldsBlank(snapshot);

          binarySnapshotStore.applySnapshot(snapshot.getDataDEPRECATED());
          ATTRIBUTE_FIELD.restoreFromSnapshot(storeProvider, snapshot);
        } else {
          LOG.info("Restoring from new-style snapshot.");

          for (SnapshotField field : SNAPSHOT_FIELDS) {
            field.restoreFromSnapshot(storeProvider, snapshot);
          }
        }
      }
    });
  }

  private interface SnapshotField {
    void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot);

    void restoreFromSnapshot(StoreProvider storeProvider, Snapshot snapshot);
  }
}
