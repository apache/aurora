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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.sql.DataSource;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.Lock;
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
import org.apache.aurora.scheduler.storage.db.MigrationManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
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

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotStoreImpl.class);

  /**
   * Number of rows to run in a single batch during dbsnapshot restore.
   */
  private static final int DB_BATCH_SIZE = 20;

  private static boolean hasDbSnapshot(Snapshot snapshot) {
    return snapshot.isSetDbScript();
  }

  private boolean hasDbTaskStore(Snapshot snapshot) {
    return useDbSnapshotForTaskStore
        && hasDbSnapshot(snapshot)
        && snapshot.isExperimentalTaskStore();
  }

  private final Iterable<SnapshotField> snapshotFields = Arrays.asList(
      // Order is critical here. The DB snapshot should always be tried first to ensure
      // graceful migration to DBTaskStore. Otherwise, there is a direct risk of losing the cluster.
      // The following scenario illustrates how that can happen:
      // - Dbsnapshot:ON, DBTaskStore:OFF
      // - Scheduler is updated with DBTaskStore:ON, restarts and populates all tasks from snapshot
      // - Should the dbsnapshot get applied last, all tables would be dropped and recreated BUT
      //   since there was no task data stored in dbsnapshot (DBTaskStore was OFF last time
      //   snapshot was cut), all tasks would be erased
      // - If the above is not detected before a new snapshot is cut all tasks will be dropped the
      //   moment a new snapshot is created
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          LOG.info("Saving dbsnapshot");
          // Note: we don't use mybatis mapped statements for performance reasons and to avoid
          // mapping/unmapping hassle as snapshot commands should never be used upstream.
          try (Connection c = ((DataSource) store.getUnsafeStoreAccess()).getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("SCRIPT")) {
              try (ResultSet rs = ps.executeQuery()) {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                while (rs.next()) {
                  String columnValue = rs.getString("SCRIPT");
                  builder.add(columnValue + "\n");
                }
                snapshot.setDbScript(builder.build());
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (snapshot.isSetDbScript()) {
            try (Connection c = ((DataSource) store.getUnsafeStoreAccess()).getConnection()) {
              LOG.info("Dropping all tables");
              try (PreparedStatement drop = c.prepareStatement("DROP ALL OBJECTS")) {
                drop.executeUpdate();
              }

              LOG.info("Restoring dbsnapshot. Row count: " + snapshot.getDbScript().size());
              // Partition the restore script into manageable size batches to avoid possible OOM
              // due to large size DML statement.
              List<List<String>> batches = Lists.partition(snapshot.getDbScript(), DB_BATCH_SIZE);
              for (List<String> batch : batches) {
                try (PreparedStatement restore = c.prepareStatement(Joiner.on("").join(batch))) {
                  restore.executeUpdate();
                }
              }
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }

            try {
              migrationManager.migrate();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        }
      },
      new SnapshotField() {
        // It's important for locks to be replayed first, since there are relations that expect
        // references to be valid on insertion.
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setLocks(ILock.toBuildersSet(store.getLockStore().fetchLocks()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbSnapshot(snapshot)) {
            LOG.info("Deferring lock restore to dbsnapshot");
            return;
          }

          store.getLockStore().deleteLocks();

          if (snapshot.isSetLocks()) {
            for (Lock lock : snapshot.getLocks()) {
              store.getLockStore().saveLock(ILock.build(lock));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setHostAttributes(
              IHostAttributes.toBuildersSet(store.getAttributeStore().getHostAttributes()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbSnapshot(snapshot)) {
            LOG.info("Deferring attribute restore to dbsnapshot");
            return;
          }

          store.getAttributeStore().deleteHostAttributes();

          if (snapshot.isSetHostAttributes()) {
            for (HostAttributes attributes : snapshot.getHostAttributes()) {
              store.getAttributeStore().saveHostAttributes(IHostAttributes.build(attributes));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setTasks(
              IScheduledTask.toBuildersSet(store.getTaskStore().fetchTasks(Query.unscoped())));
          snapshot.setExperimentalTaskStore(useDbSnapshotForTaskStore);
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbTaskStore(snapshot)) {
            LOG.info("Deferring task restore to dbsnapshot");
            return;
          }

          store.getUnsafeTaskStore().deleteAllTasks();

          if (snapshot.isSetTasks()) {
            store.getUnsafeTaskStore()
                .saveTasks(thriftBackfill.backfillTasks(snapshot.getTasks()));
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredCronJob> jobs = ImmutableSet.builder();

          for (IJobConfiguration config : store.getCronJobStore().fetchJobs()) {
            jobs.add(new StoredCronJob(config.newBuilder()));
          }
          snapshot.setCronJobs(jobs.build());
          snapshot.setExperimentalTaskStore(useDbSnapshotForTaskStore);
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbTaskStore(snapshot)) {
            LOG.info("Deferring cron job restore to dbsnapshot");
            return;
          }

          store.getCronJobStore().deleteJobs();

          if (snapshot.isSetCronJobs()) {
            for (StoredCronJob job : snapshot.getCronJobs()) {
              store.getCronJobStore().saveAcceptedJob(
                  thriftBackfill.backfillJobConfiguration(job.getJobConfiguration()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          // SchedulerMetadata is updated outside of the static list of SnapshotFields
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbSnapshot(snapshot)) {
            LOG.info("Deferring metadata restore to dbsnapshot");
            return;
          }

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
          if (hasDbSnapshot(snapshot)) {
            LOG.info("Deferring quota restore to dbsnapshot");
            return;
          }

          store.getQuotaStore().deleteQuotas();

          if (snapshot.isSetQuotaConfigurations()) {
            for (QuotaConfiguration quota : snapshot.getQuotaConfigurations()) {
              store.getQuotaStore()
                  .saveQuota(quota.getRole(), IResourceAggregate.build(quota.getQuota()));
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          snapshot.setJobUpdateDetails(store.getJobUpdateStore().fetchAllJobUpdateDetails());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          if (hasDbSnapshot(snapshot)) {
            LOG.info("Deferring job update restore to dbsnapshot");
            return;
          }

          JobUpdateStore.Mutable updateStore = store.getJobUpdateStore();
          updateStore.deleteAllUpdatesAndEvents();

          if (snapshot.isSetJobUpdateDetails()) {
            for (StoredJobUpdateDetails storedDetails : snapshot.getJobUpdateDetails()) {
              JobUpdateDetails details = storedDetails.getDetails();
              updateStore.saveJobUpdate(
                  thriftBackfill.backFillJobUpdate(details.getUpdate()),
                  Optional.fromNullable(storedDetails.getLockToken()));

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
  private final boolean useDbSnapshotForTaskStore;
  private final MigrationManager migrationManager;
  private final ThriftBackfill thriftBackfill;

  /**
   * Identifies if experimental task store is in use.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @Qualifier
  public @interface ExperimentalTaskStore { }

  @Inject
  public SnapshotStoreImpl(
      BuildInfo buildInfo,
      Clock clock,
      @Volatile Storage storage,
      @ExperimentalTaskStore boolean useDbSnapshotForTaskStore,
      MigrationManager migrationManager,
      ThriftBackfill thriftBackfill) {

    this.buildInfo = requireNonNull(buildInfo);
    this.clock = requireNonNull(clock);
    this.storage = requireNonNull(storage);
    this.useDbSnapshotForTaskStore = useDbSnapshotForTaskStore;
    this.migrationManager = requireNonNull(migrationManager);
    this.thriftBackfill = requireNonNull(thriftBackfill);
  }

  @Timed("snapshot_create")
  @Override
  public Snapshot createSnapshot() {
    // It's important to perform snapshot creation in a write lock to ensure all upstream callers
    // are correctly synchronized (e.g. during backup creation).
    return storage.write(storeProvider -> {
      Snapshot snapshot = new Snapshot();

      // Capture timestamp to signify the beginning of a snapshot operation, apply after in case
      // one of the field closures is mean and tries to apply a timestamp.
      long timestamp = clock.nowMillis();
      for (SnapshotField field : snapshotFields) {
        field.saveToSnapshot(storeProvider, snapshot);
      }

      SchedulerMetadata metadata = new SchedulerMetadata()
          .setFrameworkId(storeProvider.getSchedulerStore().fetchFrameworkId().orNull())
          .setDetails(buildInfo.getProperties());

      snapshot.setSchedulerMetadata(metadata);
      snapshot.setTimestamp(timestamp);
      return snapshot;
    });
  }

  @Timed("snapshot_apply")
  @Override
  public void applySnapshot(final Snapshot snapshot) {
    requireNonNull(snapshot);

    storage.write((NoResult.Quiet) storeProvider -> {
      LOG.info("Restoring snapshot.");

      for (SnapshotField field : snapshotFields) {
        field.restoreFromSnapshot(storeProvider, snapshot);
      }
    });
  }

  private interface SnapshotField {
    void saveToSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);

    void restoreFromSnapshot(MutableStoreProvider storeProvider, Snapshot snapshot);
  }
}
