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
import java.util.Properties;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.util.BuildInfo;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.updater.Updates;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.apiConstants.CURRENT_API_VERSION;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private static final Logger LOG = Logger.getLogger(SnapshotStoreImpl.class.getName());

  private static final Iterable<SnapshotField> SNAPSHOT_FIELDS = Arrays.asList(
      new SnapshotField() {
        // It's important for locks to be replayed first, since there are relations that expect
        // references to be valid on insertion.
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setLocks(ILock.toBuildersSet(store.getLockStore().fetchLocks()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
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
        public void saveToSnapshot(StoreProvider storeProvider, Snapshot snapshot) {
          snapshot.setHostAttributes(
              IHostAttributes.toBuildersSet(storeProvider.getAttributeStore().getHostAttributes()));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getAttributeStore().deleteHostAttributes();

          if (snapshot.isSetHostAttributes()) {
            for (HostAttributes attributes : snapshot.getHostAttributes()) {
              // Prior to commit 5cf760b, the store would persist maintenance mode changes for
              // unknown hosts.  5cf760b began rejecting these, but the replicated log may still
              // contain entries with a null slave ID.
              if (attributes.isSetSlaveId()) {
                store.getAttributeStore().saveHostAttributes(IHostAttributes.build(attributes));
              } else {
                LOG.info("Dropping host attributes with no slave ID: " + attributes);
              }
            }
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setTasks(
              IScheduledTask.toBuildersSet(store.getTaskStore().fetchTasks(Query.unscoped())));
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          store.getUnsafeTaskStore().deleteAllTasks();

          if (snapshot.isSetTasks()) {
            store.getUnsafeTaskStore().saveTasks(
                IScheduledTask.setFromBuilders(snapshot.getTasks()));
          }
        }
      },
      new SnapshotField() {
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<StoredJob> jobs = ImmutableSet.builder();
          for (String managerId : store.getJobStore().fetchManagerIds()) {
            for (IJobConfiguration config : store.getJobStore().fetchJobs(managerId)) {
              jobs.add(new StoredJob(managerId, config.newBuilder()));
            }
          }
          snapshot.setJobs(jobs.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
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
        @Override
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          Properties props = new BuildInfo().getProperties();

          snapshot.setSchedulerMetadata(
                new SchedulerMetadata()
                  .setFrameworkId(store.getSchedulerStore().fetchFrameworkId().orNull())
                  .setRevision(props.getProperty(BuildInfo.Key.GIT_REVISION.value))
                  .setTag(props.getProperty(BuildInfo.Key.GIT_TAG.value))
                  .setTimestamp(props.getProperty(BuildInfo.Key.TIMESTAMP.value))
                  .setUser(props.getProperty(BuildInfo.Key.USER.value))
                  .setMachine(props.getProperty(BuildInfo.Key.MACHINE.value))
                  .setVersion(CURRENT_API_VERSION));
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
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          ImmutableSet.Builder<QuotaConfiguration> quotas = ImmutableSet.builder();
          for (Map.Entry<String, IResourceAggregate> entry
              : store.getQuotaStore().fetchQuotas().entrySet()) {

            quotas.add(new QuotaConfiguration(entry.getKey(), entry.getValue().newBuilder()));
          }

          snapshot.setQuotaConfigurations(quotas.build());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
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
        public void saveToSnapshot(StoreProvider store, Snapshot snapshot) {
          snapshot.setJobUpdateDetails(store.getJobUpdateStore().fetchAllJobUpdateDetails());
        }

        @Override
        public void restoreFromSnapshot(MutableStoreProvider store, Snapshot snapshot) {
          JobUpdateStore.Mutable updateStore = store.getJobUpdateStore();
          updateStore.deleteAllUpdatesAndEvents();

          if (snapshot.isSetJobUpdateDetails()) {
            for (StoredJobUpdateDetails storedDetails : snapshot.getJobUpdateDetails()) {
              JobUpdateDetails details = storedDetails.getDetails();
              updateStore.saveJobUpdate(
                  IJobUpdate.build(details.getUpdate()),
                  Optional.fromNullable(storedDetails.getLockToken()));

              if (details.getUpdateEventsSize() > 0) {
                for (JobUpdateEvent updateEvent : details.getUpdateEvents()) {
                  updateStore.saveJobUpdateEvent(
                      Updates.getKey(IJobUpdateSummary.build(details.getUpdate().getSummary())),
                      IJobUpdateEvent.build(updateEvent));
                }
              }

              if (details.getInstanceEventsSize() > 0) {
                for (JobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
                  updateStore.saveJobInstanceUpdateEvent(
                      Updates.getKey(IJobUpdateSummary.build(details.getUpdate().getSummary())),
                      IJobInstanceUpdateEvent.build(instanceEvent));
                }
              }
            }
          }
        }
      }
  );

  private final Clock clock;
  private final Storage storage;

  @Inject
  public SnapshotStoreImpl(Clock clock, @Volatile Storage storage) {
    this.clock = requireNonNull(clock);
    this.storage = requireNonNull(storage);
  }

  @Timed("snapshot_create")
  @Override
  public Snapshot createSnapshot() {
    // It's important to perform snapshot creation in a write lock to ensure all upstream callers
    // are correctly synchronized (e.g. during backup creation).
    return storage.write(new MutateWork.Quiet<Snapshot>() {
      @Override
      public Snapshot apply(MutableStoreProvider storeProvider) {
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
  @Override
  public void applySnapshot(final Snapshot snapshot) {
    requireNonNull(snapshot);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
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
