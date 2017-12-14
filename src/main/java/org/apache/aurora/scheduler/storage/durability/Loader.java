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

import java.util.stream.Stream;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Loader {

  private static final Logger LOG = LoggerFactory.getLogger(Loader.class);

  private Loader() {
    // Utility class.
  }

  /**
   * Loads a sequence of storage operations into the provided stores, applying backfills.
   *
   * @param stores Stores to populate.
   * @param backfill Backfill mechanism to use.
   * @param edits Edits to apply.
   */
  public static void load(
      MutableStoreProvider stores,
      ThriftBackfill backfill,
      Stream<Edit> edits) {

    edits.forEach(edit -> load(stores, backfill, edit));
  }

  private static void load(MutableStoreProvider stores, ThriftBackfill backfill, Edit edit) {
    if (edit.isDeleteAll()) {
      LOG.info("Resetting storage");
      stores.getCronJobStore().deleteJobs();
      stores.getUnsafeTaskStore().deleteAllTasks();
      stores.getQuotaStore().deleteQuotas();
      stores.getAttributeStore().deleteHostAttributes();
      stores.getJobUpdateStore().deleteAllUpdates();
      return;
    }

    Op op = edit.getOp();
    switch (op.getSetField()) {
      case SAVE_FRAMEWORK_ID:
        stores.getSchedulerStore().saveFrameworkId(op.getSaveFrameworkId().getId());
        break;

      case SAVE_CRON_JOB:
        stores.getCronJobStore().saveAcceptedJob(
            backfill.backfillJobConfiguration(op.getSaveCronJob().getJobConfig()));
        break;

      case REMOVE_JOB:
        stores.getCronJobStore().removeJob(IJobKey.build(op.getRemoveJob().getJobKey()));
        break;

      case REMOVE_LOCK:
      case SAVE_LOCK:
        // TODO(jly): Deprecated, remove in 0.21. See AURORA-1959.
        break;

      case SAVE_TASKS:
        stores.getUnsafeTaskStore().saveTasks(backfill.backfillTasks(op.getSaveTasks().getTasks()));
        break;

      case REMOVE_TASKS:
        stores.getUnsafeTaskStore().deleteTasks(op.getRemoveTasks().getTaskIds());
        break;

      case SAVE_QUOTA:
        SaveQuota saveQuota = op.getSaveQuota();
        stores.getQuotaStore().saveQuota(
            saveQuota.getRole(),
            ThriftBackfill.backfillResourceAggregate(saveQuota.getQuota()));
        break;

      case REMOVE_QUOTA:
        stores.getQuotaStore().removeQuota(op.getRemoveQuota().getRole());
        break;

      case SAVE_HOST_ATTRIBUTES:
        HostAttributes attributes = op.getSaveHostAttributes().getHostAttributes();
        // Prior to commit 5cf760b, the store would persist maintenance mode changes for
        // unknown hosts.  5cf760b began rejecting these, but the storage may still
        // contain entries with a null slave ID.
        if (attributes.isSetSlaveId()) {
          stores.getAttributeStore().saveHostAttributes(IHostAttributes.build(attributes));
        } else {
          LOG.info("Dropping host attributes with no agent ID: " + attributes);
        }
        break;

      case SAVE_JOB_UPDATE:
        stores.getJobUpdateStore().saveJobUpdate(
            backfill.backFillJobUpdate(op.getSaveJobUpdate().getJobUpdate()));
        break;

      case SAVE_JOB_UPDATE_EVENT:
        SaveJobUpdateEvent jobEvent = op.getSaveJobUpdateEvent();
        stores.getJobUpdateStore().saveJobUpdateEvent(
            IJobUpdateKey.build(jobEvent.getKey()),
            IJobUpdateEvent.build(op.getSaveJobUpdateEvent().getEvent()));
        break;

      case SAVE_JOB_INSTANCE_UPDATE_EVENT:
        SaveJobInstanceUpdateEvent instanceEvent = op.getSaveJobInstanceUpdateEvent();
        stores.getJobUpdateStore().saveJobInstanceUpdateEvent(
            IJobUpdateKey.build(instanceEvent.getKey()),
            IJobInstanceUpdateEvent.build(op.getSaveJobInstanceUpdateEvent().getEvent()));
        break;

      case PRUNE_JOB_UPDATE_HISTORY:
        LOG.info("Dropping prune operation.  Updates will be pruned later.");
        break;

      case REMOVE_JOB_UPDATE:
        stores.getJobUpdateStore().removeJobUpdates(
            IJobUpdateKey.setFromBuilders(op.getRemoveJobUpdate().getKeys()));
        break;

      default:
        throw new IllegalArgumentException("Unrecognized op type " + op.getSetField());
    }
  }
}
