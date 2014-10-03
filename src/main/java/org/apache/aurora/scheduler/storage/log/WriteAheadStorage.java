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

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveAcceptedJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.ForwardingStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.log.LogStorage.TransactionManager;

/**
 * Mutable stores implementation that translates all operations to {@link Op}s (which are passed
 * to a provided {@link TransactionManager}) before forwarding the operations to delegate mutable
 * stores.
 */
class WriteAheadStorage extends ForwardingStore implements
    MutableStoreProvider,
    SchedulerStore.Mutable,
    JobStore.Mutable,
    TaskStore.Mutable,
    LockStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable,
    JobUpdateStore.Mutable {

  private static final Logger LOG = Logger.getLogger(WriteAheadStorage.class.getName());

  private final TransactionManager transactionManager;
  private final SchedulerStore.Mutable schedulerStore;
  private final JobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
  private final LockStore.Mutable lockStore;
  private final QuotaStore.Mutable quotaStore;
  private final AttributeStore.Mutable attributeStore;
  private final JobUpdateStore.Mutable jobUpdateStore;

  /**
   * Creates a new write-ahead storage that delegates to the providing default stores.
   *
   * @param transactionManager External controller for transaction operations.
   * @param schedulerStore Delegate.
   * @param jobStore       Delegate.
   * @param taskStore      Delegate.
   * @param lockStore      Delegate.
   * @param quotaStore     Delegate.
   * @param attributeStore Delegate.
   * @param jobUpdateStore Delegate.
   */
  WriteAheadStorage(
      TransactionManager transactionManager,
      SchedulerStore.Mutable schedulerStore,
      JobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      LockStore.Mutable lockStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore,
      JobUpdateStore.Mutable jobUpdateStore) {

    super(
        schedulerStore,
        jobStore,
        taskStore,
        lockStore,
        quotaStore,
        attributeStore,
        jobUpdateStore);

    this.transactionManager = requireNonNull(transactionManager);
    this.schedulerStore = requireNonNull(schedulerStore);
    this.jobStore = requireNonNull(jobStore);
    this.taskStore = requireNonNull(taskStore);
    this.lockStore = requireNonNull(lockStore);
    this.quotaStore = requireNonNull(quotaStore);
    this.attributeStore = requireNonNull(attributeStore);
    this.jobUpdateStore = requireNonNull(jobUpdateStore);
  }

  private void write(Op op) {
    Preconditions.checkState(
        transactionManager.hasActiveTransaction(),
        "Mutating operations must be within a transaction.");
    transactionManager.log(op);
  }

  @Timed("scheduler_log_save_framework_id")
  @Override
  public void saveFrameworkId(final String frameworkId) {
    requireNonNull(frameworkId);

    write(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
    schedulerStore.saveFrameworkId(frameworkId);
  }

  @Timed("scheduler_log_unsafe_modify_in_place")
  @Override
  public boolean unsafeModifyInPlace(final String taskId, final ITaskConfig taskConfiguration) {
    requireNonNull(taskId);
    requireNonNull(taskConfiguration);

    boolean mutated = taskStore.unsafeModifyInPlace(taskId, taskConfiguration);
    if (mutated) {
      write(Op.rewriteTask(new RewriteTask(taskId, taskConfiguration.newBuilder())));
    }
    return mutated;
  }

  @Timed("scheduler_log_tasks_remove")
  @Override
  public void deleteTasks(final Set<String> taskIds) {
    requireNonNull(taskIds);

    write(Op.removeTasks(new RemoveTasks(taskIds)));
    taskStore.deleteTasks(taskIds);
  }

  @Timed("scheduler_log_tasks_save")
  @Override
  public void saveTasks(final Set<IScheduledTask> newTasks) {
    requireNonNull(newTasks);

    write(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(newTasks))));
    taskStore.saveTasks(newTasks);
  }

  @Timed("scheduler_log_tasks_mutate")
  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      final Query.Builder query,
      final Function<IScheduledTask, IScheduledTask> mutator) {

    requireNonNull(query);
    requireNonNull(mutator);

    ImmutableSet<IScheduledTask> mutated = taskStore.mutateTasks(query, mutator);

    Map<String, IScheduledTask> tasksById = Tasks.mapById(mutated);
    if (LOG.isLoggable(Level.FINE)) {
      LOG.fine("Storing updated tasks to log: "
          + Maps.transformValues(tasksById, Tasks.GET_STATUS));
    }

    // TODO(William Farner): Avoid writing an op when mutated is empty.
    write(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))));
    return mutated;
  }

  @Timed("scheduler_log_quota_save")
  @Override
  public void saveQuota(final String role, final IResourceAggregate quota) {
    requireNonNull(role);
    requireNonNull(quota);

    write(Op.saveQuota(new SaveQuota(role, quota.newBuilder())));
    quotaStore.saveQuota(role, quota);
  }

  @Timed("scheduler_save_host_attribute")
  @Override
  public void saveHostAttributes(final IHostAttributes attrs) {
    requireNonNull(attrs);

    // Pass the updated attributes upstream, and then check if the stored value changes.
    // We do this since different parts of the system write partial HostAttributes objects
    // and they are merged together internally.
    // TODO(William Farner): Split out a separate method
    //                       saveAttributes(String host, Iterable<Attributes>) to simplify this.
    Optional<IHostAttributes> saved = getHostAttributes(attrs.getHost());
    attributeStore.saveHostAttributes(attrs);
    Optional<IHostAttributes> updated = getHostAttributes(attrs.getHost());
    if (!saved.equals(updated)) {
      write(Op.saveHostAttributes(new SaveHostAttributes(updated.get().newBuilder())));
    }
  }

  @Timed("scheduler_log_job_remove")
  @Override
  public void removeJob(final IJobKey jobKey) {
    requireNonNull(jobKey);

    write(Op.removeJob(new RemoveJob().setJobKey(jobKey.newBuilder())));
    jobStore.removeJob(jobKey);
  }

  @Timed("scheduler_log_job_save")
  @Override
  public void saveAcceptedJob(final String managerId, final IJobConfiguration jobConfig) {
    requireNonNull(managerId);
    requireNonNull(jobConfig);

    write(Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig.newBuilder())));
    jobStore.saveAcceptedJob(managerId, jobConfig);
  }

  @Timed("scheduler_log_quota_remove")
  @Override
  public void removeQuota(final String role) {
    requireNonNull(role);

    write(Op.removeQuota(new RemoveQuota(role)));
    quotaStore.removeQuota(role);
  }

  @Timed("scheduler_lock_save")
  @Override
  public void saveLock(final ILock lock) {
    requireNonNull(lock);

    write(Op.saveLock(new SaveLock(lock.newBuilder())));
    lockStore.saveLock(lock);
  }

  @Timed("scheduler_lock_remove")
  @Override
  public void removeLock(final ILockKey lockKey) {
    requireNonNull(lockKey);

    write(Op.removeLock(new RemoveLock(lockKey.newBuilder())));
    lockStore.removeLock(lockKey);
  }

  @Override
  public void saveJobUpdate(IJobUpdate update, Optional<String> lockToken) {
    requireNonNull(update);

    write(Op.saveJobUpdate(new SaveJobUpdate(update.newBuilder(), lockToken.orNull())));
    jobUpdateStore.saveJobUpdate(update, lockToken);
  }

  @Override
  public void saveJobUpdateEvent(IJobUpdateEvent event, String updateId) {
    requireNonNull(event);
    MorePreconditions.checkNotBlank(updateId);

    write(Op.saveJobUpdateEvent(new SaveJobUpdateEvent(event.newBuilder(), updateId)));
    jobUpdateStore.saveJobUpdateEvent(event, updateId);
  }

  @Override
  public void saveJobInstanceUpdateEvent(IJobInstanceUpdateEvent event, String updateId) {
    requireNonNull(event);
    MorePreconditions.checkNotBlank(updateId);

    write(Op.saveJobInstanceUpdateEvent(new SaveJobInstanceUpdateEvent(
        event.newBuilder(),
        updateId)));
    jobUpdateStore.saveJobInstanceUpdateEvent(event, updateId);
  }

  @Override
  public Set<String> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs) {
    Set<String> prunedUpdates = jobUpdateStore.pruneHistory(
        perJobRetainCount,
        historyPruneThresholdMs);

    if (!prunedUpdates.isEmpty()) {
      // Pruned updates will eventually go away from persisted storage when a new snapshot is cut.
      // So, persisting pruning attempts is not strictly necessary as the periodic pruner will
      // provide eventual consistency between volatile and persistent storage upon scheduler
      // restart. By generating an out of band pruning during log replay the consistency is
      // achieved sooner without potentially exposing pruned but not yet persisted data.
      write(Op.pruneJobUpdateHistory(
          new PruneJobUpdateHistory(perJobRetainCount, historyPruneThresholdMs)));
    }
    return prunedUpdates;
  }

  @Override
  public void deleteAllTasks() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteHostAttributes() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteJobs() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteQuotas() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteLocks() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public void deleteAllUpdatesAndEvents() {
    throw new UnsupportedOperationException(
        "Unsupported since casual storage users should never be doing this.");
  }

  @Override
  public boolean setMaintenanceMode(final String host, final MaintenanceMode mode) {
    requireNonNull(host);
    requireNonNull(mode);

    boolean saved = attributeStore.setMaintenanceMode(host, mode);
    if (saved) {
      write(Op.saveHostAttributes(
          new SaveHostAttributes(attributeStore.getHostAttributes(host).get().newBuilder())));
    }
    return saved;
  }

  @Override
  public SchedulerStore.Mutable getSchedulerStore() {
    return this;
  }

  @Override
  public JobStore.Mutable getJobStore() {
    return this;
  }

  @Override
  public TaskStore.Mutable getUnsafeTaskStore() {
    return this;
  }

  @Override
  public LockStore.Mutable getLockStore() {
    return this;
  }

  @Override
  public QuotaStore.Mutable getQuotaStore() {
    return this;
  }

  @Override
  public AttributeStore.Mutable getAttributeStore() {
    return this;
  }

  @Override
  public TaskStore getTaskStore() {
    return this;
  }

  @Override
  public JobUpdateStore.Mutable getJobUpdateStore() {
    return this;
  }
}
