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

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveCronJob;
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
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
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
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import uno.perk.forward.Forward;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.log.LogStorage.TransactionManager;

/**
 * Mutable stores implementation that translates all operations to {@link Op}s (which are passed
 * to a provided {@link TransactionManager}) before forwarding the operations to delegate mutable
 * stores.
 */
@Forward({
    SchedulerStore.class,
    CronJobStore.class,
    TaskStore.class,
    LockStore.class,
    QuotaStore.class,
    AttributeStore.class,
    JobUpdateStore.class})
class WriteAheadStorage extends WriteAheadStorageForwarder implements
    MutableStoreProvider,
    SchedulerStore.Mutable,
    CronJobStore.Mutable,
    TaskStore.Mutable,
    LockStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable,
    JobUpdateStore.Mutable {

  private final TransactionManager transactionManager;
  private final SchedulerStore.Mutable schedulerStore;
  private final CronJobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
  private final LockStore.Mutable lockStore;
  private final QuotaStore.Mutable quotaStore;
  private final AttributeStore.Mutable attributeStore;
  private final JobUpdateStore.Mutable jobUpdateStore;
  private final Logger log;
  private final EventSink eventSink;

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
      CronJobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      LockStore.Mutable lockStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore,
      JobUpdateStore.Mutable jobUpdateStore,
      Logger log,
      EventSink eventSink) {

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
    this.log = requireNonNull(log);
    this.eventSink = requireNonNull(eventSink);
  }

  private void write(Op op) {
    Preconditions.checkState(
        transactionManager.hasActiveTransaction(),
        "Mutating operations must be within a transaction.");
    transactionManager.log(op);
  }

  @Override
  public void saveFrameworkId(final String frameworkId) {
    requireNonNull(frameworkId);

    write(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
    schedulerStore.saveFrameworkId(frameworkId);
  }

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

  @Override
  public void deleteTasks(final Set<String> taskIds) {
    requireNonNull(taskIds);

    write(Op.removeTasks(new RemoveTasks(taskIds)));
    taskStore.deleteTasks(taskIds);
  }

  @Override
  public void saveTasks(final Set<IScheduledTask> newTasks) {
    requireNonNull(newTasks);

    write(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(newTasks))));
    taskStore.saveTasks(newTasks);
  }

  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      final Query.Builder query,
      final Function<IScheduledTask, IScheduledTask> mutator) {

    requireNonNull(query);
    requireNonNull(mutator);

    ImmutableSet<IScheduledTask> mutated = taskStore.mutateTasks(query, mutator);

    Map<String, IScheduledTask> tasksById = Tasks.mapById(mutated);
    if (log.isLoggable(Level.FINE)) {
      log.fine("Storing updated tasks to log: "
          + Maps.transformValues(tasksById, IScheduledTask::getStatus));
    }

    // TODO(William Farner): Avoid writing an op when mutated is empty.
    write(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))));
    return mutated;
  }

  @Override
  public void saveQuota(final String role, final IResourceAggregate quota) {
    requireNonNull(role);
    requireNonNull(quota);

    write(Op.saveQuota(new SaveQuota(role, quota.newBuilder())));
    quotaStore.saveQuota(role, quota);
  }

  @Override
  public boolean saveHostAttributes(final IHostAttributes attrs) {
    requireNonNull(attrs);

    boolean changed = attributeStore.saveHostAttributes(attrs);
    if (changed) {
      write(Op.saveHostAttributes(new SaveHostAttributes(attrs.newBuilder())));
      eventSink.post(new PubsubEvent.HostAttributesChanged(attrs));
    }
    return changed;
  }

  @Override
  public void removeJob(final IJobKey jobKey) {
    requireNonNull(jobKey);

    write(Op.removeJob(new RemoveJob().setJobKey(jobKey.newBuilder())));
    jobStore.removeJob(jobKey);
  }

  @Override
  public void saveAcceptedJob(final IJobConfiguration jobConfig) {
    requireNonNull(jobConfig);

    write(Op.saveCronJob(new SaveCronJob(jobConfig.newBuilder())));
    jobStore.saveAcceptedJob(jobConfig);
  }

  @Override
  public void removeQuota(final String role) {
    requireNonNull(role);

    write(Op.removeQuota(new RemoveQuota(role)));
    quotaStore.removeQuota(role);
  }

  @Override
  public void saveLock(final ILock lock) {
    requireNonNull(lock);

    write(Op.saveLock(new SaveLock(lock.newBuilder())));
    lockStore.saveLock(lock);
  }

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
  public void saveJobUpdateEvent(IJobUpdateKey key, IJobUpdateEvent event) {
    requireNonNull(key);
    requireNonNull(event);

    write(Op.saveJobUpdateEvent(new SaveJobUpdateEvent(event.newBuilder(), key.newBuilder())));
    jobUpdateStore.saveJobUpdateEvent(key, event);
  }

  @Override
  public void saveJobInstanceUpdateEvent(IJobUpdateKey key, IJobInstanceUpdateEvent event) {
    requireNonNull(key);
    requireNonNull(event);

    write(Op.saveJobInstanceUpdateEvent(
        new SaveJobInstanceUpdateEvent(event.newBuilder(), key.newBuilder())));
    jobUpdateStore.saveJobInstanceUpdateEvent(key, event);
  }

  @Override
  public Set<IJobUpdateKey> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs) {
    Set<IJobUpdateKey> prunedUpdates = jobUpdateStore.pruneHistory(
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
  public SchedulerStore.Mutable getSchedulerStore() {
    return this;
  }

  @Override
  public CronJobStore.Mutable getCronJobStore() {
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
