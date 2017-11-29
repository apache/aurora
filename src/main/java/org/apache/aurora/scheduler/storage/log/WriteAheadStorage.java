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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.log.LogStorage.TransactionManager;

/**
 * Mutable stores implementation that translates all operations to {@link Op}s (which are passed
 * to a provided {@link TransactionManager}) before forwarding the operations to delegate mutable
 * stores.
 */
class WriteAheadStorage implements
    MutableStoreProvider,
    SchedulerStore.Mutable,
    CronJobStore.Mutable,
    TaskStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable,
    JobUpdateStore.Mutable {

  private final TransactionManager transactionManager;
  private final SchedulerStore.Mutable schedulerStore;
  private final CronJobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
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
   * @param quotaStore     Delegate.
   * @param attributeStore Delegate.
   * @param jobUpdateStore Delegate.
   */
  WriteAheadStorage(
      TransactionManager transactionManager,
      SchedulerStore.Mutable schedulerStore,
      CronJobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore,
      JobUpdateStore.Mutable jobUpdateStore,
      Logger log,
      EventSink eventSink) {

    this.transactionManager = requireNonNull(transactionManager);
    this.schedulerStore = requireNonNull(schedulerStore);
    this.jobStore = requireNonNull(jobStore);
    this.taskStore = requireNonNull(taskStore);
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
  public Optional<IScheduledTask> mutateTask(
      String taskId,
      Function<IScheduledTask, IScheduledTask> mutator) {

    Optional<IScheduledTask> mutated = taskStore.mutateTask(taskId, mutator);
    log.debug("Storing updated task to log: {}={}", taskId, mutated.get().getStatus());
    write(Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))));

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
  public void saveJobUpdate(IJobUpdate update) {
    requireNonNull(update);

    write(Op.saveJobUpdate(new SaveJobUpdate().setJobUpdate(update.newBuilder())));
    jobUpdateStore.saveJobUpdate(update);
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
  public void removeJobUpdates(Set<IJobUpdateKey> keys) {
    requireNonNull(keys);

    // Compatibility mode - RemoveJobUpdates is not yet written since older versions cannot
    // read it.  JobUpdates are only removed implicitly when a snapshot is taken.
    jobUpdateStore.removeJobUpdates(keys);
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
  public void deleteAllUpdates() {
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

  @Override
  public Optional<String> fetchFrameworkId() {
    return this.schedulerStore.fetchFrameworkId();
  }

  @Override
  public Iterable<IJobConfiguration> fetchJobs() {
    return this.jobStore.fetchJobs();
  }

  @Override
  public Optional<IJobConfiguration> fetchJob(IJobKey jobKey) {
    return this.jobStore.fetchJob(jobKey);
  }

  @Override
  public Optional<IScheduledTask> fetchTask(String taskId) {
    return this.taskStore.fetchTask(taskId);
  }

  @Override
  public Iterable<IScheduledTask> fetchTasks(Query.Builder query) {
    return this.taskStore.fetchTasks(query);
  }

  @Override
  public Set<IJobKey> getJobKeys() {
    return this.taskStore.getJobKeys();
  }

  @Override
  public Optional<IResourceAggregate> fetchQuota(String role) {
    return this.quotaStore.fetchQuota(role);
  }

  @Override
  public Map<String, IResourceAggregate> fetchQuotas() {
    return this.quotaStore.fetchQuotas();
  }

  @Override
  public Optional<IHostAttributes> getHostAttributes(String host) {
    return this.attributeStore.getHostAttributes(host);
  }

  @Override
  public Set<IHostAttributes> getHostAttributes() {
    return this.attributeStore.getHostAttributes();
  }

  @Override
  public List<IJobUpdateDetails> fetchJobUpdates(IJobUpdateQuery query) {
    return this.jobUpdateStore.fetchJobUpdates(query);
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdate(IJobUpdateKey key) {
    return this.jobUpdateStore.fetchJobUpdate(key);
  }
}
