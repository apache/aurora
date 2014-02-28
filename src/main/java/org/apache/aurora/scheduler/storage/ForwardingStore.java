/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.storage;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A store that forwards all its operations to underlying storage systems.  Useful for decorating
 * an existing storage system.
 */
public class ForwardingStore implements
    Storage,
    SchedulerStore.Mutable,
    JobStore.Mutable,
    TaskStore.Mutable,
    LockStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable {

  private final Storage storage;
  private final SchedulerStore.Mutable schedulerStore;
  private final JobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
  private final LockStore.Mutable lockStore;
  private final QuotaStore.Mutable quotaStore;
  private final AttributeStore.Mutable attributeStore;

  /**
   * Creats a new forwarding store that delegates to the providing default stores.
   *
   * @param storage Delegate.
   * @param schedulerStore Delegate.
   * @param jobStore Delegate.
   * @param taskStore Delegate.
   * @param lockStore Delegate.
   * @param quotaStore Delegate.
   * @param attributeStore Delegate.
   */
  public ForwardingStore(
      Storage storage,
      SchedulerStore.Mutable schedulerStore,
      JobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      LockStore.Mutable lockStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore) {

    this.storage = checkNotNull(storage);
    this.schedulerStore = checkNotNull(schedulerStore);
    this.jobStore = checkNotNull(jobStore);
    this.taskStore = checkNotNull(taskStore);
    this.lockStore = checkNotNull(lockStore);
    this.quotaStore = checkNotNull(quotaStore);
    this.attributeStore = checkNotNull(attributeStore);
  }

  @Override
  public <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    return storage.consistentRead(work);
  }

  @Override
  public <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work)
      throws StorageException, E {
    return storage.weaklyConsistentRead(work);
  }

  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work)
      throws StorageException, E {

    return storage.write(work);
  }

  @Override
  public void saveFrameworkId(String frameworkId) {
    schedulerStore.saveFrameworkId(frameworkId);
  }

  @Override
  @Nullable
  public String fetchFrameworkId() {
    return schedulerStore.fetchFrameworkId();
  }

  @Override
  public Iterable<IJobConfiguration> fetchJobs(String managerId) {
    return jobStore.fetchJobs(managerId);
  }

  @Override
  public Optional<IJobConfiguration> fetchJob(String managerId, IJobKey jobKey) {
    return jobStore.fetchJob(managerId, jobKey);
  }

  @Override
  public void saveAcceptedJob(String managerId, IJobConfiguration jobConfig) {
    jobStore.saveAcceptedJob(managerId, jobConfig);
  }

  @Override
  public void removeJob(IJobKey jobKey) {
    jobStore.removeJob(jobKey);
  }

  @Override
  public void deleteJobs() {
    jobStore.deleteJobs();
  }

  @Override
  public Set<String> fetchManagerIds() {
    return jobStore.fetchManagerIds();
  }

  @Override
  public void saveTasks(Set<IScheduledTask> tasks) throws IllegalStateException {
    taskStore.saveTasks(tasks);
  }

  @Override
  public void deleteAllTasks() {
    taskStore.deleteAllTasks();
  }

  @Override
  public void deleteTasks(Set<String> taskIds) {
    taskStore.deleteTasks(taskIds);
  }

  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      Query.Builder query,
      Function<IScheduledTask, IScheduledTask> mutator) {

    return taskStore.mutateTasks(query, mutator);
  }

  @Override
  public boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration) {
    return taskStore.unsafeModifyInPlace(taskId, taskConfiguration);
  }

  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Query.Builder querySupplier) {
    return taskStore.fetchTasks(querySupplier);
  }

  @Override
  public Set<ILock> fetchLocks() {
    return lockStore.fetchLocks();
  }

  @Override
  public Optional<ILock> fetchLock(ILockKey lockKey) {
    return lockStore.fetchLock(lockKey);
  }

  @Override
  public void saveLock(ILock lock) {
    lockStore.saveLock(lock);
  }

  @Override
  public void removeLock(ILockKey lockKey) {
    lockStore.removeLock(lockKey);
  }

  @Override
  public void deleteLocks() {
    lockStore.deleteLocks();
  }

  @Override
  public Map<String, IResourceAggregate> fetchQuotas() {
    return quotaStore.fetchQuotas();
  }

  @Override
  public void removeQuota(String role) {
    quotaStore.removeQuota(role);
  }

  @Override
  public void deleteQuotas() {
    quotaStore.deleteQuotas();
  }

  @Override
  public void saveQuota(String role, IResourceAggregate quota) {
    quotaStore.saveQuota(role, quota);
  }

  @Override
  public Optional<IResourceAggregate> fetchQuota(String role) {
    return quotaStore.fetchQuota(role);
  }

  @Override
  public void saveHostAttributes(HostAttributes hostAttribute) {
    attributeStore.saveHostAttributes(hostAttribute);
  }

  @Override
  public Optional<HostAttributes> getHostAttributes(String host) {
    return attributeStore.getHostAttributes(host);
  }

  @Override
  public Set<HostAttributes> getHostAttributes() {
    return attributeStore.getHostAttributes();
  }

  @Override
  public void deleteHostAttributes() {
    attributeStore.deleteHostAttributes();
  }

  @Override
  public boolean setMaintenanceMode(String host, MaintenanceMode mode) {
    return attributeStore.setMaintenanceMode(host, mode);
  }
}
