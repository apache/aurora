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
package com.twitter.aurora.scheduler.storage;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.LockKey;
import com.twitter.aurora.gen.MaintenanceMode;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

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
    UpdateStore.Mutable,
    QuotaStore.Mutable,
    AttributeStore.Mutable {

  private final Storage storage;
  private final SchedulerStore.Mutable schedulerStore;
  private final JobStore.Mutable jobStore;
  private final TaskStore.Mutable taskStore;
  private final UpdateStore.Mutable updateStore;
  private final QuotaStore.Mutable quotaStore;
  private final AttributeStore.Mutable attributeStore;

  /**
   * Creats a new forwarding store that delegates to the providing default stores.
   *
   * @param storage Delegate.
   * @param schedulerStore Delegate.
   * @param jobStore Delegate.
   * @param taskStore Delegate.
   * @param updateStore Delegate.
   * @param quotaStore Delegate.
   * @param attributeStore Delegate.
   */
  public ForwardingStore(
      Storage storage,
      SchedulerStore.Mutable schedulerStore,
      JobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      UpdateStore.Mutable updateStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore) {

    this.storage = checkNotNull(storage);
    this.schedulerStore = checkNotNull(schedulerStore);
    this.jobStore = checkNotNull(jobStore);
    this.taskStore = checkNotNull(taskStore);
    this.updateStore = checkNotNull(updateStore);
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
  public void snapshot() throws StorageException {
    storage.snapshot();
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
  public Iterable<JobConfiguration> fetchJobs(String managerId) {
    return jobStore.fetchJobs(managerId);
  }

  @Override
  public Optional<JobConfiguration> fetchJob(String managerId, JobKey jobKey) {
    return jobStore.fetchJob(managerId, jobKey);
  }

  @Override
  public void saveAcceptedJob(String managerId, JobConfiguration jobConfig) {
    jobStore.saveAcceptedJob(managerId, jobConfig);
  }

  @Override
  public void removeJob(JobKey jobKey) {
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
  public void saveJobUpdateConfig(JobUpdateConfiguration updateConfiguration) {
    updateStore.saveJobUpdateConfig(updateConfiguration);
  }

  @Override
  public Optional<JobUpdateConfiguration> fetchJobUpdateConfig(JobKey jobKey) {
    return updateStore.fetchJobUpdateConfig(jobKey);
  }

  @Override
  public Set<JobUpdateConfiguration> fetchUpdateConfigs(String role) {
    return updateStore.fetchUpdateConfigs(role);
  }

  @Override
  public Set<String> fetchUpdatingRoles() {
    return updateStore.fetchUpdatingRoles();
  }

  @Override
  public Set<Lock> fetchLocks() {
    return updateStore.fetchLocks();
  }

  @Override
  public Optional<Lock> fetchLock(LockKey lockKey) {
    return updateStore.fetchLock(lockKey);
  }

  @Override
  public void saveLock(Lock lock) {
    updateStore.saveLock(lock);
  }

  @Override
  public void removeLock(LockKey lockKey) {
    updateStore.removeLock(lockKey);
  }

  @Override
  public void deleteLocks() {
    updateStore.deleteLocks();
  }

  @Override
  public void deleteShardUpdateConfigs() {
    updateStore.deleteShardUpdateConfigs();
  }

  @Override
  public void removeShardUpdateConfigs(JobKey jobKey) {
    updateStore.removeShardUpdateConfigs(jobKey);
  }

  @Override
  public Map<String, Quota> fetchQuotas() {
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
  public void saveQuota(String role, Quota quota) {
    quotaStore.saveQuota(role, quota);
  }

  @Override
  public Optional<Quota> fetchQuota(String role) {
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
