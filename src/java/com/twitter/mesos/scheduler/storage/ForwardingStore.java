package com.twitter.mesos.scheduler.storage;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;

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
  public <T, E extends Exception> T doInTransaction(Work<T, E> work) throws StorageException, E {
    return storage.doInTransaction(work);
  }

  @Override
  public <T, E extends Exception> T doInWriteTransaction(MutateWork<T, E> work)
      throws StorageException, E {

    return storage.doInWriteTransaction(work);
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
  @Nullable
  public JobConfiguration fetchJob(String managerId, String jobKey) {
    return jobStore.fetchJob(managerId, jobKey);
  }

  @Override
  public void saveAcceptedJob(String managerId, JobConfiguration jobConfig) {
    jobStore.saveAcceptedJob(managerId, jobConfig);
  }

  @Override
  public void removeJob(String jobKey) {
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
  public void saveTasks(Set<ScheduledTask> tasks) throws IllegalStateException {
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
  public ImmutableSet<ScheduledTask> mutateTasks(TaskQuery query, Closure<ScheduledTask> mutator) {
    return taskStore.mutateTasks(query, mutator);
  }

  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(TaskQuery query) {
    return taskStore.fetchTasks(query);
  }

  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(Supplier<TaskQuery> querySupplier) {
    return taskStore.fetchTasks(querySupplier);
  }

  @Override
  public Set<String> fetchTaskIds(TaskQuery query) {
    return taskStore.fetchTaskIds(query);
  }

  @Override
  public Set<String> fetchTaskIds(Supplier<TaskQuery> querySupplier) {
    return taskStore.fetchTaskIds(querySupplier);
  }

  @Override
  public void saveJobUpdateConfig(JobUpdateConfiguration updateConfiguration) {
    updateStore.saveJobUpdateConfig(updateConfiguration);
  }

  @Override
  public Optional<JobUpdateConfiguration> fetchJobUpdateConfig(String role, String job) {
    return updateStore.fetchJobUpdateConfig(role, job);
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
  public void deleteShardUpdateConfigs() {
    updateStore.deleteShardUpdateConfigs();
  }

  @Override
  public void removeShardUpdateConfigs(String role, String job) {
    updateStore.removeShardUpdateConfigs(role, job);
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
