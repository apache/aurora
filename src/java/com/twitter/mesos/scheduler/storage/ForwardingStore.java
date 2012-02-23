package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult.Quiet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A store that forwards all its operations to underlying storage systems.  Useful for decorating
 * an existing storage system.
 *
 * @author John Sirois
 */
public class ForwardingStore implements
    Storage,
    SchedulerStore,
    JobStore,
    TaskStore,
    UpdateStore,
    QuotaStore,
    AttributeStore {

  private final Storage storage;
  private final SchedulerStore schedulerStore;
  private final JobStore jobStore;
  private final TaskStore taskStore;
  private final UpdateStore updateStore;
  private final QuotaStore quotaStore;
  private final AttributeStore attributeStore;

  public ForwardingStore(
      Storage storage,
      SchedulerStore schedulerStore,
      JobStore jobStore,
      TaskStore taskStore,
      UpdateStore updateStore,
      QuotaStore quotaStore,
      AttributeStore attributeStore) {

    this.storage = checkNotNull(storage);
    this.schedulerStore = checkNotNull(schedulerStore);
    this.jobStore = checkNotNull(jobStore);
    this.taskStore = checkNotNull(taskStore);
    this.updateStore = checkNotNull(updateStore);
    this.quotaStore = checkNotNull(quotaStore);
    this.attributeStore = checkNotNull(attributeStore);
  }

  @Override
  public void prepare() {
    storage.prepare();
  }

  @Override
  public void start(Quiet initilizationLogic) {
    storage.start(initilizationLogic);
  }

  @Override
  public <T, E extends Exception> T doInTransaction(Work<T, E> work) throws StorageException, E {
    return storage.doInTransaction(work);
  }

  @Override
  public void stop() {
    storage.stop();
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
  public Set<String> fetchManagerIds() {
    return jobStore.fetchManagerIds();
  }

  @Override
  public void saveTasks(Set<ScheduledTask> tasks) throws IllegalStateException {
    taskStore.saveTasks(tasks);
  }

  @Override
  public void removeTasks(Query query) {
    taskStore.removeTasks(query);
  }

  @Override
  public void removeTasks(Set<String> taskIds) {
    taskStore.removeTasks(taskIds);
  }

  @Override
  public ImmutableSet<ScheduledTask> mutateTasks(Query query, Closure<ScheduledTask> mutator) {
    return taskStore.mutateTasks(query, mutator);
  }

  public ImmutableSet<ScheduledTask> fetchTasks(Query query) {
    return taskStore.fetchTasks(query);
  }

  @Override
  public Set<String> fetchTaskIds(Query query) {
    return taskStore.fetchTaskIds(query);
  }

  @Override
  public void saveShardUpdateConfigs(String role, String job, String updateToken,
      Set<TaskUpdateConfiguration> updateConfiguration) {
    updateStore.saveShardUpdateConfigs(role, job, updateToken, updateConfiguration);
  }

  @Override
  @Nullable
  public ShardUpdateConfiguration fetchShardUpdateConfig(String role, String job, int shardId) {
    return updateStore.fetchShardUpdateConfig(role, job, shardId);
  }

  @Override
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String role, String job,
      Set<Integer> shardIds) {
    return updateStore.fetchShardUpdateConfigs(role, job, shardIds);
  }

  @Override
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String role, String job) {
    return updateStore.fetchShardUpdateConfigs(role, job);
  }

  @Override
  public Multimap<String, ShardUpdateConfiguration> fetchShardUpdateConfigs(String role) {
    return updateStore.fetchShardUpdateConfigs(role);
  }

  @Override
  public void removeShardUpdateConfigs(String role, String job) {
    updateStore.removeShardUpdateConfigs(role, job);
  }

  @Override
  public void removeQuota(String role) {
    quotaStore.removeQuota(role);
  }

  @Override
  public void saveQuota(String role, Quota quota) {
    quotaStore.saveQuota(role, quota);
  }

  @Override
  public Quota fetchQuota(String role) {
    return quotaStore.fetchQuota(role);
  }

  @Override
  public void saveHostAttributes(HostAttributes hostAttribute) {
    attributeStore.saveHostAttributes(hostAttribute);
  }

  @Override
  public Iterable<Attribute> getHostAttributes(String host) {
    return attributeStore.getHostAttributes(host);
  }
}
