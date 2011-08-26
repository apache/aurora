package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.gen.storage.migration.StorageSystemId;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult.Quiet;

/**
 * A store that forwards all its operations to underlying storage systems.  Useful for decorating
 * an existing storage system.
 *
 * @author John Sirois
 */
public class ForwardingStore implements Storage, SchedulerStore, JobStore, TaskStore, UpdateStore {
  private final Storage storage;
  private final SchedulerStore schedulerStore;
  private final JobStore jobStore;
  private final TaskStore taskStore;
  private final UpdateStore updateStore;

  public ForwardingStore(Storage storage, SchedulerStore schedulerStore, JobStore jobStore,
      TaskStore taskStore, UpdateStore updateStore) {
    this.storage = Preconditions.checkNotNull(storage);
    this.schedulerStore = Preconditions.checkNotNull(schedulerStore);
    this.jobStore = Preconditions.checkNotNull(jobStore);
    this.taskStore = Preconditions.checkNotNull(taskStore);
    this.updateStore = Preconditions.checkNotNull(updateStore);
  }

  @Override
  public StorageSystemId id() {
    return storage.id();
  }

  @Override
  public void start(Quiet initilizationLogic) {
    storage.start(initilizationLogic);
  }

  @Override
  public <T, E extends Exception> T doInTransaction(Work<T, E> work) throws E {
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
}
