package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

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
  public void deleteJob(String jobKey) {
    jobStore.deleteJob(jobKey);
  }

  @Override
  public void add(Set<ScheduledTask> newTasks) throws IllegalStateException {
    taskStore.add(newTasks);
  }

  @Override
  public void remove(Query query) {
    taskStore.remove(query);
  }

  @Override
  public void remove(Set<String> taskIds) {
    taskStore.remove(taskIds);
  }

  @Override
  public ImmutableSet<ScheduledTask> mutate(Query query, Closure<ScheduledTask> mutator) {
    return taskStore.mutate(query, mutator);
  }

  @Override
  public void update(Set<ScheduledTask> updates) {
    taskStore.update(updates);
  }

  @Override
  public ImmutableSet<ScheduledTask> fetch(Query query) {
    return taskStore.fetch(query);
  }

  @Override
  public Set<String> fetchIds(Query query) {
    return taskStore.fetchIds(query);
  }

  @Override
  public void add(String jobKey, String updateToken,
      Set<TaskUpdateConfiguration> updateConfiguration) {
    updateStore.add(jobKey, updateToken, updateConfiguration);
  }

  @Override
  @Nullable
  public ShardUpdateConfiguration fetchShardUpdateConfig(String jobKey, int shardId) {
    return updateStore.fetchShardUpdateConfig(jobKey, shardId);
  }

  @Override
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String jobKey,
      Set<Integer> shardIds) {
    return updateStore.fetchShardUpdateConfigs(jobKey, shardIds);
  }

  @Override
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String jobKey) {
    return updateStore.fetchShardUpdateConfigs(jobKey);
  }

  @Override
  public void remove(String jobKey) {
    updateStore.remove(jobKey);
  }
}
