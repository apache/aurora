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
package org.apache.aurora.scheduler.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * A store that forwards all its operations to underlying storage systems.  Useful for decorating
 * an existing storage system.
 */
public class ForwardingStore implements
    SchedulerStore,
    JobStore,
    TaskStore,
    LockStore,
    QuotaStore,
    AttributeStore,
    JobUpdateStore {

  private final SchedulerStore schedulerStore;
  private final JobStore jobStore;
  private final TaskStore taskStore;
  private final LockStore lockStore;
  private final QuotaStore quotaStore;
  private final AttributeStore attributeStore;
  private final JobUpdateStore jobUpdateStore;

  /**
   * Creates a new forwarding store that delegates to the providing default stores.
   *
   * @param schedulerStore Delegate.
   * @param jobStore Delegate.
   * @param taskStore Delegate.
   * @param lockStore Delegate.
   * @param quotaStore Delegate.
   * @param attributeStore Delegate.
   * @param jobUpdateStore Delegate.
   */
  public ForwardingStore(
      SchedulerStore schedulerStore,
      JobStore jobStore,
      TaskStore taskStore,
      LockStore lockStore,
      QuotaStore quotaStore,
      AttributeStore attributeStore,
      JobUpdateStore jobUpdateStore) {

    this.schedulerStore = requireNonNull(schedulerStore);
    this.jobStore = requireNonNull(jobStore);
    this.taskStore = requireNonNull(taskStore);
    this.lockStore = requireNonNull(lockStore);
    this.quotaStore = requireNonNull(quotaStore);
    this.attributeStore = requireNonNull(attributeStore);
    this.jobUpdateStore = requireNonNull(jobUpdateStore);
  }

  @Override
  public Optional<String> fetchFrameworkId() {
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
  public Set<String> fetchManagerIds() {
    return jobStore.fetchManagerIds();
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
  public Map<String, IResourceAggregate> fetchQuotas() {
    return quotaStore.fetchQuotas();
  }

  @Override
  public Optional<IResourceAggregate> fetchQuota(String role) {
    return quotaStore.fetchQuota(role);
  }

  @Override
  public Optional<IHostAttributes> getHostAttributes(String host) {
    return attributeStore.getHostAttributes(host);
  }

  @Override
  public Set<IHostAttributes> getHostAttributes() {
    return attributeStore.getHostAttributes();
  }

  @Override
  public List<IJobUpdateSummary> fetchJobUpdateSummaries(IJobUpdateQuery query) {
    return jobUpdateStore.fetchJobUpdateSummaries(query);
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdateDetails(String updateId) {
    return jobUpdateStore.fetchJobUpdateDetails(updateId);
  }

  @Override
  public Set<StoredJobUpdateDetails> fetchAllJobUpdateDetails() {
    return jobUpdateStore.fetchAllJobUpdateDetails();
  }

  @Override
  public boolean isActive(String updateId) {
    return jobUpdateStore.isActive(updateId);
  }
}
