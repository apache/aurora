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
package org.apache.aurora.scheduler.storage.db;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IRange;

import static java.util.Objects.requireNonNull;

/**
 * A relational database-backed job update store.
 */
public class DBJobUpdateStore implements JobUpdateStore.Mutable {

  private final JobKeyMapper jobKeyMapper;
  private final JobUpdateDetailsMapper detailsMapper;
  private final JobUpdateEventMapper jobEventMapper;
  private final JobInstanceUpdateEventMapper instanceEventMapper;

  @Inject
  DBJobUpdateStore(
      JobKeyMapper jobKeyMapper,
      JobUpdateDetailsMapper detailsMapper,
      JobUpdateEventMapper jobEventMapper,
      JobInstanceUpdateEventMapper instanceEventMapper) {

    this.jobKeyMapper = requireNonNull(jobKeyMapper);
    this.detailsMapper = requireNonNull(detailsMapper);
    this.jobEventMapper = requireNonNull(jobEventMapper);
    this.instanceEventMapper = requireNonNull(instanceEventMapper);
  }

  @Override
  public void saveJobUpdate(IJobUpdate update, String lockToken) {
    requireNonNull(update);
    requireNonNull(lockToken);

    jobKeyMapper.merge(update.getSummary().getJobKey().newBuilder());
    detailsMapper.insert(update.newBuilder());

    String updateId = update.getSummary().getUpdateId();
    detailsMapper.insertLockToken(updateId, lockToken);

    // Insert optional instance update overrides.
    Set<IRange> instanceOverrides =
        update.getInstructions().getSettings().getUpdateOnlyTheseInstances();

    if (!instanceOverrides.isEmpty()) {
      detailsMapper.insertInstanceOverrides(updateId, IRange.toBuildersSet(instanceOverrides));
    }

    // Insert desired state task config and instance mappings.
    IInstanceTaskConfig desired = update.getInstructions().getDesiredState();
    detailsMapper.insertTaskConfig(
        updateId,
        desired.getTask().newBuilder(),
        true,
        new InsertResult());

    detailsMapper.insertDesiredInstances(updateId, IRange.toBuildersSet(desired.getInstances()));

    // Insert initial state task configs and instance mappings.
    for (IInstanceTaskConfig config : update.getInstructions().getInitialState()) {
      InsertResult result = new InsertResult();
      detailsMapper.insertTaskConfig(updateId, config.getTask().newBuilder(), false, result);

      detailsMapper.insertTaskConfigInstances(
          result.getId(),
          IRange.toBuildersSet(config.getInstances()));
    }
  }

  @Override
  public void saveJobUpdateEvent(IJobUpdateEvent event, String updateId) {
    jobEventMapper.insert(updateId, event.newBuilder());
  }

  @Override
  public void saveJobInstanceUpdateEvent(IJobInstanceUpdateEvent event, String updateId) {
    instanceEventMapper.insert(event.newBuilder(), updateId);
  }

  @Override
  public void deleteAllUpdatesAndEvents() {
    detailsMapper.truncate();
  }

  @Override
  public List<IJobUpdateSummary> fetchJobUpdateSummaries(IJobUpdateQuery query) {
    return IJobUpdateSummary.listFromBuilders(detailsMapper.selectSummaries(query.newBuilder()));
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdateDetails(final String updateId) {
    return Optional.fromNullable(detailsMapper.selectDetails(updateId))
        .transform(new Function<StoredJobUpdateDetails, IJobUpdateDetails>() {
          @Override
          public IJobUpdateDetails apply(StoredJobUpdateDetails input) {
            return IJobUpdateDetails.build(input.getDetails());
          }
        });
  }

  @Override
  public Optional<IJobUpdate> fetchJobUpdate(String updateId) {
    return Optional.fromNullable(detailsMapper.selectUpdate(updateId))
        .transform(new Function<JobUpdate, IJobUpdate>() {
          @Override
          public IJobUpdate apply(JobUpdate input) {
            return IJobUpdate.build(input);
          }
        });
  }

  @Override
  public Optional<IJobUpdateInstructions> fetchJobUpdateInstructions(String updateId) {
    return Optional.fromNullable(detailsMapper.selectInstructions(updateId))
        .transform(new Function<JobUpdateInstructions, IJobUpdateInstructions>() {
          @Override
          public IJobUpdateInstructions apply(JobUpdateInstructions input) {
            return IJobUpdateInstructions.build(input);
          }
        });
  }

  @Override
  public Set<StoredJobUpdateDetails> fetchAllJobUpdateDetails() {
    return ImmutableSet.copyOf(detailsMapper.selectAllDetails());
  }

  @Override
  public Optional<String> getLockToken(String updateId) {
    // We assume here that cascading deletes will cause a lock-update associative row to disappear
    // when the lock is invalidated.  This further assumes that a lock row is deleted when a lock
    // is no longer valid.
    return Optional.fromNullable(detailsMapper.selectLockToken(updateId));
  }
}
