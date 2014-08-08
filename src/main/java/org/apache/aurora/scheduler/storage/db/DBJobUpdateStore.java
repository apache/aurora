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

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

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
  public void saveJobUpdate(IJobUpdate update) {
    jobKeyMapper.merge(update.getSummary().getJobKey().newBuilder());
    detailsMapper.merge(update.newBuilder());
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
  public ImmutableSet<IJobUpdateSummary> fetchJobUpdateSummaries(JobUpdateQuery query) {
    // TODO(maxim): implement DB mapping logic.
    return ImmutableSet.of();
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdateDetails(final String updateId) {
    // TODO(maxim): add support for job_update_configs and update_only_these_instances.
    return Optional.fromNullable(detailsMapper.selectDetails(updateId))
        .transform(new Function<JobUpdateDetails, IJobUpdateDetails>() {
          @Override
          public IJobUpdateDetails apply(JobUpdateDetails input) {
            return IJobUpdateDetails.build(input);
          }
        });
  }

  @Override
  public ImmutableSet<IJobUpdateDetails> fetchAllJobUpdateDetails() {
    // TODO(maxim): implement DB mapping logic.
    return ImmutableSet.of();
  }
}
