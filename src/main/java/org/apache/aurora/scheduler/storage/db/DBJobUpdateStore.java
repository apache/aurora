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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

/**
 * A relational database-backed job update store.
 */
public class DBJobUpdateStore implements JobUpdateStore.Mutable {

  @Override
  public void saveJobUpdate(IJobUpdate update) {
    // TODO(maxim): implement DB mapping logic.
  }

  @Override
  public void saveJobUpdateEvent(IJobUpdateEvent event, String updateId) {
    // TODO(maxim): implement DB mapping logic.
  }

  @Override
  public void saveJobInstanceUpdateEvent(IJobInstanceUpdateEvent event, String updateId) {
    // TODO(maxim): implement DB mapping logic.
  }

  @Override
  public ImmutableSet<IJobUpdateSummary> fetchJobUpdateSummaries(JobUpdateQuery query) {
    // TODO(maxim): implement DB mapping logic.
    return ImmutableSet.of();
  }

  @Override
  public void deleteAllUpdatesAndEvents() {
    // TODO(maxim): implement DB mapping logic.
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdateDetails(String updateId) {
    // TODO(maxim): implement DB mapping logic.
    return Optional.absent();
  }

  @Override
  public ImmutableSet<IJobUpdateDetails> fetchAllJobUpdateDetails() {
    // TODO(maxim): implement DB mapping logic.
    return ImmutableSet.of();
  }
}
