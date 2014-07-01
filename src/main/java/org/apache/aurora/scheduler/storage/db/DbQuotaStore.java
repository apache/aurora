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

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static java.util.Objects.requireNonNull;

/**
 * Quota store backed by a relational database.
 */
class DbQuotaStore implements QuotaStore.Mutable {

  private final QuotaMapper mapper;

  @Inject
  DbQuotaStore(QuotaMapper mapper) {
    this.mapper = requireNonNull(mapper);
  }

  @Override
  public Optional<IResourceAggregate> fetchQuota(String role) {
    return Optional.fromNullable(mapper.select(role))
        .transform(IResourceAggregate.FROM_BUILDER);
  }

  @Override
  public Map<String, IResourceAggregate> fetchQuotas() {
    ImmutableMap.Builder<String, IResourceAggregate> results = ImmutableMap.builder();
    for (SaveQuota result : mapper.selectAll()) {
      results.put(result.getRole(), IResourceAggregate.build(result.getQuota()));
    }
    return results.build();
  }

  @Override
  public void deleteQuotas() {
    mapper.truncate();
  }

  @Override
  public void removeQuota(String role) {
    mapper.delete(role);
  }

  @Override
  public void saveQuota(String role, IResourceAggregate quota) {
    mapper.merge(role, quota.newBuilder());
  }
}
