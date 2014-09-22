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

import java.util.Objects;

import com.google.common.base.Optional;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.storage.SchedulerStore;

import static com.twitter.common.inject.TimedInterceptor.Timed;

/**
 * A relational database-backed scheduler store.
 */
class DbSchedulerStore implements SchedulerStore.Mutable {

  private final FrameworkIdMapper mapper;

  @Inject
  DbSchedulerStore(FrameworkIdMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper);
  }

  @Timed("scheduler_store_save_framework_id")
  @Override
  public void saveFrameworkId(String frameworkId) {
    mapper.insert(Objects.requireNonNull(frameworkId));
  }

  @Timed("scheduler_store_fetch_framework_id")
  @Override
  public Optional<String> fetchFrameworkId() {
    return Optional.fromNullable(mapper.select());
  }
}
