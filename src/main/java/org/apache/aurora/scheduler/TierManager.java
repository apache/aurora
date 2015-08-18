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
package org.apache.aurora.scheduler;

import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Translates job tier configuration into a set of task traits/attributes.
 * TODO(maxim): Implement external configuration support defined here:
 * https://docs.google.com/document/d/1gexe2uM_9gjsV62cMmX0VjH85Uokko21vEoENY2jjF0
 */
public interface TierManager {

  /**
   * Gets {@link TierInfo} instance representing task's tier details.
   *
   * @param taskConfig Task configuration to get tier for.
   * @return {@link TierInfo} for the given {@code taskConfig}.
   */
  TierInfo getTier(ITaskConfig taskConfig);

  class TierManagerImpl implements TierManager {

    @Override
    public TierInfo getTier(ITaskConfig taskConfig) {
      // TODO(maxim): Implement when schema changes are defined.
      return new TierInfo(false);
    }
  }
}
