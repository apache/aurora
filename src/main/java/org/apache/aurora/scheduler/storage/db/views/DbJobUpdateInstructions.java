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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.Set;

import com.google.common.collect.FluentIterable;

import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;

public final class DbJobUpdateInstructions {
  private Set<DbInstanceTaskConfig> initialState;
  private DbInstanceTaskConfig desiredState;
  private JobUpdateSettings settings;

  private DbJobUpdateInstructions() {
  }

  JobUpdateInstructions toThrift() {
    return new JobUpdateInstructions()
        .setInitialState(
            FluentIterable.from(initialState)
                .transform(DbInstanceTaskConfig::toThrift)
                .toSet())
        .setDesiredState(desiredState == null ? null : desiredState.toThrift())
        .setSettings(settings);
  }

  public IJobUpdateInstructions toImmutable() {
    return IJobUpdateInstructions.build(toThrift());
  }
}
