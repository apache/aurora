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
package org.apache.aurora.scheduler.mesos;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;

/**
 * Utility class to contain constants related to setting up executor settings.
 */
public final class TaskExecutors {

  private TaskExecutors() {
    // Utility class.
  }

  public static final ExecutorSettings NO_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(ResourceSlot.NONE);

  public static final ExecutorSettings SOME_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(
          new ResourceSlot(0.01, Amount.of(256L, Data.MB), Amount.of(0L, Data.MB), 0));
}
