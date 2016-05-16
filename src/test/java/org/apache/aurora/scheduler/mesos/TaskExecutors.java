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

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Utility class to contain constants related to setting up executor settings.
 */
public final class TaskExecutors {

  private TaskExecutors() {
    // Utility class.
  }

  public static final ExecutorSettings NO_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(ImmutableSet.of());

  public static final ExecutorSettings SOME_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(ImmutableSet.of(
          mesosScalar(CPUS, 0.01),
          mesosScalar(RAM_MB, 256)));
}
