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

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.Resources;

/**
 * Utility class to contain constants related to setting up executor settings.
 */
public final class TaskExecutors {

  private TaskExecutors() {
    // Utility class.
  }

  private static final String EXECUTOR_PATH = "/fake/executor.pex";

  public static final ExecutorSettings NO_OVERHEAD_EXECUTOR =
      ExecutorSettings.newBuilder()
          .setExecutorPath(EXECUTOR_PATH)
          .setThermosObserverRoot("/var/run/thermos")
          .build();

  public static final ExecutorSettings SOME_OVERHEAD_EXECUTOR =
      ExecutorSettings.newBuilder()
          .setExecutorPath(EXECUTOR_PATH)
          .setThermosObserverRoot("/var/run/thermos")
          .setExecutorOverhead(
              new Resources(0.01, Amount.of(256L, Data.MB), Amount.of(0L, Data.MB), 0))
          .build();
}
