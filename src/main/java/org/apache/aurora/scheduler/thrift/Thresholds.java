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
package org.apache.aurora.scheduler.thrift;

import com.google.common.base.MoreObjects;

/**
 * Input value thresholds for creating entities via the API.
 */
public class Thresholds {
  private final int maxTasksPerJob;
  private final int maxUpdateInstanceFailures;

  public Thresholds(int maxTasksPerJob, int maxUpdateInstanceFailures) {
    this.maxTasksPerJob = maxTasksPerJob;
    this.maxUpdateInstanceFailures = maxUpdateInstanceFailures;
  }

  public int getMaxTasksPerJob() {
    return maxTasksPerJob;
  }

  public int getMaxUpdateInstanceFailures() {
    return maxUpdateInstanceFailures;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("maxTasksPerJob", maxTasksPerJob)
        .add("maxUpdateInstanceFailures", maxUpdateInstanceFailures)
        .toString();
  }
}
