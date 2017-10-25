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

package org.apache.aurora.scheduler.storage;

import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateStatus;

public final class Util {

  private Util() {
    // Utility class.
  }

  /**
   * Gets the gauge name for the running count of {@link JobUpdateAction}s.
   *
   * @param action Update action.
   * @return Gauge name.
   */
  public static String jobUpdateActionStatName(JobUpdateAction action) {
    return "update_instance_transition_" + action;
  }

  /**
   * Gets the gauge name for the running count of {@link JobUpdateStatus}es.
   *
   * @param status Update status.
   * @return Gauge name.
   */
  public static String jobUpdateStatusStatName(JobUpdateStatus status) {
    return "update_transition_" + status;
  }
}
