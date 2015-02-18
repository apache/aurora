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

import org.apache.aurora.gen.JobUpdateKey;

/**
 * A job update that should be pruned.
 */
public class PruneVictim {
  private long rowId;
  private JobUpdateKey update;

  public long getRowId() {
    return rowId;
  }

  public JobUpdateKey getUpdate() {
    return update;
  }

  public void setRowId(long rowId) {
    this.rowId = rowId;
  }

  public void setUpdate(JobUpdateKey update) {
    this.update = update;
  }
}
