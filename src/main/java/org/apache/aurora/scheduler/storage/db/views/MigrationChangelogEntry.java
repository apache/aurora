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

import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;

import org.apache.ibatis.migration.Change;

public class MigrationChangelogEntry extends Change {
  private byte[] downgradeScript;

  public String getDowngradeScript() {
    return new String(downgradeScript, Charsets.UTF_8);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MigrationChangelogEntry)) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    MigrationChangelogEntry other = (MigrationChangelogEntry) o;
    return Arrays.equals(downgradeScript, other.downgradeScript);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), downgradeScript);
  }
}
