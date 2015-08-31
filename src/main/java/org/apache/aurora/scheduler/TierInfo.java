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

import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

/**
 * Defines common task tier traits and behaviors.
 */
public class TierInfo {
  private final boolean revocable;

  @VisibleForTesting
  public TierInfo(boolean revocable) {
    this.revocable = revocable;
  }

  /**
   * Checks if this tier intends to run with Mesos revocable resource offers.
   *
   * @return {@code true} if this tier requires revocable resource offers, {@code false} otherwise.
   */
  public boolean isRevocable() {
    return revocable;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(revocable);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TierInfo)) {
      return false;
    }

    TierInfo other = (TierInfo) obj;
    return Objects.equals(revocable, other.revocable);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("revocable", revocable)
        .toString();
  }
}
