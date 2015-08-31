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
package org.apache.aurora.scheduler.preemptor;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import static java.util.Objects.requireNonNull;

/**
 * A set of tasks proposed for preemption on a given slave.
 */
class PreemptionProposal {
  private final Set<PreemptionVictim> victims;
  private final String slaveId;

  PreemptionProposal(ImmutableSet<PreemptionVictim> victims, String slaveId) {
    this.victims = requireNonNull(victims);
    this.slaveId = requireNonNull(slaveId);
  }

  Set<PreemptionVictim> getVictims() {
    return victims;
  }

  String getSlaveId() {
    return slaveId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PreemptionProposal)) {
      return false;
    }

    PreemptionProposal other = (PreemptionProposal) o;
    return Objects.equals(getVictims(), other.getVictims())
        && Objects.equals(getSlaveId(), other.getSlaveId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(victims, slaveId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("victims", getVictims())
        .add("slaveId", getSlaveId())
        .toString();
  }
}
