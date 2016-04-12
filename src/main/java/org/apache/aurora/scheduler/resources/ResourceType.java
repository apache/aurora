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
package org.apache.aurora.scheduler.resources;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;

import static java.util.Objects.requireNonNull;

/**
 * Describes Mesos resource types and their Aurora traits.
 */
@VisibleForTesting
public enum ResourceType {
  /**
   * CPU resource.
   */
  CPUS("cpus", "CPU", 16),

  /**
   * RAM resource.
   */
  RAM_MB("mem", "RAM", Amount.of(24, Data.GB).as(Data.MB)),

  /**
   * DISK resource.
   */
  DISK_MB("disk", "disk", Amount.of(450, Data.GB).as(Data.MB)),

  /**
   * Port resource.
   */
  PORTS("ports", "ports", 1000);

  /**
   * Mesos resource name.
   */
  private final String mesosName;

  /**
   * Aurora resource name.
   */
  private final String auroraName;

  /**
   * Scaling range to use for comparison of scheduling vetoes. This has no real bearing besides
   * trying to determine if a veto along one resource vector is a 'stronger' veto than that of
   * another vector. The value represents the typical slave machine resources.
   */
  private final int scalingRange;

  /**
   * Describes a Resource type.
   *
   * @param mesosName See {@link #getMesosName()} for more details.
   * @param auroraName See {@link #getAuroraName()} for more details.
   * @param scalingRange See {@link #getScalingRange()} for more details.
   */
  ResourceType(String mesosName, String auroraName, int scalingRange) {
    this.mesosName = requireNonNull(mesosName);
    this.auroraName = requireNonNull(auroraName);
    this.scalingRange = scalingRange;
  }

  /**
   * Gets Mesos resource name.
   * <p>
   * @see <a href="https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto/">Mesos
   * protobuf for more details</a>
   *
   * @return Mesos resource name.
   */
  public String getMesosName() {
    return mesosName;
  }

  /**
   * Gets resource name for internal Aurora representation (e.g. in the UI).
   *
   * @return Aurora resource name.
   */
  public String getAuroraName() {
    return auroraName;
  }

  /**
   * Returns scaling range for comparing scheduling vetoes.
   *
   * @return Resource scaling range.
   */
  public int getScalingRange() {
    return scalingRange;
  }
}
