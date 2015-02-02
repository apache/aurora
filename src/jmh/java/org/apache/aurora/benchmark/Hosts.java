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
package org.apache.aurora.benchmark;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

/**
 * Host attribute builder.
 */
final class Hosts {
  private Hosts() {
    // Utility class.
  }

  /**
   * Builds host attributes for the specified configuration.
   */
  static final class Builder {
    private static final String SLAVE_ID_FORMAT = "slave-%s";
    private static final String HOST_NAME_FORMAT = "host-%s";
    private static final String RACK_NAME_FORMAT = "rack-%s";
    private int hostsPerRack = 1;

    Builder setNumHostsPerRack(int newHostsPerRack) {
      hostsPerRack = newHostsPerRack;
      return this;
    }

    Set<IHostAttributes> build(int count) {
      ImmutableSet.Builder<IHostAttributes> attributes = ImmutableSet.builder();
      int rackIndex = 0;
      for (int i = 0; i < count; i++) {
        attributes.add(IHostAttributes.build(new HostAttributes()
            .setHost(String.format(HOST_NAME_FORMAT, i))
            .setSlaveId(String.format(SLAVE_ID_FORMAT, i))
            .setMode(MaintenanceMode.NONE)
            .setAttributes(ImmutableSet.of(
                new Attribute("rack", ImmutableSet.of(String.format(RACK_NAME_FORMAT, rackIndex))),
                new Attribute("host", ImmutableSet.of(String.format(HOST_NAME_FORMAT, i)))))));

        if (i % hostsPerRack == 0) {
          rackIndex++;
        }
      }
      return attributes.build();
    }
  }
}
