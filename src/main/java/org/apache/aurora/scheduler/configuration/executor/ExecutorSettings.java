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
package org.apache.aurora.scheduler.configuration.executor;

import java.util.Objects;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.resources.ResourceSlot;
import org.apache.aurora.scheduler.resources.ResourceType;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public class ExecutorSettings {
  private final ExecutorConfig config;
  private final boolean populateDiscoveryInfo;

  public ExecutorSettings(ExecutorConfig config, boolean populateDiscoveryInfo) {
    this.config = requireNonNull(config);
    this.populateDiscoveryInfo = populateDiscoveryInfo;
  }

  public ExecutorConfig getExecutorConfig() {
    // TODO(wfarner): Replace this with a generic name-based accessor once tasks can specify the
    // executor they wish to use.
    return config;
  }

  public boolean shouldPopulateDiscoverInfo() {
    return populateDiscoveryInfo;
  }

  private double getExecutorResourceValue(ResourceType resource) {
    return config.getExecutor().getResourcesList().stream()
        .filter(r -> r.getName().equals(resource.getName()))
        .findFirst()
        .map(r -> r.getScalar().getValue())
        .orElse(0D);
  }

  public ResourceSlot getExecutorOverhead() {
    return new ResourceSlot(
        getExecutorResourceValue(ResourceType.CPUS),
        Amount.of((long) getExecutorResourceValue(ResourceType.RAM_MB), Data.MB),
        Amount.of((long) getExecutorResourceValue(ResourceType.DISK_MB), Data.MB),
        0);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ExecutorSettings)) {
      return false;
    }

    ExecutorSettings other = (ExecutorSettings) obj;
    return Objects.equals(config, other.config);
  }
}
