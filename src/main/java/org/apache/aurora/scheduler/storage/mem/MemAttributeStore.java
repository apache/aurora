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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.AttributeStore.Mutable;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

/**
 * An in-memory attribute store.
 */
class MemAttributeStore implements Mutable {
  private final ConcurrentMap<String, IHostAttributes> hostAttributes = Maps.newConcurrentMap();

  @Override
  public void deleteHostAttributes() {
    hostAttributes.clear();
  }

  @Override
  public void saveHostAttributes(IHostAttributes attributes) {
    hostAttributes.putIfAbsent(attributes.getHost(), attributes);

    IHostAttributes stored = hostAttributes.get(attributes.getHost());
    HostAttributes updated = stored.newBuilder();
    if (!stored.isSetMode()) {
      updated.setMode(attributes.isSetMode() ? attributes.getMode() : MaintenanceMode.NONE);
    }
    updated.setAttributes(updated.isSetAttributes()
        ? updated.getAttributes() : ImmutableSet.<Attribute>of());
    hostAttributes.replace(attributes.getHost(), stored, IHostAttributes.build(updated));
  }

  @Override
  public boolean setMaintenanceMode(String host, MaintenanceMode mode) {
    IHostAttributes stored = hostAttributes.get(host);
    if (stored == null) {
      return false;
    } else {
      hostAttributes.replace(
          host,
          stored,
          IHostAttributes.build(stored.newBuilder().setMode(mode)));
      return true;
    }
  }

  @Override
  public Optional<IHostAttributes> getHostAttributes(String host) {
    return Optional.fromNullable(hostAttributes.get(host));
  }

  @Override
  public Set<IHostAttributes> getHostAttributes() {
    return ImmutableSet.copyOf(hostAttributes.values());
  }
}
