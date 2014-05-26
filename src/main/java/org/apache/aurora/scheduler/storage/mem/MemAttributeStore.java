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

/**
 * An in-memory attribute store.
 */
class MemAttributeStore implements Mutable {
  private final ConcurrentMap<String, HostAttributes> hostAttributes = Maps.newConcurrentMap();

  @Override
  public void deleteHostAttributes() {
    hostAttributes.clear();
  }

  @Override
  public void saveHostAttributes(HostAttributes attributes) {
    hostAttributes.putIfAbsent(attributes.getHost(), attributes);

    HostAttributes stored = hostAttributes.get(attributes.getHost());
    if (!stored.isSetMode()) {
      stored.setMode(attributes.isSetMode() ? attributes.getMode() : MaintenanceMode.NONE);
    }
    stored.setAttributes(attributes.isSetAttributes()
        ? attributes.getAttributes() : ImmutableSet.<Attribute>of());
  }

  @Override
  public boolean setMaintenanceMode(String host, MaintenanceMode mode) {
    HostAttributes stored = hostAttributes.get(host);
    if (stored != null) {
      stored.setMode(mode);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Optional<HostAttributes> getHostAttributes(String host) {
    return Optional.fromNullable(hostAttributes.get(host));
  }

  @Override
  public Set<HostAttributes> getHostAttributes() {
    return ImmutableSet.copyOf(hostAttributes.values());
  }
}
