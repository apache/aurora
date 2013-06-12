package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.scheduler.storage.AttributeStore.Mutable;

/**
 * An in-memory attribute store.
 */
class MemAttributeStore implements Mutable {
  private final Map<String, HostAttributes> hostAttributes = Maps.newHashMap();

  @Override
  public synchronized void deleteHostAttributes() {
    hostAttributes.clear();
  }

  @Override
  public synchronized void saveHostAttributes(HostAttributes attributes) {
    HostAttributes stored = hostAttributes.get(attributes.getHost());
    if (stored == null) {
      stored = attributes;
      hostAttributes.put(attributes.getHost(), attributes);
    }
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
  public synchronized Optional<HostAttributes> getHostAttributes(String host) {
    return Optional.fromNullable(hostAttributes.get(host));
  }

  @Override
  public Set<HostAttributes> getHostAttributes() {
    return ImmutableSet.copyOf(hostAttributes.values());
  }
}
