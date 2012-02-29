package com.twitter.mesos.scheduler.storage;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;

/**
 * Storage interface for host attributes.
 */
public interface AttributeStore {

  /**
   * Deletes all attributes in the store.
   */
  void deleteHostAttributes();

  /**
   * Save a host attribute in the attribute store.
   *
   * @param hostAttributes The attribute we are going to save.
   */
  void saveHostAttributes(HostAttributes hostAttributes);

  /**
   * Fetches all host attributes given by the host.
   *
   * @param host the host we want to get attributes from.
   * @return return the host attributes that belong to the given host.
   */
  Iterable<Attribute> getHostAttributes(String host);

  /**
   * Fetches all attributes in the store.
   *
   * @return All host attributes.
   */
  Set<HostAttributes> getHostAttributes();

  class AttributeStoreImpl implements AttributeStore {
    private final Map<String, HostAttributes> hostAttributes;

    @Inject
    public AttributeStoreImpl() {
      this.hostAttributes = Maps.newHashMap();
    }

    @Override
    public synchronized void deleteHostAttributes() {
      hostAttributes.clear();
    }

    @Override
    public synchronized void saveHostAttributes(HostAttributes attributes) {
      hostAttributes.put(attributes.getHost(), attributes);
    }

    @Override
    public synchronized Iterable<Attribute> getHostAttributes(String host) {
      HostAttributes attributes = hostAttributes.get(host);
      return (attributes == null) ? ImmutableList.<Attribute>of() : attributes.getAttributes();
    }

    @Override
    public Set<HostAttributes> getHostAttributes() {
      return ImmutableSet.copyOf(hostAttributes.values());
    }
  }
}

