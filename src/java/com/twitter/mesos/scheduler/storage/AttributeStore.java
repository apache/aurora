package com.twitter.mesos.scheduler.storage;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;

/**
 * Storage interface for host attributes.
 */
public interface AttributeStore {

  /**
   * Save a host attribute in the attribute store.
   *
   * @param hostAttributes The attribute we are going to save.
   */
  void saveHostAttribute(HostAttributes hostAttributes);

  /**
   * Fetches all host attributes given by the host.
   *
   * @param host the host we want to get attributes from.
   * @return return the host attributes that belong to the given host.
   */
  Iterable<Attribute> getAttributeForHost(String host);

  class AttributeStoreImpl implements SnapshotStore<Set<HostAttributes>>, AttributeStore {
    private final Multimap<String, Attribute> hostAttributes;

    @Inject
    public AttributeStoreImpl() {
      this.hostAttributes = HashMultimap.create();
    }

    @Override
    synchronized public void saveHostAttribute(HostAttributes attributes) {
      // TODO(wfarner): If the attributes have changed, write to the log as well.
      hostAttributes.putAll(attributes.getHost(), attributes.getAttributes());
    }

    @Override
    synchronized public Iterable<Attribute> getAttributeForHost(String host) {
      return hostAttributes.get(host);
    }

    @Override
    synchronized public Set<HostAttributes> createSnapshot() {
      ImmutableSet.Builder<HostAttributes> builder = ImmutableSet.builder();
      for (Entry<String, Collection<Attribute>> entry : hostAttributes.asMap().entrySet()) {
        HostAttributes attributes = new HostAttributes();
        attributes.setHost(entry.getKey());
        attributes.setAttributes(Sets.newHashSet(entry.getValue()));
        builder.add(attributes);
      }
      return builder.build();
    }

    @Override
    synchronized public void applySnapshot(Set<HostAttributes> snapshot) {
      hostAttributes.clear();
      for (HostAttributes attribute : snapshot) {
        saveHostAttribute(attribute);
      }
    }
  }
}

