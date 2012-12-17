package com.twitter.mesos.scheduler.storage;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;

/**
 * Storage interface for host attributes.
 */
public interface AttributeStore {
  /**
   * Fetches all host attributes given by the host.
   *
   * @param host host name.
   * @return attributes associated with {@code host}, if the host is known.
   */
  Optional<HostAttributes> getHostAttributes(String host);

  /**
   * Fetches all attributes in the store.
   *
   * @return All host attributes.
   */
  Set<HostAttributes> getHostAttributes();

  /**
   * Attributes are considered mostly ephemeral and extremely low risk when inconsistency
   * is present.  As a result, this is the only store lacking a {@link Transactional} sub-interface.
   */
  public interface Mutable extends AttributeStore {

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
     * Adjusts the maintenance mode for a host.
     * No adjustment will be made if the host is unknown.
     *
     * @param host Host to adjust.
     * @param mode Mode to place the host in.
     * @return {@code true} if the host is known and the state was adjusted,
     *         {@code false} if the host is unrecognized.
     */
    boolean setMaintenanceMode(String host, MaintenanceMode mode);
  }

  public static final class Util {
    private Util() {
    }

    /**
     * Fetches attributes about a {@code host}.
     *
     * @param store Store to fetch host attributes from.
     * @param host Host to fetch attributes about.
     * @return Attributes associated with {@code host}, or an empty iterable if the host is
     *         unknown.
     */
    public static Iterable<Attribute> attributesOrNone(StoreProvider store, String host) {
      Optional<HostAttributes> attributes = store.getAttributeStore().getHostAttributes(host);
      return attributes.isPresent()
          ? attributes.get().getAttributes() : ImmutableList.<Attribute>of();
    }
  }

  class AttributeStoreImpl implements Mutable {
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
}
