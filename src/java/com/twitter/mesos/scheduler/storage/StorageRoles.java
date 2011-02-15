package com.twitter.mesos.scheduler.storage;

import com.google.common.base.Preconditions;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;

import java.io.Serializable;
import java.lang.annotation.Annotation;

/**
 * Utility methods for working with {@literal StorageRole}s.
 *
 * @author jsirois
 */
public class StorageRoles {

  static class StorageRoleImpl implements StorageRole, Serializable {
    private static final long serialVersionUID = 0;

    private final Role role;

    StorageRoleImpl(Role role) {
      this.role = Preconditions.checkNotNull(role);
    }

    @Override
    public Role value() {
      return role;
    }

    public int hashCode() {
      // This is specified in java.lang.Annotation.
      return (127 * "value".hashCode()) ^ role.hashCode();
    }

    public boolean equals(Object o) {
      if (!(o instanceof StorageRole)) {
        return false;
      }

      StorageRole other = (StorageRole) o;
      return role.equals(other.value());
    }

    public String toString() {
      return "@" + StorageRole.class.getName() + "(value=" + role + ")";
    }

    public Class<? extends Annotation> annotationType() {
      return StorageRole.class;
    }
  }

  /**
   * Creates a {@code StorageRole} annotation with {@code storageRole} as the value.
   *
   * @param storageRole The role value for the StorageRole annotation
   * @return A {@code StorageRole} annotation implementation with the specified role.
   */
  public static StorageRole forRole(Role storageRole) {
    return new StorageRoleImpl(storageRole);
  }
}
