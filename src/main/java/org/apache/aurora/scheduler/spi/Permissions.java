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
package org.apache.aurora.scheduler.spi;

import java.util.Optional;

import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Utilities for Aurora-specific Shiro permissions.
 */
public final class Permissions {
  private Permissions() {
    // Utility class.
  }

  /**
   * A permission representing an intended invocation of an RPC exposed by the Aurora scheduler API.
   *
   * Drop-in implementations of {@link org.apache.shiro.realm.Realm} that construct standard Shiro
   * {@link org.apache.shiro.authz.permission.WildcardPermission}s will work fine, but realms that
   * want to authorize or account access using type-safe Aurora-specific information can inspect
   * permission checks and add additional information if they are instances of this or its
   * public subclasses in the SPI.
   */
  public interface AuroraRpcPermission extends Permission {
    /**
     * The domain of the RPC permitted to be invoked.
     */
    Domain getDomain();

    /**
     * The name of the RPC permitted to be invoked.
     */
    String getRpc();
  }

  /**
   * Domain of a permitted RPC (the first part of a {@link WildcardPermission}).
   */
  public enum Domain {
    /**
     * RPCs on the {@link org.apache.aurora.gen.AuroraSchedulerManager} service.
     */
    THRIFT_AURORA_SCHEDULER_MANAGER("thrift.AuroraSchedulerManager"),

    /**
     * RPCs on the {@link org.apache.aurora.gen.AuroraAdmin} service.
     */
    THRIFT_AURORA_ADMIN("thrift.AuroraAdmin");

    private final String permissionPart;

    Domain(String permissionPart) {
      this.permissionPart = permissionPart;
    }

    /**
     * The String form of the permission part represented by this domain.
     */
    @Override
    public String toString() {
      return permissionPart;
    }

    /**
     * Get the {@link Domain} associated with a given permission part. Inverse of {@link #toString}.
     *
     * @param permissionPart The permission part representing the domain.
     * @return The domain represented by it, if one exists.
     */
    public static Optional<Domain> fromString(String permissionPart) {
      for (Domain domain : Domain.values()) {
        if (domain.permissionPart.equals(permissionPart)) {
          return Optional.of(domain);
        }
      }

      return Optional.empty();
    }
  }

  /**
   * A permission to invoke an RPC with any arguments.
   */
  public static final class UnscopedRpcPermission extends WildcardPermission
      implements AuroraRpcPermission {

    private final Domain domain;
    private final String rpc;

    UnscopedRpcPermission(Domain domain, String rpc) {
      this.domain = requireNonNull(domain);
      this.rpc = requireNonNull(rpc);
      setParts(String.format("%s:%s", domain, rpc));
    }

    @Override
    public Domain getDomain() {
      return domain;
    }

    @Override
    public String getRpc() {
      return rpc;
    }

    @Override
    public String toString() {
      return toStringHelper(this).add("domain", domain).add("rpc", rpc).toString();
    }
  }

  /**
   * Permission to invoke an RPC only with arguments scoped to a single job.
   */
  public static final class JobScopedRpcPermission extends WildcardPermission
      implements AuroraRpcPermission {

    private static final Domain DOMAIN = Domain.THRIFT_AURORA_SCHEDULER_MANAGER;

    private final String rpc;
    private final IJobKey permittedJob;

    JobScopedRpcPermission(String rpc, IJobKey permittedJob) {
      this.rpc = requireNonNull(rpc);
      this.permittedJob = requireNonNull(permittedJob);

      setParts(
          String.format("%s:%s:%s:%s:%s",
              DOMAIN,
              rpc,
              permittedJob.getRole(),
              permittedJob.getEnvironment(),
              permittedJob.getName()));
    }

    /**
     * The job permitted as an argument to the permitted RPC.
     */
    public IJobKey getPermittedJob() {
      return permittedJob;
    }

    @Override
    public Domain getDomain() {
      return DOMAIN;
    }

    @Override
    public String getRpc() {
      return rpc;
    }

    @Override
    public String toString() {
      return toStringHelper(this).add("rpc", rpc).add("permittedJob", permittedJob).toString();
    }
  }

  /**
   * Creates a permission permitting the given RPC to operate on a single given job.
   *
   * @param rpc The RPC permitted to be called.
   * @param targetJob The job permitted to be operated upon.
   * @return A permission permitting the given RPC to operate on the given job.
   */
  public static JobScopedRpcPermission createJobScopedPermission(String rpc, IJobKey targetJob) {
    return new JobScopedRpcPermission(rpc, targetJob);
  }

  /**
   * Creates a permission permitting invocation of the given RPC with any possible argument.
   *
   * @param domain The domain of the RPC.
   * @param rpc The RPC permitted to be called.
   * @return A permission permitting invocation of the given RPC for all arguments.
   */
  public static UnscopedRpcPermission createUnscopedPermission(Domain domain, String rpc) {
    return new UnscopedRpcPermission(domain, rpc);
  }
}
