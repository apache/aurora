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
package org.apache.aurora.common.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ZooKeeper-backed implementation of {@link ServerSet} and {@link DynamicHostSet}.
 */
public class ServerSetImpl implements ServerSet, DynamicHostSet<ServiceInstance> {
  private static final Logger LOG = LoggerFactory.getLogger(ServerSetImpl.class);

  private final ZooKeeperClient zkClient;
  private final Group group;
  private final Codec<ServiceInstance> codec;
  private final BackoffHelper backoffHelper;

  /**
   * Creates a new ServerSet using open ZooKeeper node ACLs.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param path the name-service path of the service to connect to
   */
  public ServerSetImpl(ZooKeeperClient zkClient, String path) {
    this(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, path);
  }

  /**
   * Creates a new ServerSet for the given service {@code path}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param acl the ACL to use for creating the persistent group path if it does not already exist
   * @param path the name-service path of the service to connect to
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Iterable<ACL> acl, String path) {
    this(zkClient, new Group(zkClient, acl, path), JSON_CODEC);
  }

  /**
   * Creates a new ServerSet using the given service {@code group}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param group the server group
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Group group) {
    this(zkClient, group, JSON_CODEC);
  }

  /**
   * Creates a new ServerSet using the given service {@code group} and a custom {@code codec}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param group the server group
   * @param codec a codec to use for serializing and de-serializing the ServiceInstance data to and
   *     from a byte array
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Group group, Codec<ServiceInstance> codec) {
    this.zkClient = checkNotNull(zkClient);
    this.group = checkNotNull(group);
    this.codec = checkNotNull(codec);

    // TODO(John Sirois): Inject the helper so that backoff strategy can be configurable.
    backoffHelper = new BackoffHelper();
  }

  @VisibleForTesting
  ZooKeeperClient getZkClient() {
    return zkClient;
  }

  @Override
  public EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints)
      throws Group.JoinException, InterruptedException {

    checkNotNull(endpoint);
    checkNotNull(additionalEndpoints);

    MemberStatus memberStatus = new MemberStatus(endpoint, additionalEndpoints);
    Supplier<byte[]> serviceInstanceSupplier = memberStatus::serializeServiceInstance;
    Group.Membership membership = group.join(serviceInstanceSupplier);

    return () -> memberStatus.leave(membership);
  }

  @Override
  public Command watch(HostChangeMonitor<ServiceInstance> monitor) throws MonitorException {
    ServerSetWatcher serverSetWatcher = new ServerSetWatcher(zkClient, monitor);
    try {
      return serverSetWatcher.watch();
    } catch (Group.WatchException e) {
      throw new MonitorException("ZooKeeper watch failed.", e);
    } catch (InterruptedException e) {
      throw new MonitorException("Interrupted while watching ZooKeeper.", e);
    }
  }

  private class MemberStatus {
    private final InetSocketAddress endpoint;
    private final Map<String, InetSocketAddress> additionalEndpoints;

    private MemberStatus(
        InetSocketAddress endpoint,
        Map<String, InetSocketAddress> additionalEndpoints) {

      this.endpoint = endpoint;
      this.additionalEndpoints = additionalEndpoints;
    }

    synchronized void leave(Group.Membership membership) throws UpdateException {
      try {
        membership.cancel();
      } catch (Group.JoinException e) {
        throw new UpdateException(
            "Failed to auto-cancel group membership on transition to DEAD status", e);
      }
    }

    byte[] serializeServiceInstance() {
      ServiceInstance serviceInstance = new ServiceInstance(
          ServerSets.toEndpoint(endpoint),
          Maps.transformValues(additionalEndpoints, ServerSets.TO_ENDPOINT),
          Status.ALIVE);

      LOG.debug("updating endpoint data to:\n\t" + serviceInstance);
      try {
        return ServerSets.serializeServiceInstance(serviceInstance, codec);
      } catch (IOException e) {
        throw new IllegalStateException("Unexpected problem serializing thrift struct " +
            serviceInstance + "to a byte[]", e);
      }
    }
  }

  private static class ServiceInstanceFetchException extends RuntimeException {
    ServiceInstanceFetchException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class ServiceInstanceDeletedException extends RuntimeException {
    ServiceInstanceDeletedException(String path) {
      super(path);
    }
  }

  private class ServerSetWatcher {
    private final ZooKeeperClient zkClient;
    private final HostChangeMonitor<ServiceInstance> monitor;
    @Nullable private ImmutableSet<ServiceInstance> serverSet;

    ServerSetWatcher(ZooKeeperClient zkClient, HostChangeMonitor<ServiceInstance> monitor) {
      this.zkClient = zkClient;
      this.monitor = monitor;
    }

    public Command watch() throws Group.WatchException, InterruptedException {
      Watcher onExpirationWatcher = zkClient.registerExpirationHandler(this::rebuildServerSet);

      try {
        return group.watch(this::notifyGroupChange);
      } catch (Group.WatchException e) {
        zkClient.unregister(onExpirationWatcher);
        throw e;
      } catch (InterruptedException e) {
        zkClient.unregister(onExpirationWatcher);
        throw e;
      }
    }

    private ServiceInstance getServiceInstance(final String nodePath) {
      try {
        return backoffHelper.doUntilResult(() -> {
          try {
            byte[] data = zkClient.get().getData(nodePath, false, null);
            return ServerSets.deserializeServiceInstance(data, codec);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceInstanceFetchException(
                "Interrupted updating service data for: " + nodePath, e);
          } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            LOG.warn("Temporary error trying to updating service data for: " + nodePath, e);
            return null;
          } catch (NoNodeException e) {
            invalidateNodePath(nodePath);
            throw new ServiceInstanceDeletedException(nodePath);
          } catch (KeeperException e) {
            if (zkClient.shouldRetry(e)) {
              LOG.warn("Temporary error trying to update service data for: " + nodePath, e);
              return null;
            } else {
              throw new ServiceInstanceFetchException(
                  "Failed to update service data for: " + nodePath, e);
            }
          } catch (IOException e) {
            throw new ServiceInstanceFetchException(
                "Failed to deserialize the ServiceInstance data for: " + nodePath, e);
          }
        });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ServiceInstanceFetchException(
            "Interrupted trying to update service data for: " + nodePath, e);
      }
    }

    private final LoadingCache<String, ServiceInstance> servicesByMemberId =
        CacheBuilder.newBuilder().build(new CacheLoader<String, ServiceInstance>() {
          @Override public ServiceInstance load(String memberId) {
            return getServiceInstance(group.getMemberPath(memberId));
          }
        });

    private void rebuildServerSet() {
      Set<String> memberIds = ImmutableSet.copyOf(servicesByMemberId.asMap().keySet());
      servicesByMemberId.invalidateAll();
      notifyGroupChange(memberIds);
    }

    private String invalidateNodePath(String deletedPath) {
      String memberId = group.getMemberId(deletedPath);
      servicesByMemberId.invalidate(memberId);
      return memberId;
    }

    private final Function<String, ServiceInstance> MAYBE_FETCH_NODE =
        memberId -> {
          // This get will trigger a fetch
          try {
            return servicesByMemberId.getUnchecked(memberId);
          } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof ServiceInstanceDeletedException)) {
              Throwables.propagateIfInstanceOf(cause, ServiceInstanceFetchException.class);
              throw new IllegalStateException(
                  "Unexpected error fetching member data for: " + memberId, e);
            }
            return null;
          }
        };

    private synchronized void notifyGroupChange(Iterable<String> memberIds) {
      ImmutableSet<String> newMemberIds = ImmutableSortedSet.copyOf(memberIds);
      Set<String> existingMemberIds = servicesByMemberId.asMap().keySet();

      // Ignore no-op state changes except for the 1st when we've seen no group yet.
      if ((serverSet == null) || !newMemberIds.equals(existingMemberIds)) {
        SetView<String> deletedMemberIds = Sets.difference(existingMemberIds, newMemberIds);
        // Implicit removal from servicesByMemberId.
        existingMemberIds.removeAll(ImmutableSet.copyOf(deletedMemberIds));

        Iterable<ServiceInstance> serviceInstances = Iterables.filter(
            Iterables.transform(newMemberIds, MAYBE_FETCH_NODE), Predicates.notNull());

        notifyServerSetChange(ImmutableSet.copyOf(serviceInstances));
      }
    }

    private void notifyServerSetChange(ImmutableSet<ServiceInstance> currentServerSet) {
      // ZK nodes may have changed if there was a session expiry for a server in the server set, but
      // if the server's status has not changed, we can skip any onChange updates.
      if (!currentServerSet.equals(serverSet)) {
        if (currentServerSet.isEmpty()) {
          LOG.warn("server set empty for path " + group.getPath());
        } else {
          if (serverSet == null) {
            LOG.info("received initial membership {}", currentServerSet);
          } else {
            logChange(currentServerSet);
          }
        }
        serverSet = currentServerSet;
        monitor.onChange(serverSet);
      }
    }

    private void logChange(ImmutableSet<ServiceInstance> newServerSet) {
      StringBuilder message = new StringBuilder("server set " + group.getPath() + " change: ");
      if (serverSet.size() != newServerSet.size()) {
        message.append("from ").append(serverSet.size())
            .append(" members to ").append(newServerSet.size());
      }

      Joiner joiner = Joiner.on("\n\t\t");

      SetView<ServiceInstance> left = Sets.difference(serverSet, newServerSet);
      if (!left.isEmpty()) {
        message.append("\n\tleft:\n\t\t").append(joiner.join(left));
      }

      SetView<ServiceInstance> joined = Sets.difference(newServerSet, serverSet);
      if (!joined.isEmpty()) {
        message.append("\n\tjoined:\n\t\t").append(joiner.join(joined));
      }

      LOG.info(message.toString());
    }
  }
}
