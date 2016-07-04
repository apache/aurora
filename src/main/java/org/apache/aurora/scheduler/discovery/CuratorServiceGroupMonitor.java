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
package org.apache.aurora.scheduler.discovery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.scheduler.app.SchedulerMain;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

class CuratorServiceGroupMonitor implements ServiceGroupMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

  private final PathChildrenCache groupCache;
  private final Predicate<String> memberSelector;
  private final Codec<ServiceInstance> codec;

  /**
   * Creates a {@code ServiceGroupMonitor} backed by Curator.
   *
   * Although this monitor can be queried at any time, it will not usefully reflect service group
   * membership until it is {@link #start() started}. When starting a monitor, it should be arranged
   * that the monitor is {@link #close() closed} when no longer needed.
   *
   * It's important to be able to pick out group member nodes amongst child nodes for group paths
   * that can contain mixed-content nodes. The given {@code memberSelector} should be able to
   * discriminate member nodes from non-member nodes given the node name.
   *
   * @param groupCache The cache of group nodes.
   * @param memberSelector A predicate that returns {@code true} for group node names that represent
   *                       group members.  Here the name is just the `basename` of the node's full
   *                       ZooKeeper path.
   * @param codec A codec that can be used to deserialize group member {@link ServiceInstance} data.
   */
  CuratorServiceGroupMonitor(
      PathChildrenCache groupCache,
      Predicate<String> memberSelector,
      Codec<ServiceInstance> codec) {

    this.groupCache = requireNonNull(groupCache);
    this.memberSelector = requireNonNull(memberSelector);
    this.codec = requireNonNull(codec);
  }

  @Override
  public void start() throws MonitorException {
    try {
      // NB: This blocks on an initial group population to emulate legacy ServerSetMonitor behavior;
      // asynchronous population is an option using NORMAL or POST_INITIALIZED_EVENT StartModes
      // though.
      groupCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new MonitorException("Failed to begin monitoring service group.", e);
    }
  }

  /**
   * The complement of {@link #start()}; stops service group monitoring activities.
   *
   * NB: This operation idempotent; a close can be safely called regardless of the current state of
   * this service group monitor and only if in a started state will action be taken; otherwise close
   * will no-op.
   *
   * @throws IOException if there is a problem stopping any of the service group monitoring
   *                     activities.
   */
  @Override
  public void close() throws IOException {
    groupCache.close();
  }

  @Override
  public ImmutableSet<ServiceInstance> get() {
    return groupCache.getCurrentData().stream()
        .filter(cd -> memberSelector.test(ZKPaths.getNodeFromPath(cd.getPath())))
        .map(this::extractServiceInstance)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(GuavaUtils.toImmutableSet());
  }

  private Optional<ServiceInstance> extractServiceInstance(ChildData data) {
    ByteArrayInputStream source = new ByteArrayInputStream(data.getData());
    try {
      return Optional.of(codec.deserialize(source));
    } catch (IOException e) {
      LOG.error("Failed to deserialize ServiceInstance from " + data, e);
      return Optional.empty();
    }
  }
}
