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
package org.apache.aurora.scheduler.resources;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.Offer;

import static java.util.stream.StreamSupport.stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.DiscreteDomain.integers;

import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;

/**
 * Maps requested (task) resources to available (offer) resources.
 */
public interface ResourceMapper<T> {

  /**
   * Maps task resources to offer resources and returns a new task with updated mapping.
   *
   * @param offer Offer with available resources.
   * @param task Task with requested resources.
   * @return A new task with updated mapping.
   */
  IAssignedTask mapAndAssign(Offer offer, IAssignedTask task);

  /**
   * Gets assigned resource values stored in {@code task}.
   *
   * @param task Task to get assigned resources from.
   * @return Assigned resource values.
   */
  T getAssigned(IAssignedTask task);

  PortMapper PORT_MAPPER = new PortMapper();

  class PortMapper implements ResourceMapper<Set<Integer>> {
    @Override
    public IAssignedTask mapAndAssign(Offer offer, IAssignedTask task) {
      List<Integer> availablePorts =
          stream(ResourceManager.getOfferResources(offer, PORTS).spliterator(), false)
              .flatMap(resource -> resource.getRanges().getRangeList().stream())
              .flatMap(range -> ContiguousSet.create(
                  Range.closed((int) range.getBegin(), (int) range.getEnd()),
                  integers()).stream())
              .collect(Collectors.toList());

      Collections.shuffle(availablePorts);

      List<String> requestedPorts =
          stream(ResourceManager.getTaskResources(task.getTask(), PORTS).spliterator(), false)
            .map(e -> e.getNamedPort())
            .collect(Collectors.toList());

      checkState(
          availablePorts.size() >= requestedPorts.size(),
          "Insufficient ports %d when matching %s",
          availablePorts.size(),
          task);

      Iterator<Integer> ports = availablePorts.iterator();
      Map<String, Integer> portMap =
          requestedPorts.stream().collect(Collectors.toMap(key -> key, value -> ports.next()));

      AssignedTask builder = task.newBuilder();
      builder.setAssignedPorts(ImmutableMap.copyOf(portMap));
      return IAssignedTask.build(builder);
    }

    @Override
    public Set<Integer> getAssigned(IAssignedTask task) {
      return ImmutableSet.copyOf(task.getAssignedPorts().values());
    }
  }
}
