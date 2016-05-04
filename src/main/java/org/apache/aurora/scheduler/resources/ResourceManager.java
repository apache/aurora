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

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.Resource;

import static org.apache.aurora.scheduler.resources.ResourceType.fromResource;
import static org.apache.mesos.Protos.Offer;

/**
 * Manages resources and provides Aurora/Mesos translation.
 */
public final class ResourceManager {
  private ResourceManager() {
    // Utility class.
  }

  /**
   * Gets offer resources matching specified {@link ResourceType}.
   *
   * @param offer Offer to get resources from.
   * @param type {@link ResourceType} to filter resources by.
   * @return Offer resources matching {@link ResourceType}.
   */
  public static Iterable<Resource> getOfferResources(Offer offer, ResourceType type) {
    return Iterables.filter(offer.getResourcesList(), r -> fromResource(r).equals(type));
  }

  /**
   * Same as {@link #getTaskResources(ITaskConfig, ResourceType)}.
   *
   * @param task Scheduled task to get resources from.
   * @param type {@link ResourceType} to filter resources by.
   * @return Task resources matching {@link ResourceType}.
   */
  public static Iterable<IResource> getTaskResources(IScheduledTask task, ResourceType type) {
    return getTaskResources(task.getAssignedTask().getTask(), type);
  }

  /**
   * Gets task resources matching specified {@link ResourceType}.
   *
   * @param task Task config to get resources from.
   * @param type {@link ResourceType} to filter resources by.
   * @return Task resources matching {@link ResourceType}.
   */
  public static Iterable<IResource> getTaskResources(ITaskConfig task, ResourceType type) {
    return Iterables.filter(task.getResources(), r -> fromResource(r).equals(type));
  }

  /**
   * Gets unique task resource types.
   *
   * @param task Task to get resource types from.
   * @return Set of {@link ResourceType} instances representing task resources.
   */
  public static Set<ResourceType> getTaskResourceTypes(IScheduledTask task) {
    return EnumSet.copyOf(task.getAssignedTask().getTask().getResources().stream()
        .map(r -> fromResource(r))
        .collect(Collectors.toSet()));
  }

  /**
   * Gets the quantity of the Mesos resource specified by {@code type}.
   *
   * @param resources Mesos resources.
   * @param type Type of resource to quantify.
   * @return Mesos resource value.
   */
  public static Double quantityOf(Iterable<Resource> resources, ResourceType type) {
    return StreamSupport.stream(resources.spliterator(), false)
        .filter(r -> fromResource(r).equals(type))
        .map(r -> fromResource(r).getMesosResourceConverter().quantify(r))
        .reduce((l, r) -> l + r).orElse(0.0);
  }
}
