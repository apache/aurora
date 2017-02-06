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

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Resource;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import static org.apache.aurora.scheduler.resources.ResourceManager.getOfferResources;

/**
 * Allocate resources from an accepted Mesos Offer to TaskInfo and ExecutorInfo.
 */
public final class AcceptedOffer {
  /**
   * Reserved resource filter.
   */
  @VisibleForTesting
  static final Predicate<Resource> RESERVED = e -> e.hasRole() && !e.getRole().equals("*");

  /**
   * Get proper value for {@link org.apache.mesos.Protos.TaskInfo}'s resources.
   * @return A list of Resource used for TaskInfo.
   */
  public Iterable<Resource> getTaskResources() {
    return taskResources;
  }

  /**
   * Get proper value for {@link org.apache.mesos.Protos.ExecutorInfo}'s resources.
   * @return A list of Resource used for ExecutorInfo.
   */
  public Iterable<Resource> getExecutorResources() {
    return executorResources;
  }

  private final Iterable<Resource> taskResources;
  private final Iterable<Resource> executorResources;

  public static AcceptedOffer create(
      Offer offer,
      IAssignedTask task,
      ResourceBag executorOverhead,
      TierInfo tierInfo) throws ResourceManager.InsufficientResourcesException {

    ImmutableList.Builder<Resource> taskResources = ImmutableList.builder();
    ImmutableList.Builder<Resource> executorResources = ImmutableList.builder();

    ResourceManager.bagFromResources(task.getTask().getResources())
        .streamResourceVectors()
        .forEach(entry -> {
          ResourceType type = entry.getKey();
          Iterable<Resource.Builder> offerResources = StreamSupport
              .stream(getOfferResources(offer, tierInfo, entry.getKey()).spliterator(), false)
              // Note the reverse order of args in .compare(): we want RESERVED resources first.
              .sorted((l, r) -> Boolean.compare(RESERVED.test(r), RESERVED.test(l)))
              .map(Resource::toBuilder)
              .collect(toList());

          boolean isRevocable = type.isMesosRevocable() && tierInfo.isRevocable();

          taskResources.addAll(type.getMesosResourceConverter().toMesosResource(
              offerResources,
              type.getMapper().isPresent()
                  ? () -> type.getMapper().get().getAssigned(task)
                  : () -> entry.getValue(),
              isRevocable));

          if (executorOverhead.getResourceVectors().containsKey(type)) {
            executorResources.addAll(type.getMesosResourceConverter().toMesosResource(
                offerResources,
                () -> executorOverhead.getResourceVectors().get(type),
                isRevocable));
          }
        });

    return new AcceptedOffer(taskResources.build(), executorResources.build());
  }

  private AcceptedOffer(
      List<Resource> taskResources,
      List<Resource> executorResources) {

    this.taskResources = requireNonNull(taskResources);
    this.executorResources = requireNonNull(executorResources);
  }
}
