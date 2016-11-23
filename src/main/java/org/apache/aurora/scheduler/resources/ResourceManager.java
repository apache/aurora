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
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.log.ThriftBackfill;
import org.apache.mesos.Protos.Resource;

import static org.apache.aurora.scheduler.resources.ResourceType.BY_MESOS_NAME;
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
   * TODO(maxim): reduce visibility by redirecting callers to #getRevocableOfferResources().
   */
  public static final Predicate<Resource> REVOCABLE =
      r -> !fromResource(r).isMesosRevocable() || r.hasRevocable();

  /**
   * TODO(maxim): reduce visibility by redirecting callers to #getNonRevocableOfferResources().
   */
  public static final Predicate<Resource> NON_REVOCABLE = r -> !r.hasRevocable();

  private static final Function<IResource, ResourceType> RESOURCE_TO_TYPE = r -> fromResource(r);

  private static final Function<Resource, ResourceType> MESOS_RESOURCE_TO_TYPE =
      r -> fromResource(r);

  private static final Function<IResource, Double> QUANTIFY_RESOURCE =
      r -> fromResource(r).getAuroraResourceConverter().quantify(r.getRawValue());

  private static final Function<Resource, Double> QUANTIFY_MESOS_RESOURCE =
      r -> fromResource(r).getMesosResourceConverter().quantify(r);

  private static final BinaryOperator<Double> REDUCE_VALUES = (l, r) -> l + r;

  /**
   * TODO(rdelvalle): Remove filters when arbitrary resources are fully supported (AURORA-1328).
   */
  private static final Predicate<Resource> SUPPORTED_RESOURCE =
      r -> BY_MESOS_NAME.containsKey(r.getName());

  /**
   * Gets offer resources matching specified {@link ResourceType}.
   *
   * @param offer Offer to get resources from.
   * @param type {@link ResourceType} to filter resources by.
   * @return Offer resources matching {@link ResourceType}.
   */
  public static Iterable<Resource> getOfferResources(Offer offer, ResourceType type) {
    return Iterables.filter(
        Iterables.filter(offer.getResourcesList(), SUPPORTED_RESOURCE),
        r -> fromResource(r).equals(type));
  }

  /**
   * Gets Mesos-revocable offer resources.
   *
   * @param offer Offer to get resources from.
   * @return Mesos-revocable offer resources.
   */
  public static Iterable<Resource> getRevocableOfferResources(Offer offer) {
    return Iterables.filter(
        offer.getResourcesList(),
        Predicates.and(SUPPORTED_RESOURCE, REVOCABLE));
  }

  /**
   * Gets non-Mesos-revocable offer resources.
   *
   * @param offer Offer to get resources from.
   * @return Non-Mesos-revocable offer resources.
   */
  public static Iterable<Resource> getNonRevocableOfferResources(Offer offer) {
    return Iterables.filter(
        offer.getResourcesList(),
        Predicates.and(SUPPORTED_RESOURCE, NON_REVOCABLE));
  }

  /**
   * Gets offer resources filtered by the provided {@code tierInfo} instance.
   *
   * @param offer Offer to get resources from.
   * @param tierInfo Tier info.
   * @return Offer resources filtered by {@code tierInfo}.
   */
  public static Iterable<Resource> getOfferResources(Offer offer, TierInfo tierInfo) {
    return tierInfo.isRevocable()
        ? getRevocableOfferResources(offer)
        : getNonRevocableOfferResources(offer);
  }

  /**
   * Gets offer resoruces filtered by the {@code tierInfo} and {@code type}.
   *
   * @param offer Offer to get resources from.
   * @param tierInfo Tier info.
   * @param type Resource type.
   * @return Offer resources filtered by {@code tierInfo} and {@code type}.
   */
  public static Iterable<Resource> getOfferResources(
      Offer offer,
      TierInfo tierInfo,
      ResourceType type) {

    return Iterables.filter(getOfferResources(offer, tierInfo), r -> fromResource(r).equals(type));
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
   * Gets task resources matching any of the specified resource types.
   *
   * @param task Task config to get resources from.
   * @param typesToMatch EnumSet of resource types.
   * @return Task resources matching any of the resource types.
   */
  public static Iterable<IResource> getTaskResources(
      ITaskConfig task,
      EnumSet<ResourceType> typesToMatch) {

    return Iterables.filter(task.getResources(), r -> typesToMatch.contains(fromResource(r)));
  }

  /**
   * Gets unique task resource types.
   *
   * @param task Task to get resource types from.
   * @return Set of {@link ResourceType} instances representing task resources.
   */
  public static Set<ResourceType> getTaskResourceTypes(IAssignedTask task) {
    Set<ResourceType> types = task.getTask().getResources().stream()
        .map(RESOURCE_TO_TYPE)
        .collect(Collectors.toSet());
    return types.isEmpty() ? types : EnumSet.copyOf(types);
  }

  /**
   * Gets the quantity of the Mesos resource specified by {@code type}.
   *
   * @param resources Mesos resources.
   * @param type Type of resource to quantify.
   * @return Aggregate Mesos resource value.
   */
  public static Double quantityOfMesosResource(Iterable<Resource> resources, ResourceType type) {
    return StreamSupport.stream(resources.spliterator(), false)
        .filter(r -> SUPPORTED_RESOURCE.apply(r))
        .filter(r -> fromResource(r).equals(type))
        .map(QUANTIFY_MESOS_RESOURCE)
        .reduce(REDUCE_VALUES)
        .orElse(0.0);
  }

  /**
   * Gets the quantity of resource specified by {@code type}.
   *
   * @param resources Resources.
   * @param type Type of resource to quantify.
   * @return Aggregate resource value.
   */
  public static Double quantityOf(Iterable<IResource> resources, ResourceType type) {
    return quantityOf(StreamSupport.stream(resources.spliterator(), false)
        .filter(r -> fromResource(r).equals(type))
        .collect(Collectors.toList()));
  }

  /**
   * Gets the quantity of resources. Caller to ensure all resources are of the same type.
   *
   * @param resources Resources to sum up.
   * @return Aggregate resource value.
   */
  public static Double quantityOf(Iterable<IResource> resources) {
    return StreamSupport.stream(resources.spliterator(), false)
        .map(QUANTIFY_RESOURCE)
        .reduce(REDUCE_VALUES)
        .orElse(0.0);
  }

  /**
   * Creates a {@link ResourceBag} from resources.
   *
   * @param resources Resources to convert.
   * @return A {@link ResourceBag} instance.
   */
  public static ResourceBag bagFromResources(Iterable<IResource> resources) {
    return bagFromResources(resources, RESOURCE_TO_TYPE, QUANTIFY_RESOURCE);
  }

  /**
   * Creates a {@link ResourceBag} from Mesos resources.
   *
   * @param resources Mesos resources to convert.
   * @return A {@link ResourceBag} instance.
   */
  public static ResourceBag bagFromMesosResources(Iterable<Resource> resources) {
    return bagFromResources(
        Iterables.filter(resources, SUPPORTED_RESOURCE),
        MESOS_RESOURCE_TO_TYPE,
        QUANTIFY_MESOS_RESOURCE);
  }

  /**
   * Creates a {@link ResourceBag} from {@link IResourceAggregate}.
   *
   * @param aggregate {@link IResourceAggregate} to convert.
   * @return A {@link ResourceBag} instance.
   */
  public static ResourceBag bagFromAggregate(IResourceAggregate aggregate) {
    return new ResourceBag(aggregate.getResources().stream()
        .collect(Collectors.toMap(RESOURCE_TO_TYPE, QUANTIFY_RESOURCE)));
  }

  /**
   * Creates a {@link IResourceAggregate} from {@link ResourceBag}.
   *
   * @param bag {@link ResourceBag} to convert.
   * @return A {@link IResourceAggregate} instance.
   */
  public static IResourceAggregate aggregateFromBag(ResourceBag bag) {
    return ThriftBackfill.backfillResourceAggregate(new ResourceAggregate()
        .setResources(bag.streamResourceVectors()
            .map(e -> IResource.newBuilder(
                e.getKey().getValue(),
                e.getKey().getAuroraResourceConverter().valueOf(e.getValue())))
            .collect(Collectors.toSet())));
  }

  private static <T> ResourceBag bagFromResources(
      Iterable<T> resources,
      Function<T, ResourceType> typeMapper,
      Function<T, Double> valueMapper) {

    return new ResourceBag(StreamSupport.stream(resources.spliterator(), false)
        .collect(Collectors.groupingBy(typeMapper))
        .entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            group -> group.getValue().stream()
                .map(valueMapper)
                .reduce(REDUCE_VALUES)
                .orElse(0.0))));
  }

  /**
   * Thrown when there are insufficient resources to satisfy a request.
   */
  public static class InsufficientResourcesException extends RuntimeException {
    InsufficientResourcesException(String message) {
      super(message);
    }
  }

}
