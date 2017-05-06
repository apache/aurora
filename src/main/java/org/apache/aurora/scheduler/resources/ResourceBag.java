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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import static java.util.stream.Collectors.toMap;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * A bag of unique resource values aggregated by {@link ResourceType}.
 */
public class ResourceBag {
  public static final ResourceBag EMPTY = new ResourceBag(ImmutableMap.of(
      CPUS, 0.0,
      RAM_MB, 0.0,
      DISK_MB, 0.0
  ));

  public static final ResourceBag SMALL = new ResourceBag(ImmutableMap.of(
      CPUS, 1.0,
      RAM_MB, 1024.0,
      DISK_MB, 4096.0
  ));

  public static final ResourceBag MEDIUM = new ResourceBag(ImmutableMap.of(
      CPUS, 4.0,
      RAM_MB, 8192.0,
      DISK_MB, 16384.0
  ));

  public static final ResourceBag LARGE = new ResourceBag(ImmutableMap.of(
      CPUS, 8.0,
      RAM_MB, 16384.0,
      DISK_MB, 32768.0
  ));

  public static final ResourceBag XLARGE = new ResourceBag(ImmutableMap.of(
      CPUS, 16.0,
      RAM_MB, 32768.0,
      DISK_MB, 65536.0
  ));

  public static final Predicate<Map.Entry<ResourceType, Double>> IS_NEGATIVE =
      entry -> entry.getValue() < 0;

  public static final Predicate<Map.Entry<ResourceType, Double>> IS_POSITIVE =
      entry -> entry.getValue() > 0;

  public static final Predicate<Map.Entry<ResourceType, Double>> IS_MESOS_REVOCABLE =
      entry -> entry.getKey().isMesosRevocable();

  private final Map<ResourceType, Double> resourceVectors;

  /**
   * Creates an instance of ResourceBag with given resource vectors (type -> value).
   *
   * @param resourceVectors Map of resource vectors.
   */
  ResourceBag(Map<ResourceType, Double> resourceVectors) {
    this.resourceVectors = ImmutableMap.copyOf(resourceVectors);
  }

  /**
   * Gets resource vectors in the bag.
   *
   * @return Map of resource vectors.
   */
  public Map<ResourceType, Double> getResourceVectors() {
    return resourceVectors;
  }

  /**
   * Convenience function to return a stream of resource vectors.
   *
   * @return A stream of resource vectors.
   */
  public Stream<Map.Entry<ResourceType, Double>> streamResourceVectors() {
    return resourceVectors.entrySet().stream();
  }

  /**
   * Gets the value of resource specified by {@code type} or 0.0.
   *
   * @param type Resource type to get value for.
   * @return Resource value or 0.0 if no mapping for {@code type} is found.
   */
  public Double valueOf(ResourceType type) {
    return resourceVectors.getOrDefault(type, 0.0);
  }

  /**
   * Adds this and other bag contents.
   *
   * @param other Other bag to add.
   * @return Result of addition.
   */
  public ResourceBag add(ResourceBag other) {
    return binaryOp(other, (l, r) -> l + r);
  }

  /**
   * Subtracts other bag contents from this.
   *
   * @param other Other bag to subtract.
   * @return Result of subtraction.
   */
  public ResourceBag subtract(ResourceBag other) {
    return binaryOp(other, (l, r) -> l - r);
  }

  /**
   * Divides this by other bag contents.
   * <p>
   * Note: any missing or zero resource values in {@code other} will result in
   * {@code java.lang.Double.POSITIVE_INFINITY}.
   * @param other Other bag to divide by.
   * @return Result of division.
   */
  public ResourceBag divide(ResourceBag other) {
    return binaryOp(other, (l, r) -> l / r);
  }

  /**
   * Applies {@code Math.max()} for each matching resource vector.
   *
   * @param other Other bag to compare with.
   * @return A new bag with max resource vectors.
   */
  public ResourceBag max(ResourceBag other) {
    return binaryOp(other, (l, r) -> Math.max(l, r));
  }

  /**
   * Scales each resource vector by {@code m}.
   *
   * @param m Scale factor.
   * @return Result of scale operation.
   */
  public ResourceBag scale(int m) {
    return new ResourceBag(resourceVectors.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, v -> v.getValue() * m)));
  }

  /**
   * Filters bag resources by {@code predicate}.
   *
   * @param predicate Predicate to filter by.
   * @return A new bag with resources filtered by {@code predicate}.
   */
  public ResourceBag filter(Predicate<Map.Entry<ResourceType, Double>> predicate) {
    return new ResourceBag(resourceVectors.entrySet().stream()
        .filter(predicate)
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /**
   * Verifies whether another bag would be able to fit into this one.
   *
   * @param other Other bag to try and fit.
   * @return Whether or not the bag fits.
   */
  public boolean greaterThanOrEqualTo(ResourceBag other) {
    // Subtract the subject and check if any of the resources have a negative value.
    return subtract(other).filter(IS_NEGATIVE).getResourceVectors().isEmpty();
  }

  /**
   * Applies {@code operator} to this and {@code other} resource values. Any missing resource type
   * values on either side will get substituted with 0.0 before applying the operator.
   *
   * @param other Other ResourceBag.
   * @param operator Operator to apply.
   * @return Operation result.
   */
  private ResourceBag binaryOp(ResourceBag other, BinaryOperator<Double> operator) {
    ImmutableMap.Builder<ResourceType, Double> builder = ImmutableMap.builder();
    Set<ResourceType> resourceTypes =
        Sets.union(resourceVectors.keySet(), other.getResourceVectors().keySet());
    for (ResourceType type : resourceTypes) {
      Double left = resourceVectors.getOrDefault(type, 0.0);
      Double right = other.getResourceVectors().getOrDefault(type, 0.0);
      builder.put(type, operator.apply(left, right));
    }

    return new ResourceBag(builder.build());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ResourceBag)) {
      return false;
    }

    ResourceBag other = (ResourceBag) o;
    return Objects.equals(resourceVectors, other.resourceVectors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceVectors);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("resourceVectors", resourceVectors).toString();
  }
}
