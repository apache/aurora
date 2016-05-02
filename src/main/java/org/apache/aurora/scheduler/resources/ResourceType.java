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
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.gen.Resource._Fields;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.mesos.Protos;
import org.apache.thrift.TEnum;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.quantity.Data.GB;
import static org.apache.aurora.common.quantity.Data.MB;
import static org.apache.aurora.scheduler.resources.ResourceMapper.PORT_MAPPER;
import static org.apache.aurora.scheduler.resources.ResourceTypeConverter.DOUBLE;
import static org.apache.aurora.scheduler.resources.ResourceTypeConverter.LONG;
import static org.apache.aurora.scheduler.resources.ResourceTypeConverter.STRING;
import static org.apache.mesos.Protos.Value.Type.RANGES;
import static org.apache.mesos.Protos.Value.Type.SCALAR;

/**
 * Describes Mesos resource types and their Aurora traits.
 */
@VisibleForTesting
public enum ResourceType implements TEnum {
  /**
   * CPU resource.
   */
  CPUS(_Fields.NUM_CPUS, SCALAR, "cpus", DOUBLE, Optional.empty(), "CPU", 16, false),

  /**
   * RAM resource.
   */
  RAM_MB(
      _Fields.RAM_MB,
      SCALAR,
      "mem",
      LONG,
      Optional.empty(),
      "RAM",
      Amount.of(24, GB).as(MB),
      false),

  /**
   * DISK resource.
   */
  DISK_MB(
      _Fields.DISK_MB,
      SCALAR,
      "disk",
      LONG,
      Optional.empty(),
      "disk",
      Amount.of(450, GB).as(MB),
      false),

  /**
   * Port resource.
   */
  PORTS(_Fields.NAMED_PORT, RANGES, "ports", STRING, Optional.of(PORT_MAPPER), "ports", 1000, true);

  /**
   * Correspondent thrift {@link org.apache.aurora.gen.Resource} enum value.
   */
  private final _Fields value;

  /**
   * Mesos resource type.
   */
  private final Protos.Value.Type mesosType;

  /**
   * Mesos resource name.
   */
  private final String mesosName;

  /**
   * Type converter for resource values.
   */
  private final ResourceTypeConverter<?> typeConverter;

  /**
   * Optional resource mapper to use.
   */
  private final Optional<ResourceMapper> mapper;

  /**
   * Aurora resource name.
   */
  private final String auroraName;

  /**
   * Scaling range for comparing scheduling vetoes.
   */
  private final int scalingRange;

  /**
   * Indicates if multiple resource types are allowed in a task.
   */
  private final boolean isMultipleAllowed;

  private static ImmutableMap<Integer, ResourceType> byField =
      Maps.uniqueIndex(EnumSet.allOf(ResourceType.class),  ResourceType::getValue);

  /**
   * Describes a Resource type.
   *
   * @param value Correspondent {@link _Fields} value.
   * @param mesosType See {@link #getMesosType()} for more details.
   * @param mesosName See {@link #getMesosName()} for more details.
   * @param typeConverter See {@link #getTypeConverter()} for more details.
   * @param mapper See {@link #getMapper()} for more details.
   * @param auroraName See {@link #getAuroraName()} for more details.
   * @param scalingRange See {@link #getScalingRange()} for more details.
   * @param isMultipleAllowed See {@link #isMultipleAllowed()} for more details.
   */
  ResourceType(
      _Fields value,
      Protos.Value.Type mesosType,
      String mesosName,
      ResourceTypeConverter<?> typeConverter,
      Optional<ResourceMapper> mapper,
      String auroraName,
      int scalingRange,
      boolean isMultipleAllowed) {

    this.value = value;
    this.mesosType = requireNonNull(mesosType);
    this.mesosName = requireNonNull(mesosName);
    this.typeConverter = requireNonNull(typeConverter);
    this.auroraName = requireNonNull(auroraName);
    this.mapper = requireNonNull(mapper);
    this.scalingRange = scalingRange;
    this.isMultipleAllowed = isMultipleAllowed;
  }

  /**
   * Get unique ID value.
   *
   * @return Enum ID.
   */
  @Override
  public int getValue() {
    return value.getThriftFieldId();
  }

  /**
   * Gets Mesos resource type.
   * <p>
   * @see <a href="https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto/">Mesos
   * protobuf for more details</a>
   *
   * @return Mesos resource type.
   */
  public Protos.Value.Type getMesosType() {
    return mesosType;
  }

  /**
   * Gets Mesos resource name.
   * <p>
   * @see <a href="https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto/">Mesos
   * protobuf for more details</a>
   *
   * @return Mesos resource name.
   */
  public String getMesosName() {
    return mesosName;
  }

  /**
   * Gets {@link ResourceTypeConverter} to convert resource values.
   *
   * @return {@link ResourceTypeConverter} instance.
   */
  public ResourceTypeConverter<?> getTypeConverter() {
    return typeConverter;
  }

  /**
   * Gets optional resource mapper. See {@link ResourceMapper} for more details.
   *
   * @return Optional ResourceMapper.
   */
  public Optional<ResourceMapper> getMapper() {
    return mapper;
  }

  /**
   * Gets resource name for internal Aurora representation (e.g. in the UI).
   *
   * @return Aurora resource name.
   */
  public String getAuroraName() {
    return auroraName;
  }

  /**
   * Scaling range to use for comparison of scheduling vetoes.
   * <p>
   * This has no real bearing besides trying to determine if a veto along one resource vector
   * is a 'stronger' veto than that of another vector. The value represents the typical slave
   * machine resources.
   *
   * @return Resource scaling range.
   */
  public int getScalingRange() {
    return scalingRange;
  }

  /**
   * Returns a flag indicating if multiple resource of the same type are allowed in a given task.
   *
   * @return True if multiple resources of the same type are allowed, false otherwise.
   */
  public boolean isMultipleAllowed() {
    return isMultipleAllowed;
  }

  /**
   * Returns a {@link ResourceType} for the given ID.
   *
   * @param value ID value to search by. See {@link #getValue()}.
   * @return {@link ResourceType}.
   */
  public static ResourceType fromIdValue(int value) {
    return requireNonNull(byField.get(value), "Unmapped value: " + value);
  }

  /**
   * Returns a {@link ResourceType} for the given resource.
   *
   * @param resource {@link IResource} to search by.
   * @return {@link ResourceType}.
   */
  public static ResourceType fromResource(IResource resource) {
    return requireNonNull(
        byField.get((int) resource.getSetField().getThriftFieldId()),
        "Unknown resource: " + resource);
  }
}
