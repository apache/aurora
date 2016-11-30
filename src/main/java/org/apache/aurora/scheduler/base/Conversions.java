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
package org.apache.aurora.scheduler.base;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of utility functions to convert mesos protobuf types to internal thrift types.
 */
public final class Conversions {

  private static final Logger LOG = LoggerFactory.getLogger(Conversions.class);

  private Conversions() {
    // Utility class.
  }

  // Maps from mesos state to scheduler interface state.
  private static final Map<TaskState, ScheduleStatus> STATE_TRANSLATION =
      new ImmutableMap.Builder<TaskState, ScheduleStatus>()
          .put(TaskState.TASK_STARTING, ScheduleStatus.STARTING)
          .put(TaskState.TASK_STAGING, ScheduleStatus.STARTING)
          .put(TaskState.TASK_RUNNING, ScheduleStatus.RUNNING)
          .put(TaskState.TASK_FINISHED, ScheduleStatus.FINISHED)
          .put(TaskState.TASK_FAILED, ScheduleStatus.FAILED)
          // N.B. the executor does not currently send TASK_KILLING, nor do we opt in to the
          // framework capability to receive notifications of this state. We map TaskState.KILLING
          // to ScheduleStatus.KILLED out of an abundance of caution, to avoid potentially
          // unexpected behavior (since ScheduleStatus.KILLING is a transient state) should this be
          // enabled in the future.
          .put(TaskState.TASK_KILLING, ScheduleStatus.KILLED)
          .put(TaskState.TASK_KILLED, ScheduleStatus.KILLED)
          .put(TaskState.TASK_LOST, ScheduleStatus.LOST)
          .put(TaskState.TASK_ERROR, ScheduleStatus.LOST)
          .build();

  /**
   * Converts a protobuf state to an internal schedule status.
   *
   * @param taskState Protobuf state.
   * @return Equivalent thrift-generated state.
   */
  public static ScheduleStatus convertProtoState(TaskState taskState) {
    ScheduleStatus status = STATE_TRANSLATION.get(taskState);
    Preconditions.checkArgument(status != null, "Unrecognized task state %s", taskState);
    return status;
  }

  private static final Function<Protos.Attribute, String> ATTRIBUTE_NAME =
      Protos.Attribute::getName;

  /**
   * Typedef to make anonymous implementation more concise.
   */
  private interface AttributeConverter
      extends Function<Entry<String, Collection<Protos.Attribute>>, Attribute> {
  }

  private static final Function<Protos.Attribute, String> VALUE_CONVERTER =
      attribute -> {
        switch (attribute.getType()) {
          case SCALAR:
            return String.valueOf(attribute.getScalar().getValue());

          case TEXT:
            return attribute.getText().getValue();

          default:
            LOG.debug("Unrecognized attribute type: {} , ignoring.", attribute.getType());
            return null;
        }
      };

  private static final AttributeConverter ATTRIBUTE_CONVERTER = entry -> {
    // Convert values and filter any that were ignored.
    return new Attribute(
        entry.getKey(),
        FluentIterable.from(entry.getValue())
            .transform(VALUE_CONVERTER)
            .filter(Predicates.notNull())
            .toSet());
  };

  /**
   * Converts protobuf attributes into thrift-generated attributes.
   *
   * @param offer Resource offer.
   * @return Equivalent thrift host attributes.
   */
  public static IHostAttributes getAttributes(Offer offer) {
    // Group by attribute name.
    Multimap<String, Protos.Attribute> valuesByName =
        Multimaps.index(offer.getAttributesList(), ATTRIBUTE_NAME);

    return IHostAttributes.build(new HostAttributes(
        offer.getHostname(),
        FluentIterable.from(valuesByName.asMap().entrySet())
            .transform(ATTRIBUTE_CONVERTER)
            .toSet())
        .setSlaveId(offer.getSlaveId().getValue()));
  }

  /**
   * Determines whether an offer is associated with a slave that is dedicated, based on the presence
   * of an attribute named {@link ConfigurationManager#DEDICATED_ATTRIBUTE}.
   *
   * @param offer Host resource offer.
   * @return {@code true} of {@code offer} is associated with a dedicated slave, otherwise
   *         {@code false}.
   */
  public static boolean isDedicated(Offer offer) {
    return FluentIterable.from(offer.getAttributesList())
        .transform(ATTRIBUTE_NAME)
        .anyMatch(Predicates.equalTo(ConfigurationManager.DEDICATED_ATTRIBUTE));
  }
}
