package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskState;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * Collection of utility functions to convert mesos protobuf types to internal thrift types.
 */
public final class Conversions {

  private static final Logger LOG = Logger.getLogger(Conversions.class.getName());

  private Conversions() {
    // Utility class.
  }

  // Maps from mesos state to scheduler interface state.
  private static final Map<TaskState, ScheduleStatus> STATE_TRANSLATION =
      new ImmutableMap.Builder<TaskState, ScheduleStatus>()
          .put(TaskState.TASK_STARTING, ScheduleStatus.STARTING)
          .put(TaskState.TASK_RUNNING, ScheduleStatus.RUNNING)
          .put(TaskState.TASK_FINISHED, ScheduleStatus.FINISHED)
          .put(TaskState.TASK_FAILED, ScheduleStatus.FAILED)
          .put(TaskState.TASK_KILLED, ScheduleStatus.KILLED)
          .put(TaskState.TASK_LOST, ScheduleStatus.LOST)
          .build();

  /**
   * Converts a protobuf state to an internal schedule status.
   *
   * @param taskState Protobuf state.
   * @return Equivalent thrift-generated state.
   */
  public static ScheduleStatus convertProtoState(TaskState taskState) {
    ScheduleStatus status = STATE_TRANSLATION.get(taskState);
    Preconditions.checkArgument(status != null, "Unrecognized task state " + taskState);
    return status;
  }

  private static final Function<Protos.Attribute, String> ATTRIBUTE_NAME =
      new Function<Protos.Attribute, String>() {
        @Override public String apply(Protos.Attribute attr) {
          return attr.getName();
        }
      };

  /**
   * Typedef to make anonymous implementation more concise.
   */
  private abstract static class AttributeConverter
      implements Function<Entry<String, Collection<Protos.Attribute>>, Attribute> {
  }

  private static final Function<Protos.Attribute, String> VALUE_CONVERTER =
      new Function<Protos.Attribute, String>() {
        @Override public String apply(Protos.Attribute attribute) {
          switch (attribute.getType()) {
            case SCALAR:
              return String.valueOf(attribute.getScalar().getValue());

            case TEXT:
              return attribute.getText().getValue();

            default:
              LOG.finest("Unrecognized attribute type:" + attribute.getType() + " , ignoring.");
              return null;
          }
        }
      };

  private static final AttributeConverter ATTRIBUTE_CONVERTER = new AttributeConverter() {
    @Override public Attribute apply(Entry<String, Collection<Protos.Attribute>> entry) {
      // Convert values and filter any that were ignored.
      return new Attribute(
          entry.getKey(),
          FluentIterable.from(entry.getValue())
              .transform(VALUE_CONVERTER)
              .filter(Predicates.notNull())
              .toImmutableSet());
    }
  };

  /**
   * Converts protobuf attributes into thrift-generated attributes.
   *
   * @param offer Resource offer.
   * @return Equivalent thrift host attributes.
   */
  public static HostAttributes getAttributes(Offer offer) {
    // Group by attribute name.
    Multimap<String, Protos.Attribute> valuesByName =
        Multimaps.index(offer.getAttributesList(), ATTRIBUTE_NAME);

    // TODO(William Farner): Include slave id.
    return new HostAttributes(
        offer.getHostname(),
        FluentIterable.from(valuesByName.asMap().entrySet())
            .transform(ATTRIBUTE_CONVERTER)
            .toImmutableSet());
  }
}
