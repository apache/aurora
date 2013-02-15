package com.twitter.mesos.scheduler;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Utility class to handle command line expansion.
 * TODO(wfarner): Rename this class, since it no longer deals with expanding command lines.
 */
public final class CommandLineExpander {

  private static final Logger LOG = Logger.getLogger(CommandLineExpander.class.getName());

  // Double percents to escape formatting sequence.
  private static final String PORT_FORMAT = "%%port:%s%%";
  private static final String SHARD_ID_REGEXP = "%shard_id%";
  private static final String TASK_ID_REGEXP = "%task_id%";
  private static final String HOST_REGEXP = "%host%";

  private CommandLineExpander() {
    // Utility.
  }

  /**
   * Associates requested port names with the allocated port numbers.
   *
   * @param portNames Names of requested ports.
   * @param allocatedPorts Allocated ports.
   * @return Named ports mapped to assigned port numbers.
   */
  public static Map<String, Integer> getNameMappedPorts(Set<String> portNames,
      Set<Integer> allocatedPorts) {
    Preconditions.checkNotNull(portNames);

    // Expand ports.
    Map<String, Integer> ports = Maps.newHashMap();
    Set<Integer> portsRemaining = Sets.newHashSet(allocatedPorts);
    Iterator<Integer> portConsumer = Iterables.consumingIterable(portsRemaining).iterator();

    for (String portName : portNames) {
      Preconditions.checkArgument(portConsumer.hasNext(),
          "Allocated ports %s were not sufficient to expand task.", allocatedPorts);
      int portNumber = portConsumer.next();
      ports.put(portName, portNumber);
    }

    if (!portsRemaining.isEmpty()) {
      LOG.warning("Not all allocated ports were used to map ports!");
    }

    return ports;
  }

  /**
   * Expands wildcards in an arbitrary string.
   *
   * @param value String to expand.
   * @param task Context for expansion of wildcards.
   * @return {@code value} with any wildcards expanded.
   */
  public static String expand(String value, AssignedTask task) {
    String expanded = value;
    TwitterTaskInfo config = task.getTask();

    expanded = expanded.replaceAll(SHARD_ID_REGEXP, String.valueOf(config.getShardId()));
    expanded = expanded.replaceAll(TASK_ID_REGEXP, task.getTaskId());

    if (task.isSetSlaveHost()) {
      expanded = expanded.replaceAll(HOST_REGEXP, task.getSlaveHost());
    }

    // Expand ports.
    if (task.isSetAssignedPorts()) {
      for (Map.Entry<String, Integer> portEntry : task.getAssignedPorts().entrySet()) {
        expanded = expanded.replaceAll(
            String.format(PORT_FORMAT, portEntry.getKey()),
            String.valueOf(portEntry.getValue()));
      }
    }

    return expanded;
  }
}
