package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class CommandLineExpanderTest {

  private static final String TASK_ID = "my_task_id";
  private static final int SHARD_ID = 3;

  private static AssignedTask makeTask(String startCommand) {
    return new AssignedTask()
        .setTaskId(TASK_ID)
        .setTask(new TwitterTaskInfo()
            .setStartCommand(startCommand)
            .setShardId(SHARD_ID));
  }

  private static AssignedTask checkAndExpand(AssignedTask task, Set<Integer> ports)
      throws TaskDescriptionException {
    String command = task.getTask().getStartCommand();
    assertEquals(ports.size(), CommandLineExpander.getNumPortsRequested(command));
    return CommandLineExpander.expand(task, ports);
  }

  @Test
  public void testExpandPort() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of(5);

    AssignedTask task = makeTask("echo '%port:http%'");
    task = checkAndExpand(task, ports);
    assertEquals("echo '5'", task.getTask().getStartCommand());
    assertEquals(ImmutableMap.of("http", 5), task.getAssignedPorts());
  }

  @Test
  public void testExpandPortDuplicate() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of(5);

    AssignedTask task = makeTask("echo '%port:http%'; echo '%port:http%';");
    task = checkAndExpand(task, ports);
    assertEquals("echo '5'; echo '5';", task.getTask().getStartCommand());
    assertEquals(ImmutableMap.of("http", 5), task.getAssignedPorts());
  }

  @Test
  public void testExpandPorts() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of(20, 30, 50);

    AssignedTask task = makeTask("echo '%port:http%'; echo '%port:thrift%'; echo '%port:mail%'");
    task = checkAndExpand(task, ports);
    assertEquals(ImmutableSet.of("http", "thrift", "mail"), task.getAssignedPorts().keySet());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExpandPortsTooManyRequested() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of(5);

    AssignedTask task = makeTask("echo '%port:http% %port:web%'");
    CommandLineExpander.expand(task, ports);
  }

  @Test
  public void testGetShardId() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of();

    AssignedTask task = makeTask("echo %shard_id%");
    task = checkAndExpand(task, ports);
    assertEquals("echo " + SHARD_ID, task.getTask().getStartCommand());
  }

  @Test
  public void testGetTaskId() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of();

    AssignedTask task = makeTask("echo %task_id%");
    task = checkAndExpand(task, ports);
    assertEquals("echo " + TASK_ID, task.getTask().getStartCommand());
  }

  @Test
  public void testPortNameWithSpace() throws TaskDescriptionException {
    Set<Integer> ports = ImmutableSet.of();

    AssignedTask task = makeTask("echo %port: foo %");
    task = checkAndExpand(task, ports);
    assertEquals("echo %port: foo %", task.getTask().getStartCommand());
  }
}
