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
package org.apache.aurora.scheduler.sla;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.gen.ScheduleStatus;

import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.SANDBOX_DELETED;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.AGGREGATE_PLATFORM_UPTIME;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.JOB_UPTIME_50;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.JOB_UPTIME_75;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.JOB_UPTIME_90;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.JOB_UPTIME_95;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.JOB_UPTIME_99;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.MEDIAN_TIME_TO_ASSIGNED;
import static org.apache.aurora.scheduler.sla.SlaAlgorithm.AlgorithmType.MEDIAN_TIME_TO_RUNNING;
import static org.junit.Assert.assertEquals;

public class SlaAlgorithmTest {

  @Test
  public void MedianTimeToAssignedEvenTest() {
    Number actual = MEDIAN_TIME_TO_ASSIGNED.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING, 200L, ASSIGNED, 250L, KILLED)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED, 300L, RUNNING)),
            makeTask(ImmutableMap.of(200L, PENDING, 250L, ASSIGNED, 350L, STARTING))),
        Range.<Long>all());
    assertEquals(50L, actual);
  }

  @Test
  public void MedianTimeToAssignedOddTest() {
    Number actual = MEDIAN_TIME_TO_ASSIGNED.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING, 200L, ASSIGNED, 250L, RUNNING)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED, 300L, RUNNING)),
            makeTask(ImmutableMap.of(200L, PENDING, 250L, ASSIGNED, 350L, STARTING))),
        Range.<Long>all());
    assertEquals(100L, actual);
  }

  @Test
  public void MedianTimeToAssignedZeroTest() {
    Number actual = MEDIAN_TIME_TO_ASSIGNED.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED, 300L, KILLED))),
        Range.<Long>all());
    assertEquals(0L, actual);
  }

  @Test
  public void MedianTimeToAssignedOneTest() {
    Number actual = MEDIAN_TIME_TO_ASSIGNED.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED))),
        Range.<Long>all());
    assertEquals(100L, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void MedianTimeToAssignedNoPendingTest() {
    MEDIAN_TIME_TO_ASSIGNED.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, ASSIGNED))),
        Range.<Long>all());
  }

  @Test
  public void MedianTimeToRunningEvenTest() {
    Number actual = MEDIAN_TIME_TO_RUNNING.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING)), // Ignored as not RUNNING
            makeTask(ImmutableMap.of(50L, PENDING, 100L, ASSIGNED, 150L, STARTING, 180L, RUNNING)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED, 300L, STARTING, 400L, RUNNING)),
            makeTask(ImmutableMap.of(
                50L, PENDING,
                100L, ASSIGNED,
                150L, STARTING,
                200L, RUNNING,
                300L, KILLED))), // Ignored due to being terminal.
        Range.<Long>all());
    assertEquals(130L, actual);
  }

  @Test
  public void MedianTimeToRunningOddTest() {
    Number actual = MEDIAN_TIME_TO_RUNNING.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING)), // Ignored as not RUNNING
            makeTask(ImmutableMap.of(50L, PENDING, 100L, ASSIGNED, 150L, STARTING, 180L, RUNNING)),
            makeTask(ImmutableMap.of(100L, PENDING, 200L, ASSIGNED, 300L, STARTING, 400L, RUNNING)),
            makeTask(ImmutableMap.of(50L, PENDING, 100L, ASSIGNED, 150L, STARTING, 200L, RUNNING))),
        Range.<Long>all());
    assertEquals(150L, actual);
  }

  @Test
  public void MedianTimeToRunningZeroTest() {
    Number actual = MEDIAN_TIME_TO_RUNNING.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, PENDING)),
            makeTask(ImmutableMap.of(50L, PENDING, 100L, RUNNING, 200L, KILLED))),
            Range.<Long>all());
    assertEquals(0L, actual);
  }

  @Test
  public void JobUptime50Test() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_50.getAlgorithm().calculate(
        makeUptimeTasks(100, now),
        Range.closed(0L, now));
    assertEquals(50, actual);
  }

  @Test
  public void JobUptime75Test() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_75.getAlgorithm().calculate(
        makeUptimeTasks(100, now),
        Range.closed(0L, now));
    assertEquals(25, actual);
  }

  @Test
  public void JobUptime90Test() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_90.getAlgorithm().calculate(
        makeUptimeTasks(100, now),
        Range.closed(0L, now));
    assertEquals(10, actual);
  }

  @Test
  public void JobUptime95Test() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_95.getAlgorithm().calculate(
        makeUptimeTasks(100, now),
        Range.closed(0L, now));
    assertEquals(5, actual);
  }

  @Test
  public void JobUptime99Test() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_99.getAlgorithm().calculate(
        makeUptimeTasks(100, now),
        Range.closed(0L, now));
    assertEquals(1, actual);
  }

  @Test
  public void JobUptimeEmptyTest() {
    long now = System.currentTimeMillis();
    Number actual = JOB_UPTIME_99.getAlgorithm().calculate(
        new LinkedList<IScheduledTask>(),
        Range.closed(0L, now));
    assertEquals(0, actual);
  }

  @Test
  public void JobUptimeNonTerminalIgnoredTest() {
    long now = System.currentTimeMillis();
    Set<IScheduledTask> instances = makeUptimeTasks(100, now);
    instances.add(makeTask(ImmutableMap.of(now - 5000, RUNNING, now - 3000, KILLED)));
    Number actual = JOB_UPTIME_99.getAlgorithm().calculate(instances, Range.closed(0L, now));
    assertEquals(1, actual);
  }

  @Test
  public void JobUptimeLiveNonTerminalIgnoredTest() {
    long now = System.currentTimeMillis();
    Set<IScheduledTask> instances = makeUptimeTasks(100, now);
    instances.add(makeTask(ImmutableMap.of(now - 5000, RUNNING, now - 3000, RESTARTING)));
    Number actual = JOB_UPTIME_99.getAlgorithm().calculate(instances, Range.closed(0L, now));
    assertEquals(1, actual);
  }

  @Test
  public void AggregatePlatformUptimeTest() {
    Number actual = AGGREGATE_PLATFORM_UPTIME.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(100L, PENDING), 0), // Ignored.
            makeTask(ImmutableMap.of(
                100L, PENDING,
                200L, ASSIGNED,
                300L, STARTING,
                400L, RUNNING), 1), // 100% uptime.
            makeTask(ImmutableMap.<Long, ScheduleStatus>builder()
                .put(5L, INIT)
                .put(10L, PENDING)
                .put(20L, ASSIGNED)
                .put(30L, STARTING)
                .put(50L, RUNNING)
                .put(400L, KILLING)
                .put(450L, KILLED).build(), 2)), // 100% uptime.
        Range.closedOpen(100L, 500L));
    assertEquals(100.0, actual);
  }

  @Test
  public void AggregatePlatformUptimeRecoveredFromDownTest() {
    Number actual = AGGREGATE_PLATFORM_UPTIME.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, RUNNING, 300L, LOST, 310L, KILLED), 0), // DOWN mid range.
            makeTask(ImmutableMap.of(
                320L, PENDING,
                330L, ASSIGNED,
                350L, STARTING,
                400L, RUNNING), 0)),  // Recovered within range.
        Range.closedOpen(100L, 500L));
    assertEquals(75.0, actual);
  }

  @Test
  public void AggregatePlatformUptimeKilledByPlatformTest() {
    Number actual = AGGREGATE_PLATFORM_UPTIME.getAlgorithm().calculate(
        ImmutableSet.of(makeTask(ImmutableMap.of(50L, RUNNING, 300L, KILLED), 0)),
        Range.closedOpen(100L, 500L));
    assertEquals(50.0, actual);
  }

  @Test
  public void AggregatePlatformUptimeSandboxDeletedIgnoredTest() {
    Number actual = AGGREGATE_PLATFORM_UPTIME.getAlgorithm().calculate(
        ImmutableSet.of(
            makeTask(ImmutableMap.of(50L, RUNNING, 300L, LOST, 400L, SANDBOX_DELETED), 0)),
        Range.closedOpen(100L, 500L));
    assertEquals(50.0, actual);
  }

  @Test
  public void AggregatePlatformUptimeEmptyTest() {
    Number actual = AGGREGATE_PLATFORM_UPTIME.getAlgorithm().calculate(
        ImmutableSet.of(makeTask(ImmutableMap.of(50L, PENDING), 0)),
        Range.closedOpen(100L, 500L));
    assertEquals(100.0, actual);
  }

  private static Set<IScheduledTask> makeUptimeTasks(int num, long now) {
    Set<IScheduledTask> instances = Sets.newHashSet();
    for (int i = 0; i < num; i++) {
      instances.add(makeTask(ImmutableMap.of(now - (i + 1) * 1000, RUNNING)));
    }
    return instances;
  }

  private static IScheduledTask makeTask(Map<Long, ScheduleStatus> events) {
    return makeTask(events, 0);
  }

  private static IScheduledTask makeTask(Map<Long, ScheduleStatus> events, int instanceId) {
    return SlaTestUtil.makeTask(events, instanceId);
  }
}
