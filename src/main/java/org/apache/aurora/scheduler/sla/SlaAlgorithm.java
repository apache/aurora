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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;

/**
 * Defines an SLA algorithm to be applied to a {@link IScheduledTask}
 * set for calculating a specific SLA metric.
 */
interface SlaAlgorithm {

  /**
   * Applies this algorithm to a set of {@link IScheduledTask} to
   * produce a named metric value over the specified time frame.
   *
   * @param tasks Set of tasks to apply this algorithm to.
   * @param timeFrame Relevant time frame.
   * @return Produced metric value.
   */
  Number calculate(Iterable<IScheduledTask> tasks, Range<Long> timeFrame);

  /**
   * Pre-configured SLA algorithms.
   */
  enum AlgorithmType {

    JOB_UPTIME_99(new JobUptime(99f), String.format(JobUptime.NAME_FORMAT, 99f)),
    JOB_UPTIME_95(new JobUptime(95f), String.format(JobUptime.NAME_FORMAT, 95f)),
    JOB_UPTIME_90(new JobUptime(90f), String.format(JobUptime.NAME_FORMAT, 90f)),
    JOB_UPTIME_75(new JobUptime(75f), String.format(JobUptime.NAME_FORMAT, 75f)),
    JOB_UPTIME_50(new JobUptime(50f), String.format(JobUptime.NAME_FORMAT, 50f)),
    AGGREGATE_PLATFORM_UPTIME(new AggregatePlatformUptime(), "platform_uptime_percent"),
    MEDIAN_TIME_TO_ASSIGNED(new MedianAlgorithm(ASSIGNED), "mtta_ms"),
    MEDIAN_TIME_TO_STARTING(new MedianAlgorithm(STARTING), "mtts_ms"),
    MEDIAN_TIME_TO_RUNNING(new MedianAlgorithm(RUNNING), "mttr_ms");

    private final SlaAlgorithm algorithm;
    private final String name;

    AlgorithmType(SlaAlgorithm algorithm, String name) {
      this.algorithm = algorithm;
      this.name = name;
    }

    SlaAlgorithm getAlgorithm() {
      return algorithm;
    }

    String getAlgorithmName() {
      return name;
    }
  }

  /**
   * Median time to status SLA algorithm.
   * Represents the median time spent waiting for a set of tasks to reach specified status.
   * A combined metric that helps tracking the task scheduling performance dependency on the
   * requested resources (user scope) as well as the internal scheduler bin-packing algorithm
   * efficiency (platform scope).
   * <p/>
   * Median time calculated as:
   * <pre>
   *    MT =  MEDIAN(Wait_times)
   * where:
   *    Wait_times - a collection of qualifying time intervals between PENDING and specified task
   *                 state. An interval is qualified if its end point is contained by the sample
   *                 time frame.
   *</pre>
   */
  final class MedianAlgorithm implements SlaAlgorithm {

    private final ScheduleStatus status;

    private MedianAlgorithm(ScheduleStatus status) {
      this.status = status;
    }

    @Override
    public Number calculate(Iterable<IScheduledTask> tasks, Range<Long> timeFrame) {
      Iterable<IScheduledTask> activeTasks = FluentIterable.from(tasks)
          .filter(
              Predicates.compose(Predicates.in(Tasks.ACTIVE_STATES), IScheduledTask::getStatus));

      List<Long> waitTimes = Lists.newLinkedList();
      for (IScheduledTask task : activeTasks) {
        long pendingTs = 0;
        for (ITaskEvent event : task.getTaskEvents()) {
          if (event.getStatus() == PENDING) {
            pendingTs = event.getTimestamp();
          } else if (event.getStatus() == status && timeFrame.contains(event.getTimestamp())) {

            if (pendingTs == 0) {
              throw new IllegalArgumentException("SLA: missing PENDING status for:"
                  + task.getAssignedTask().getTaskId());
            }

            waitTimes.add(event.getTimestamp() - pendingTs);
            break;
          }
        }
      }

      return SlaUtil.percentile(waitTimes, 50.0);
    }
  }

  /**
   * Job uptime SLA algorithm.
   * Represents the percentage of instances considered to be in running state for
   * the specified duration relative to SLA calculation time.
   */
  final class JobUptime implements SlaAlgorithm {

    private static final String NAME_FORMAT = "job_uptime_%.2f_sec";
    private final float percentile;

    private static final Predicate<IScheduledTask> IS_RUNNING =
        Predicates.compose(
            Predicates.in(ImmutableSet.of(RUNNING)),
            IScheduledTask::getStatus);

    private static final Function<IScheduledTask, ITaskEvent> TASK_TO_EVENT =
        Tasks::getLatestEvent;

    private JobUptime(float percentile) {
      this.percentile = percentile;
    }

    @Override
    public Number calculate(Iterable<IScheduledTask> tasks, final Range<Long> timeFrame) {
      List<Long> uptimes = FluentIterable.from(tasks)
          .filter(IS_RUNNING)
          .transform(Functions.compose(
              event -> timeFrame.upperEndpoint() - event.getTimestamp(),
              TASK_TO_EVENT)).toList();

      return (double) SlaUtil.percentile(uptimes, percentile) / 1000;
    }
  }

  /**
   * Aggregate Platform Uptime SLA algorithm.
   * Aggregate amount of runnable time a platform managed to deliver for a set of tasks from the
   * moment of reaching them RUNNING status. Excludes any time a task is not in a runnable state
   * due to user activities (e.g. newly created waiting for host assignment or restarted/killed
   * by the user).
   * <p/>
   * Aggregate platform uptime calculated as:
   * <pre>
   *    APU = SUM(Up_time) / SUM(SI - Removed_time)
   * where:
   *    Up_time - the aggregate instance UP time over the sampling interval (SI);
   *    SI - sampling interval (e.g. 1 minute);
   *    Removed_time - the aggregate instance REMOVED time over the sampling interval.
   * </pre>
   */
  final class AggregatePlatformUptime implements SlaAlgorithm {

    /**
     * Task platform SLA state.
     */
    enum SlaState {
      /**
       * Starts a period when the task is not expected to be UP due to user initiated action
       * or failure.
       * <p/>
       * This period is ignored for the calculation purposes.
       */
      REMOVED,

      /**
       * Starts a period when the task cannot reach the UP state for some non-user-related reason.
       * <p/>
       * Only platform-incurred task state transitions are considered here. If a task is newly
       * created (e.g. by job create/update) the amount of time a task spends to reach its UP
       * state is not counted towards platform downtime. For example, a newly added PENDING task
       * is considered as REMOVED, whereas a PENDING task rescheduled from LOST will be considered
       * as DOWN. This approach ensures this metric is not sensitive to user-initiated activities
       * and is a true reflection of the system recovery performance.
       */
      DOWN,

      /**
       * Starts a period when the task is considered to be up and running from the Aurora
       * platform standpoint.
       * <p/>
       * Note: The platform uptime does not necessarily equate to the real application
       * availability. This is because a hosted application needs time to deploy, initialize,
       * and start executing.
       */
      UP
    }

    private static class Interval {
      private final SlaState state;
      private final Range<Long> range;

      Interval(SlaState state, long start, long end) {
        this.state = state;
        range = Range.closedOpen(start, end);
      }
    }

    private static class InstanceId {
      private final IJobKey jobKey;
      private final int id;

      InstanceId(IJobKey jobKey, int instanceId) {
        this.jobKey = requireNonNull(jobKey);
        this.id = instanceId;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof InstanceId)) {
          return false;
        }

        InstanceId other = (InstanceId) o;
        return Objects.equals(jobKey, other.jobKey)
            && Objects.equals(id, other.id);
      }

      @Override
      public int hashCode() {
        return Objects.hash(jobKey, id);
      }
    }

    private static final Function<IScheduledTask, InstanceId> TO_ID =
        task -> new InstanceId(
            task.getAssignedTask().getTask().getJob(),
            task.getAssignedTask().getInstanceId());

    private static final Function<ITaskEvent, Long> TASK_EVENT_TO_TIMESTAMP =
        ITaskEvent::getTimestamp;

    /**
     * Combine all task events per given instance into the unified sorted instance history view.
     */
    private static final Function<Collection<IScheduledTask>, List<ITaskEvent>> TO_SORTED_EVENTS =
        tasks -> {
          List<ITaskEvent> result = Lists.newLinkedList();
          for (IScheduledTask task : tasks) {
            result.addAll(task.getTaskEvents());
          }

          return Ordering.natural()
              .onResultOf(TASK_EVENT_TO_TIMESTAMP).immutableSortedCopy(result);
        };

    /**
     * Convert instance history into the {@link SlaState} based {@link Interval} list.
     */
    private static final Function<List<ITaskEvent>, List<Interval>> TASK_EVENTS_TO_INTERVALS =
        events -> {

          ImmutableList.Builder<Interval> intervals = ImmutableList.builder();
          Pair<SlaState, Long> current = Pair.of(SlaState.REMOVED, 0L);

          for (ITaskEvent event : events) {
            long timestamp = event.getTimestamp();

            // Event status in the instance timeline signifies either of the following:
            // - termination of the existing SlaState interval AND start of a new one;
            // - continuation of the existing matching SlaState interval.
            switch (event.getStatus()) {
              case LOST:
              case DRAINING:
              case PREEMPTING:
                current = updateIntervals(timestamp, SlaState.DOWN, current, intervals);
                break;

              case PENDING:
              case ASSIGNED:
              case STARTING:
                if (current.getFirst() != SlaState.DOWN) {
                  current = updateIntervals(timestamp, SlaState.REMOVED, current, intervals);
                }
                break;

              case THROTTLED:
              case FINISHED:
              case RESTARTING:
              case FAILED:
              case KILLING:
                current = updateIntervals(timestamp, SlaState.REMOVED, current, intervals);
                break;

              case RUNNING:
                current = updateIntervals(timestamp, SlaState.UP, current, intervals);
                break;

              case KILLED:
                if (current.getFirst() == SlaState.UP) {
                  current = updateIntervals(timestamp, SlaState.DOWN, current, intervals);
                }
                break;

              case INIT:
                // Ignore.
                break;

              default:
                throw new IllegalArgumentException("Unsupported status:" + event.getStatus());
            }
          }
          // Add the last event interval.
          intervals.add(new Interval(current.getFirst(), current.getSecond(), Long.MAX_VALUE));
          return intervals.build();
        };

    private static Pair<SlaState, Long> updateIntervals(
        long timestamp,
        SlaState state,
        Pair<SlaState, Long> current,
        ImmutableList.Builder<Interval> intervals) {

      if (current.getFirst() == state) {
        // Current interval state matches the event state - skip.
        return current;
      } else {
        // Terminate current interval, add it to list and start a new interval.
        intervals.add(new Interval(current.getFirst(), current.getSecond(), timestamp));
        return Pair.of(state, timestamp);
      }
    }

    private AggregatePlatformUptime() {
      // Interface private.
    }

    @Override
    public Number calculate(Iterable<IScheduledTask> tasks, Range<Long> timeFrame) {
      // Given the set of tasks do the following:
      // - index all available tasks by InstanceId (JobKey + instance ID);
      // - combine individual task ITaskEvent lists into the instance based timeline to represent
      //   all available history for a given task instance;
      // - convert instance timeline into the SlaState intervals.
      Map<InstanceId, List<Interval>> instanceSlaTimeline =
          Maps.transformValues(
              Multimaps.index(tasks, TO_ID).asMap(),
              Functions.compose(TASK_EVENTS_TO_INTERVALS, TO_SORTED_EVENTS));

      // Given the instance timeline converted to SlaState-based time intervals, aggregate the
      // platform uptime per given timeFrame.
      long aggregateUptime = 0;
      long aggregateTotal = 0;
      for (List<Interval> intervals : instanceSlaTimeline.values()) {
        long instanceUptime = elapsedFromRange(timeFrame);
        long instanceTotal = instanceUptime;
        for (Interval interval : intervals) {
          if (timeFrame.isConnected(interval.range)) {
            long intersection = elapsedFromRange(timeFrame.intersection(interval.range));
            if (interval.state == SlaState.REMOVED) {
              instanceUptime -= intersection;
              instanceTotal -= intersection;
            } else if (interval.state == SlaState.DOWN) {
              instanceUptime -= intersection;
            }
          }
        }
        aggregateUptime += instanceUptime;
        aggregateTotal += instanceTotal;
      }

      // Calculate effective platform uptime or default to 100.0 if no instances are running yet.
      return aggregateTotal > 0 ? (double) aggregateUptime * 100 / aggregateTotal : 100.0;
    }

    private static long elapsedFromRange(Range<Long> range) {
      return range.upperEndpoint() - range.lowerEndpoint();
    }
  }
}
