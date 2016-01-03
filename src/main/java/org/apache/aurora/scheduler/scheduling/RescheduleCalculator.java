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
package org.apache.aurora.scheduler.scheduling;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.common.util.Random;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.DRAINING;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;

/**
 * Calculates scheduling delays for tasks.
 */
public interface RescheduleCalculator {
  /**
   * Calculates the delay, in milliseconds, before the task should be considered eligible for
   * (re)scheduling at scheduler startup.
   *
   * @param task Task to calculate delay for.
   * @return Delay in msec.
   */
  long getStartupScheduleDelayMs(IScheduledTask task);

  /**
   * Calculates the penalty, in milliseconds, that a task should be penalized before being
   * eligible for rescheduling.
   *
   * @param task Task to calculate delay for.
   * @return Delay in msec.
   */
  long getFlappingPenaltyMs(IScheduledTask task);

  class RescheduleCalculatorImpl implements RescheduleCalculator {

    private static final Logger LOG = Logger.getLogger(TaskGroups.class.getName());

    private final Storage storage;
    private final RescheduleCalculatorSettings settings;
    // TODO(wfarner): Inject 'random' in the constructor for better test coverage.
    private final Random random = Random.Util.newDefaultRandom();

    private static final Predicate<ScheduleStatus> IS_ACTIVE_STATUS =
        Predicates.in(Tasks.ACTIVE_STATES);

    private static final Set<ScheduleStatus> INTERRUPTED_TASK_STATES =
        EnumSet.of(RESTARTING, KILLING, DRAINING);

    private final Predicate<IScheduledTask> flapped = new Predicate<IScheduledTask>() {
      @Override
      public boolean apply(IScheduledTask task) {
        if (!task.isSetTaskEvents()) {
          return false;
        }

        List<ITaskEvent> events = Lists.reverse(task.getTaskEvents());

        // Avoid penalizing tasks that were interrupted by outside action, such as a user
        // restarting them.
        if (Iterables.any(Iterables.transform(events, ITaskEvent::getStatus),
            Predicates.in(INTERRUPTED_TASK_STATES))) {
          return false;
        }

        ITaskEvent terminalEvent = Iterables.get(events, 0);
        ScheduleStatus terminalState = terminalEvent.getStatus();
        Preconditions.checkState(Tasks.isTerminated(terminalState));

        ITaskEvent activeEvent = Iterables.find(
            events,
            Predicates.compose(IS_ACTIVE_STATUS, ITaskEvent::getStatus));

        long thresholdMs = settings.flappingTaskThreashold.as(Time.MILLISECONDS);

        return (terminalEvent.getTimestamp() - activeEvent.getTimestamp()) < thresholdMs;
      }
    };

    @VisibleForTesting
    public static class RescheduleCalculatorSettings {
      private final BackoffStrategy flappingTaskBackoff;
      private final Amount<Long, Time> flappingTaskThreashold;
      private final Amount<Integer, Time>  maxStartupRescheduleDelay;

      public RescheduleCalculatorSettings(
          BackoffStrategy flappingTaskBackoff,
          Amount<Long, Time> flappingTaskThreashold,
          Amount<Integer, Time> maxStartupRescheduleDelay) {

        this.flappingTaskBackoff = requireNonNull(flappingTaskBackoff);
        this.flappingTaskThreashold = requireNonNull(flappingTaskThreashold);
        this.maxStartupRescheduleDelay = requireNonNull(maxStartupRescheduleDelay);
      }
    }

    @Inject
    RescheduleCalculatorImpl(Storage storage, RescheduleCalculatorSettings settings) {
      this.storage = requireNonNull(storage);
      this.settings = requireNonNull(settings);
    }

    @Override
    public long getStartupScheduleDelayMs(IScheduledTask task) {
      return random.nextInt(settings.maxStartupRescheduleDelay.as(Time.MILLISECONDS).intValue())
          + getFlappingPenaltyMs(task);
    }

    private Optional<IScheduledTask> getTaskAncestor(IScheduledTask task) {
      if (!task.isSetAncestorId()) {
        return Optional.absent();
      }

      Iterable<IScheduledTask> res =
          Storage.Util.fetchTasks(storage, Query.taskScoped(task.getAncestorId()));

      return Optional.fromNullable(Iterables.getOnlyElement(res, null));
    }

    @Override
    public long getFlappingPenaltyMs(IScheduledTask task) {
      Optional<IScheduledTask> curTask = getTaskAncestor(task);
      long penaltyMs = 0;
      while (curTask.isPresent() && flapped.apply(curTask.get())) {
        LOG.info(
            String.format("Ancestor of %s flapped: %s", Tasks.id(task), Tasks.id(curTask.get())));
        long newPenalty = settings.flappingTaskBackoff.calculateBackoffMs(penaltyMs);
        // If the backoff strategy is truncated then there is no need for us to continue.
        if (newPenalty == penaltyMs) {
          break;
        }
        penaltyMs = newPenalty;
        curTask = getTaskAncestor(curTask.get());
      }

      return penaltyMs;
    }
  }
}
