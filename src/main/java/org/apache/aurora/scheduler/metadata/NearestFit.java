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
package org.apache.aurora.scheduler.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.scheduling.TaskGroup;

/**
 * Tracks vetoes against scheduling decisions and maintains the closest fit among all the vetoes
 * for a task.
 */
public class NearestFit implements EventSubscriber {
  @VisibleForTesting
  static final Amount<Long, Time> EXPIRATION = Amount.of(10L, Time.MINUTES);

  @VisibleForTesting
  static final ImmutableSet<Veto> NO_VETO = ImmutableSet.of();

  private final LoadingCache<TaskGroupKey, Fit> fitByGroupKey;

  public NearestFit(Ticker ticker) {
    fitByGroupKey = CacheBuilder.newBuilder()
        .expireAfterWrite(EXPIRATION.getValue(), EXPIRATION.getUnit().getTimeUnit())
        .ticker(ticker)
        .build(new CacheLoader<TaskGroupKey, Fit>() {
          @Override
          public Fit load(TaskGroupKey groupKey) {
            return new Fit();
          }
        });
  }

  @Inject
  NearestFit() {
    this(Ticker.systemTicker());
  }

  /**
   * Gets the vetoes that represent the nearest fit for the given task.
   *
   * @param groupKey The task group key to look up.
   * @return The nearest fit vetoes for the given task.  This will return an empty set if
   *         no vetoes have been recorded for the task.
   */
  public synchronized ImmutableSet<Veto> getNearestFit(TaskGroupKey groupKey) {
    Fit fit = fitByGroupKey.getIfPresent(groupKey);
    return (fit == null) ? NO_VETO : fit.vetoes;
  }

  /**
   * Records a task deletion event.
   *
   * @param deletedEvent Task deleted event.
   */
  @Subscribe
  public synchronized void remove(TasksDeleted deletedEvent) {
    fitByGroupKey.invalidateAll(Iterables.transform(deletedEvent.getTasks(), Functions.compose(
        TaskGroupKey::from,
        Tasks::getConfig)));
  }

  /**
   * Records a task state change event.
   * This will ignore any events where the previous state is not {@link ScheduleStatus#PENDING}.
   *
   * @param event Task state change.
   */
  @Subscribe
  public synchronized void stateChanged(TaskStateChange event) {
    if (event.isTransition() && event.getOldState().get() == ScheduleStatus.PENDING) {
      fitByGroupKey.invalidate(TaskGroupKey.from(event.getTask().getAssignedTask().getTask()));
    }
  }

  /**
   * Records a task veto event.
   *
   * @param vetoEvent Veto event.
   */
  @Subscribe
  public synchronized void vetoed(Vetoed vetoEvent) {
    Objects.requireNonNull(vetoEvent);
    fitByGroupKey.getUnchecked(vetoEvent.getGroupKey()).maybeUpdate(vetoEvent.getVetoes());
  }

  /**
   * Determine the pending reason, for each of the given tasks in taskGroups.
   *
   * @param taskGroups Group of pending tasks.
   * @return A map with key=String (the taskgroup key) and value=List of reasons.
   */
  public synchronized Map<String, List<String>> getPendingReasons(Iterable<TaskGroup> taskGroups) {
    return StreamSupport.stream(taskGroups.spliterator(), false).map(t -> {
      List<String> reasons = getNearestFit(t.getKey()).stream()
          .map(Veto::getReason).collect(Collectors.toList());
      return new HashMap.SimpleEntry<>(t.getKey().toString(), reasons);
    }).collect(GuavaUtils.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static class Fit {
    private ImmutableSet<Veto> vetoes;

    private static int score(Iterable<Veto> vetoes) {
      int total = 0;
      for (Veto veto : vetoes) {
        total += veto.getScore();
      }
      return total;
    }

    private void update(Iterable<Veto> newVetoes) {
      vetoes = ImmutableSet.copyOf(newVetoes);
    }

    /**
     * Updates the nearest fit if the provided vetoes represents a closer fit than the current
     * best fit.
     * <p>
     * Vetoes with a lower aggregate score are considered a better fit regardless of the total veto
     * count. See {@link Veto} for more details on scoring differences.
     * @param newVetoes The vetoes for the scheduling assignment with {@code newHost}.
     */
    void maybeUpdate(Set<Veto> newVetoes) {
      if (vetoes == null) {
        update(newVetoes);
        return;
      }

      if (score(newVetoes) < score(vetoes)) {
        update(newVetoes);
      }
    }
  }
}
