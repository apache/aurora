/**
 * Copyright 2013 Apache Software Foundation
 *
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

import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;

/**
 * Tracks vetoes against scheduling decisions and maintains the closest fit among all the vetoes
 * for a task.
 */
public class NearestFit implements EventSubscriber {
  @VisibleForTesting
  static final Amount<Long, Time> EXPIRATION = Amount.of(10L, Time.MINUTES);

  @VisibleForTesting
  static final ImmutableSet<Veto> NO_VETO = ImmutableSet.of();

  private final LoadingCache<String, Fit> fitByTask;

  @VisibleForTesting
  NearestFit(Ticker ticker) {
    fitByTask = CacheBuilder.newBuilder()
        .expireAfterWrite(EXPIRATION.getValue(), EXPIRATION.getUnit().getTimeUnit())
        .ticker(ticker)
        .build(new CacheLoader<String, Fit>() {
          @Override
          public Fit load(String taskId) {
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
   * @param taskId The task to look up.
   * @return The nearest fit vetoes for the given task.  This will return an empty set if
   *         no vetoes have been recorded for the task.
   */
  public synchronized ImmutableSet<Veto> getNearestFit(String taskId) {
    Fit fit = fitByTask.getIfPresent(taskId);
    return (fit == null) ? NO_VETO : fit.vetoes;
  }

  /**
   * Records a task deletion event.
   *
   * @param deletedEvent Task deleted event.
   */
  @Subscribe
  public synchronized void remove(TasksDeleted deletedEvent) {
    fitByTask.invalidateAll(Tasks.ids(deletedEvent.getTasks()));
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
      fitByTask.invalidate(event.getTaskId());
    }
  }

  private static final Predicate<Veto> IS_CONSTRAINT_MISMATCH = new Predicate<Veto>() {
    @Override
    public boolean apply(Veto veto) {
      return veto.isConstraintMismatch();
    }
  };

  /**
   * Records a task veto event.
   * This will ignore any veto events where any veto returns {@code true} from
   * {@link Veto#isConstraintMismatch()}.
   *
   * @param vetoEvent Veto event.
   */
  @Subscribe
  public synchronized void vetoed(Vetoed vetoEvent) {
    Preconditions.checkNotNull(vetoEvent);
    fitByTask.getUnchecked(vetoEvent.getTaskId()).maybeUpdate(vetoEvent.getVetoes());
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
     * There are two classes of vetoes: those with and without constraint mismatches. A set of
     * vetoes without a constraint mismatch is always a better fit than a set with constraint
     * mismatches.
     * <p>
     * If two sets are equivalent in that they do or do not have constraint mismatches, they are
     * compared by the following criteria:
     * <ul>
     *   <li> the one with fewer vetoes is a better fit, irrespective of scores
     *   <li> if the veto count is equal, the one with the smaller aggregate score is a better fit
     * </ul>
     *
     * @param newVetoes The vetoes for the scheduling assignment with {@code newHost}.
     */
    void maybeUpdate(Set<Veto> newVetoes) {
      if (vetoes == null) {
        update(newVetoes);
        return;
      }

      boolean valueMismatchOld = Iterables.any(vetoes, IS_CONSTRAINT_MISMATCH);
      boolean valueMismatchNew = Iterables.any(newVetoes, IS_CONSTRAINT_MISMATCH);
      if (valueMismatchOld == valueMismatchNew) {
        if (newVetoes.size() < vetoes.size()) {
          update(newVetoes);
        } else if (newVetoes.size() == vetoes.size() && score(newVetoes) < score(vetoes)) {
          update(newVetoes);
        }
      } else if (valueMismatchOld) {
        update(newVetoes);
      }
    }
  }
}
