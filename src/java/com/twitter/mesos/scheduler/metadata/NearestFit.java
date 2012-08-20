package com.twitter.mesos.scheduler.metadata;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;

/**
 * Tracks vetoes against scheduling decisions and maintains the closest fit among all the vetoes
 * for a task.
 *
 * TODO(William Farner): Wire this up to SchedulingFilterImpl and TaskStateMachine.
 * when recording vetoes from the filter, only record if the host and task have a matching
 * dedicated attribute/constraint.
 * TODO(William Farner): Doc methods in this class when integrating with other components.
 */
class NearestFit {
  @VisibleForTesting
  static final Amount<Long, Time> EXPIRATION = Amount.of(10L, Time.MINUTES);

  @VisibleForTesting
  static final ImmutableSet<Veto> NO_VETO = ImmutableSet.of();

  // Probably need expiration
  private final LoadingCache<String, Fit> fitByTask;

  @VisibleForTesting
  NearestFit(Ticker ticker) {
    fitByTask = CacheBuilder.newBuilder()
        .expireAfterWrite(EXPIRATION.getValue(), EXPIRATION.getUnit().getTimeUnit())
        .ticker(ticker)
        .build(new CacheLoader<String, Fit>() {
          @Override public Fit load(String taskId) {
            return new Fit();
          }
        });
  }

  @Inject
  NearestFit() {
    this(Ticker.systemTicker());
  }

  synchronized void record(String taskId, Set<Veto> vetoes) {
    Preconditions.checkNotNull(taskId);
    MorePreconditions.checkNotBlank(vetoes);
    fitByTask.getUnchecked(taskId).maybeUpdate(vetoes);
  }

  synchronized ImmutableSet<Veto> getNearestFit(String taskId) {
    Fit fit = fitByTask.getIfPresent(taskId);
    return (fit == null) ? NO_VETO : fit.vetoes;
  }

  synchronized void remove(String taskId) {
    fitByTask.invalidate(taskId);
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
     * A set of vetoes is considered a better fit if it:
     * <ul>
     * <li> has a smaller number of vetoes, irrespective of scores
     * <li> has the same number of vetoes and a smaller aggregate score
     * </ul>
     *
     * @param newVetoes The vetoes for the scheduling assignment with {@code newHost}.
     */
    void maybeUpdate(Set<Veto> newVetoes) {
      if ((vetoes == null) || (newVetoes.size() < vetoes.size())) {
        update(newVetoes);
      } else if ((newVetoes.size() == vetoes.size()) && (score(newVetoes) < score(vetoes))) {
        update(newVetoes);
      }
    }
  }
}
