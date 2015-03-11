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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * Defines methods to manage preemptor metrics.
 */
@VisibleForTesting
public class PreemptorMetrics {
  @VisibleForTesting
  static final String MISSING_ATTRIBUTES_NAME = "preemptor_missing_attributes";

  private volatile boolean exported = false;
  private final CachedCounters counters;

  @Inject
  PreemptorMetrics(CachedCounters counters) {
    this.counters = requireNonNull(counters);
    assertFullyExported();
  }

  private static String name(boolean production) {
    return production ? "prod" : "non_prod";
  }

  private void assertFullyExported() {
    if (exported) {
      return;
    }

    // Dummy-read all stats to ensure they are exported.
    Set<String> allStats = ImmutableSet.of(
        attemptsStatName(false),
        attemptsStatName(true),
        successStatName(false),
        successStatName(true),
        failureStatName(false),
        failureStatName(true),
        MISSING_ATTRIBUTES_NAME);
    for (String stat : allStats) {
      counters.get(stat);
    }

    exported = true;
  }

  private void increment(String stat) {
    assertFullyExported();
    counters.get(stat).incrementAndGet();
  }

  @VisibleForTesting
  static String attemptsStatName(boolean production) {
    return "preemptor_attempts_for_" + name(production);
  }

  void recordPreemptionAttemptFor(ITaskConfig task) {
    increment(attemptsStatName(task.isProduction()));
  }

  @VisibleForTesting
  static String successStatName(boolean production) {
    return "preemptor_tasks_preempted_" + name(production);
  }

  void recordTaskPreemption(PreemptionVictim victim) {
    increment(successStatName(victim.isProduction()));
  }

  @VisibleForTesting
  static String failureStatName(boolean production) {
    return "preemptor_no_slots_found_for_" + name(production);
  }

  void recordMissingAttributes() {
    increment(MISSING_ATTRIBUTES_NAME);
  }

  void recordPreemptionFailure(ITaskConfig task) {
    increment(failureStatName(task.isProduction()));
  }
}
