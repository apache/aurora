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
package org.apache.aurora.scheduler.preemptor;

import java.util.Set;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
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

  @VisibleForTesting
  static final String TASK_PROCESSOR_RUN_NAME = "preemptor_task_processor_runs";

  private volatile boolean exported = false;
  private final CachedCounters counters;

  @Inject
  PreemptorMetrics(CachedCounters counters) {
    this.counters = requireNonNull(counters);
    assertFullyExported();
  }

  private static String prod(boolean production) {
    return production ? "prod" : "non_prod";
  }

  private static String result(boolean success) {
    return success ? "successful" : "failed";
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
        slotSearchStatName(true, false),
        slotSearchStatName(false, false),
        slotSearchStatName(true, true),
        slotSearchStatName(false, true),
        slotValidationStatName(true),
        slotValidationStatName(false),
        MISSING_ATTRIBUTES_NAME,
        TASK_PROCESSOR_RUN_NAME);
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
    return "preemptor_slot_search_attempts_for_" + prod(production);
  }

  @VisibleForTesting
  static String successStatName(boolean production) {
    return "preemptor_tasks_preempted_" + prod(production);
  }

  @VisibleForTesting
  static String slotSearchStatName(boolean success, boolean production) {
    return "preemptor_slot_search_" + result(success) + "_for_" + prod(production);
  }

  @VisibleForTesting
  static String slotValidationStatName(boolean success) {
    return "preemptor_slot_validation_" + result(success);
  }

  void recordPreemptionAttemptFor(ITaskConfig task) {
    increment(attemptsStatName(task.isProduction()));
  }

  void recordTaskPreemption(PreemptionVictim victim) {
    increment(successStatName(victim.isProduction()));
  }

  void recordSlotSearchResult(Optional<?> result, ITaskConfig task) {
    increment(slotSearchStatName(result.isPresent(), task.isProduction()));
  }

  void recordSlotValidationResult(Optional<?> result) {
    increment(slotValidationStatName(result.isPresent()));
  }

  void recordMissingAttributes() {
    increment(MISSING_ATTRIBUTES_NAME);
  }

  void recordTaskProcessorRun() {
    increment(TASK_PROCESSOR_RUN_NAME);
  }
}
