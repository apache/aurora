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
package org.apache.aurora.scheduler.updater;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * The difference between two states of a job.
 */
public final class JobDiff {
  private final Map<Integer, ITaskConfig> replacedInstances;
  private final Set<Integer> replacementInstances;
  private final Map<Integer, ITaskConfig> unchangedInstances;

  /**
   * Creates a job diff containing the instances to be replaced (original state), and instances
   * replacing them (target state). An instance may be absent in either side, which means that
   * it was absent in that state.
   *
   * @param replacedInstances Instances being replaced.
   * @param replacementInstances Instances to replace {@code replacedInstances}.
   * @param unchangedInstances Instances unchanged by the update.
   */
  public JobDiff(
      Map<Integer, ITaskConfig> replacedInstances,
      Set<Integer> replacementInstances,
      Map<Integer, ITaskConfig> unchangedInstances) {

    this.replacedInstances = requireNonNull(replacedInstances);
    this.replacementInstances = requireNonNull(replacementInstances);
    this.unchangedInstances = requireNonNull(unchangedInstances);
  }

  public Map<Integer, ITaskConfig> getReplacedInstances() {
    return replacedInstances;
  }

  public Set<Integer> getReplacementInstances() {
    return replacementInstances;
  }

  public Map<Integer, ITaskConfig> getUnchangedInstances() {
    return unchangedInstances;
  }

  /**
   * Gets instances contained in a {@code scope} that are not a part of the job diff.
   *
   * @param scope Scope to search within.
   * @return Instance IDs in {@code scope} that are not in this job diff.
   */
  public Set<Integer> getOutOfScopeInstances(Set<Integer> scope) {
    Set<Integer> allValidInstances = ImmutableSet.<Integer>builder()
        .addAll(getReplacedInstances().keySet())
        .addAll(getReplacementInstances())
        .addAll(getUnchangedInstances().keySet())
        .build();
    return ImmutableSet.copyOf(Sets.difference(scope, allValidInstances));
  }

  /**
   * Checks whether this diff contains no work to be done.
   *
   * @return {@code true} if this diff is a no-op, otherwise {@code false}.
   */
  public boolean isNoop() {
    return replacedInstances.isEmpty() && replacementInstances.isEmpty();
  }

  @Override
  public int hashCode() {
    return Objects.hash(replacedInstances, replacementInstances, unchangedInstances);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JobDiff)) {
      return false;
    }

    JobDiff other = (JobDiff) o;
    return Objects.equals(getReplacedInstances(), other.getReplacedInstances())
        && Objects.equals(getReplacementInstances(), other.getReplacementInstances())
        && Objects.equals(getUnchangedInstances(), other.getUnchangedInstances());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("replacedInstances", getReplacedInstances())
        .add("replacementInstances", getReplacementInstances())
        .add("unchangedInstances", getUnchangedInstances())
        .toString();
  }

  private static <V> Function<MapDifference.ValueDifference<V>, V> leftValue() {
    return new Function<MapDifference.ValueDifference<V>, V>() {
      @Override
      public V apply(MapDifference.ValueDifference<V> diff) {
        return diff.leftValue();
      }
    };
  }

  private static JobDiff computeUnscoped(
      Map<Integer, ITaskConfig> currentState,
      IJobKey job,
      Map<Integer, ITaskConfig> proposedState) {

    requireNonNull(job);
    requireNonNull(proposedState);

    MapDifference<Integer, ITaskConfig> diff = Maps.difference(currentState, proposedState);

    Map<Integer, ITaskConfig> removedInstances = ImmutableMap.<Integer, ITaskConfig>builder()
        .putAll(diff.entriesOnlyOnLeft())
        .putAll(Maps.transformValues(diff.entriesDiffering(), JobDiff.leftValue()))
        .build();

    Set<Integer> addedInstances = ImmutableSet.<Integer>builder()
        .addAll(diff.entriesOnlyOnRight().keySet())
        .addAll(diff.entriesDiffering().keySet())
        .build();

    return new JobDiff(
        removedInstances,
        addedInstances,
        ImmutableMap.copyOf(diff.entriesInCommon()));
  }

  /**
   * Calculates the diff necessary to change the current state of a job to the proposed state.
   *
   * @param taskStore Store to fetch the job's current state from.
   * @param job Job being diffed.
   * @param proposedState Proposed state to move the job towards.
   * @param scope Instances to limit the diff to.
   * @return A diff of the current state compared with {@code proposedState}, within {@code scope}.
   */
  public static JobDiff compute(
      TaskStore taskStore,
      IJobKey job,
      Map<Integer, ITaskConfig> proposedState,
      Set<IRange> scope) {

    Map<Integer, ITaskConfig> currentState = ImmutableMap.copyOf(
        Maps.transformValues(
            Maps.uniqueIndex(
                taskStore.fetchTasks(Query.jobScoped(job).active()),
                Tasks::getInstanceId),
            Tasks::getConfig));

    JobDiff diff = computeUnscoped(currentState, job, proposedState);
    if (scope.isEmpty()) {
      return diff;
    } else {
      Set<Integer> limit = Numbers.rangesToInstanceIds(scope);
      Map<Integer, ITaskConfig> replaced =
          ImmutableMap.copyOf(Maps.filterKeys(diff.getReplacedInstances(), Predicates.in(limit)));
      Set<Integer> replacements =
          ImmutableSet.copyOf(Sets.intersection(diff.getReplacementInstances(), limit));

      Set<Integer> unchangedIds = ImmutableSet.copyOf(
          Sets.difference(
              ImmutableSet.copyOf(Sets.difference(currentState.keySet(), replaced.keySet())),
              replacements));
      Map<Integer, ITaskConfig> unchanged =
          ImmutableMap.copyOf(Maps.filterKeys(currentState, Predicates.in(unchangedIds)));

      return new JobDiff(replaced, replacements, unchanged);
    }
  }

  /**
   * Creates a map of {@code instanceCount} copies of {@code config}.
   *
   * @param config Configuration to generate an instance mapping for.
   * @param instanceCount Number of instances to represent.
   * @return A map of instance IDs (from 0 to {@code instanceCount - 1}) to {@code config}.
   */
  public static Map<Integer, ITaskConfig> asMap(ITaskConfig config, int instanceCount) {
    requireNonNull(config);

    Set<Integer> desiredInstances = ContiguousSet.create(
        Range.closedOpen(0, instanceCount),
        DiscreteDomain.integers());
    return ImmutableMap.copyOf(Maps.asMap(desiredInstances, Functions.constant(config)));
  }
}
