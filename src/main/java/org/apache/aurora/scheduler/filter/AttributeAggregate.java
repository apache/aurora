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
package org.apache.aurora.scheduler.filter;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AtomicLongMap;
import com.twitter.common.collections.Pair;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * A temporary view of a job's state. Once constructed, instances of this class should be discarded
 * once the job state may change (e.g. after exiting a write transaction). This is intended to
 * capture job state once and avoid redundant queries.
 * <p>
 * Note that while the state injected into this class is used lazily (to allow for queries to happen
 * only on-demand), calling {@link #equals(Object)} and {@link #hashCode()} rely on the aggregation
 * result, thus invoking the {@link Supplier} and {@link AttributeStore}.
 */
public class AttributeAggregate {

  /**
   * A lazily-computed mapping from attribute name and value to the count of tasks with that
   * name/value combination.  See doc for {@link #getNumTasksWithAttribute(String, String)} for
   * further details.
   */
  private final Supplier<Map<Pair<String, String>, Long>> aggregate;

  /**
   * Initializes an {@link AttributeAggregate} instance from data store.
   *
   * @param storeProvider Store provider to get data from.
   * @param jobKey Job key.
   * @return An {@link AttributeAggregate} instance.
   */
  public static AttributeAggregate getJobActiveState(
      final StoreProvider storeProvider,
      final IJobKey jobKey) {

    Supplier<ImmutableSet<IScheduledTask>> taskSupplier = Suppliers.memoize(
        new Supplier<ImmutableSet<IScheduledTask>>() {
          @Override
          public ImmutableSet<IScheduledTask> get() {
            return storeProvider.getTaskStore().fetchTasks(
                Query.jobScoped(jobKey).byStatus(Tasks.SLAVE_ASSIGNED_STATES));
          }
        });
    return new AttributeAggregate(taskSupplier, storeProvider.getAttributeStore());
  }

  /**
   * Creates a new attribute aggregate, which will be computed from the provided external state.
   *
   * @param activeTaskSupplier Supplier of active tasks within the aggregated scope.
   * @param attributeStore Source of host attributes to associate with tasks.
   */
  public AttributeAggregate(
      final Supplier<ImmutableSet<IScheduledTask>> activeTaskSupplier,
      final AttributeStore attributeStore) {

    requireNonNull(activeTaskSupplier);
    requireNonNull(attributeStore);

    final Function<IScheduledTask, Iterable<IAttribute>> getHostAttributes =
        new Function<IScheduledTask, Iterable<IAttribute>>() {
          @Override
          public Iterable<IAttribute> apply(IScheduledTask task) {
            // Note: this assumes we have access to attributes for hosts where all active tasks
            // reside.
            String host = requireNonNull(task.getAssignedTask().getSlaveHost());
            return attributeStore.getHostAttributes(host).get().getAttributes();
          }
        };

    aggregate = Suppliers.memoize(new Supplier<Map<Pair<String, String>, Long>>() {
      @Override
      public Map<Pair<String, String>, Long> get() {
        AtomicLongMap<Pair<String, String>> counts = AtomicLongMap.create();
        Iterable<IAttribute> allAttributes =
            Iterables.concat(Iterables.transform(activeTaskSupplier.get(), getHostAttributes));
        for (IAttribute attribute : allAttributes) {
          for (String value : attribute.getValues()) {
            counts.incrementAndGet(Pair.of(attribute.getName(), value));
          }
        }

        return ImmutableMap.copyOf(counts.asMap());
      }
    });
  }

  /**
   * Gets the total number of tasks with a given attribute name and value combination.
   * <p>
   * For example, the counts for a group of tasks that are host-diverse but not rack-diverse, you
   * may have counts like this:
   * <pre>
   * {
   *   ("host", "hostA"): 1
   *   ("host", "hostB"): 1
   *   ("rack", "rackA"): 2
   * }
   * </pre>
   *
   * @param name Name of the attribute.
   * @param value Value of the attribute.
   * @return Number of tasks in the job whose hosts have the provided attribute name and value.
   */
  public long getNumTasksWithAttribute(String name, String value) {
    return Optional.fromNullable(aggregate.get().get(Pair.of(name, value)))
        .or(0L);
  }

  @VisibleForTesting
  Map<Pair<String, String>, Long> getAggregates() {
    return aggregate.get();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AttributeAggregate)) {
      return false;
    }

    AttributeAggregate other = (AttributeAggregate) o;
    return getAggregates().equals(other.getAggregates());
  }

  @Override
  public int hashCode() {
    return getAggregates().hashCode();
  }
}
