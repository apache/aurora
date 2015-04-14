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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
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
 * TODO(wfarner): Consider preserving this as only a helper class to compute the Multiset
 * representing the aggregate, since this class is now a thin wrapper over a Multiset.
 */
public final class AttributeAggregate {

  /**
   * A mapping from attribute name and value to the count of tasks with that name/value combination.
   * See doc for {@link #getNumTasksWithAttribute(String, String)} for further details.
   */
  private final Supplier<Multiset<Pair<String, String>>> aggregate;

  private AttributeAggregate(Supplier<Multiset<Pair<String, String>>> aggregate) {
    this.aggregate = Suppliers.memoize(aggregate);
  }

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

    return create(
        new Supplier<Iterable<IScheduledTask>>() {
          @Override
          public Iterable<IScheduledTask> get() {
            return storeProvider.getTaskStore()
                .fetchTasks(Query.jobScoped(jobKey).byStatus(Tasks.SLAVE_ASSIGNED_STATES));
          }
        },
        storeProvider.getAttributeStore());
  }

  @VisibleForTesting
  static AttributeAggregate create(
      final Supplier<Iterable<IScheduledTask>> taskSupplier,
      final AttributeStore attributeStore) {

    final Function<String, Iterable<IAttribute>> getHostAttributes =
        new Function<String, Iterable<IAttribute>>() {
          @Override
          public Iterable<IAttribute> apply(String host) {
            // Note: this assumes we have access to attributes for hosts where all active tasks
            // reside.
            requireNonNull(host);
            return attributeStore.getHostAttributes(host).get().getAttributes();
          }
        };

    return create(Suppliers.compose(
        new Function<Iterable<IScheduledTask>, Iterable<IAttribute>>() {
          @Override
          public Iterable<IAttribute> apply(Iterable<IScheduledTask> tasks) {
            return FluentIterable.from(tasks)
                .transform(Tasks.SCHEDULED_TO_SLAVE_HOST)
                .transformAndConcat(getHostAttributes);
          }
        },
        taskSupplier));
  }

  @VisibleForTesting
  static AttributeAggregate create(Supplier<Iterable<IAttribute>> attributes) {
    Supplier<Multiset<Pair<String, String>>> aggregator = Suppliers.compose(
        new Function<Iterable<IAttribute>, Multiset<Pair<String, String>>>() {
          @Override
          public Multiset<Pair<String, String>> apply(Iterable<IAttribute> attributes) {
            ImmutableMultiset.Builder<Pair<String, String>> builder = ImmutableMultiset.builder();
            for (IAttribute attribute : attributes) {
              for (String value : attribute.getValues()) {
                builder.add(Pair.of(attribute.getName(), value));
              }
            }

            return builder.build();
          }
        },
        attributes
    );

    return new AttributeAggregate(aggregator);
  }

  @VisibleForTesting
  public static final AttributeAggregate EMPTY =
      new AttributeAggregate(Suppliers.<Multiset<Pair<String, String>>>ofInstance(
          ImmutableMultiset.<Pair<String, String>>of()));

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
    return aggregate.get().count(Pair.of(name, value));
  }

  @VisibleForTesting
  Multiset<Pair<String, String>> getAggregates() {
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
