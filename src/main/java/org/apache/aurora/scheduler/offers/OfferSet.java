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
package org.apache.aurora.scheduler.offers;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;

/**
 * A set that holds all offers within the scheduler, used internally by HostOffers. This
 * interface is injectable via the '-offer_set_module' flag.
 */
public interface OfferSet {

  void add(HostOffer offer);

  void remove(HostOffer removed);

  int size();

  void clear();

  Iterable<HostOffer> values();

  /**
   * Get an ordered stream of offers to consider given a {@link TaskGroupKey} and
   * {@link ResourceRequest} as context. For example, an implementation may return a different
   * ordering depending on the name of the task, or if the task is revocable.
   */
  Iterable<HostOffer> getOrdered(TaskGroupKey groupKey, ResourceRequest resourceRequest);
}
