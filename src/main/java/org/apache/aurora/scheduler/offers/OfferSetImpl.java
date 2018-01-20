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

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;

/**
 * Default implementation for OfferSet. Backed by a ConcurrentSkipListSet.
 */
@VisibleForTesting
public class OfferSetImpl implements OfferSet {

  private final Set<HostOffer> offers;

  @Inject
  public OfferSetImpl(Ordering<HostOffer> ordering) {
    offers = new ConcurrentSkipListSet<>(ordering);
  }

  @Override
  public void add(HostOffer offer) {
    offers.add(offer);
  }

  @Override
  public void remove(HostOffer removed) {
    offers.remove(removed);
  }

  @Override
  public int size() {
    // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more expensive.
    // Could track this separately if it turns out to pose problems.
    return offers.size();
  }

  @Override
  public void clear() {
    offers.clear();
  }

  @Override
  public Iterable<HostOffer> values() {
    return offers;
  }

  @Override
  public Iterable<HostOffer> getOrdered(TaskGroupKey groupKey, ResourceRequest resourceRequest) {
    return offers;
  }
}
