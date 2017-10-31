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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Ordering;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.HostOffer;

import static java.util.Objects.requireNonNull;

/**
 * Settings required to create an OfferManager.
 */
public class OfferSettings {

  private final Amount<Long, Time> filterDuration;
  private final Ordering<HostOffer> ordering;
  private final CacheBuilder<Object, Object> staticBanCacheBuilder;

  @VisibleForTesting
  public OfferSettings(Amount<Long, Time> filterDuration,
                       List<OfferOrder> ordering,
                       Amount<Long, Time> maxHoldTime,
                       long staticBanCacheMaxSize,
                       Ticker staticBanCacheTicker) {

    this(filterDuration,
        OfferOrderBuilder.create(ordering),
        maxHoldTime,
        staticBanCacheMaxSize,
        staticBanCacheTicker);
  }

  OfferSettings(Amount<Long, Time> filterDuration,
                Ordering<HostOffer> ordering,
                Amount<Long, Time> maxHoldTime,
                long staticBanCacheMaxSize,
                Ticker staticBanTicker) {

    this.filterDuration = requireNonNull(filterDuration);
    this.ordering = requireNonNull(ordering);
    this.staticBanCacheBuilder = CacheBuilder.newBuilder()
        .expireAfterWrite(maxHoldTime.as(Time.SECONDS), Time.SECONDS.getTimeUnit())
        .maximumSize(staticBanCacheMaxSize)
        .ticker(staticBanTicker)
        .recordStats();
  }

  /**
   * Duration after which we want Mesos to re-offer unused or declined resources.
   */
  public Amount<Long, Time> getFilterDuration() {
    return filterDuration;
  }

  /**
   * The ordering to use when fetching offers from OfferManager.
   */
  public Ordering<HostOffer> getOrdering() {
    return ordering;
  }

  /**
   * The builder for the static ban cache. Cache settings (e.g. max size, entry expiration) should
   * already be added to the builder by this point.
   */
  public CacheBuilder<Object, Object> getStaticBanCacheBuilder() {
    return staticBanCacheBuilder;
  }
}
