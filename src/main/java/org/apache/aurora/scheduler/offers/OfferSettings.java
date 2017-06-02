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

import com.google.common.base.Supplier;
import com.google.common.collect.Ordering;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.HostOffer;

import static java.util.Objects.requireNonNull;

/**
 * Settings required to create an OfferManager.
 */
public class OfferSettings {

  private final Amount<Long, Time> offerFilterDuration;
  private final Supplier<Amount<Long, Time>> returnDelaySupplier;
  private final Ordering<HostOffer> offerOrder;

  public OfferSettings(
      Amount<Long, Time> offerFilterDuration,
      Supplier<Amount<Long, Time>> returnDelaySupplier,
      List<OfferOrder> offerOrder) {

    this(offerFilterDuration, returnDelaySupplier, OfferOrderBuilder.create(offerOrder));
  }

  OfferSettings(
      Amount<Long, Time> offerFilterDuration,
      Supplier<Amount<Long, Time>> returnDelaySupplier,
      Ordering<HostOffer> offerOrder) {

    this.offerFilterDuration = requireNonNull(offerFilterDuration);
    this.returnDelaySupplier = requireNonNull(returnDelaySupplier);
    this.offerOrder = requireNonNull(offerOrder);
  }

  /**
   * Duration after which we want Mesos to re-offer unused or declined resources.
   */
  public Amount<Long, Time> getOfferFilterDuration() {
    return offerFilterDuration;
  }

  /**
   * The amount of time after which an unused offer should be 'returned' to Mesos by declining it.
   * The delay is calculated for each offer using a random duration within a fixed window.
   */
  public Amount<Long, Time> getOfferReturnDelay() {
    return returnDelaySupplier.get();
  }

  /**
   * The ordering to use when fetching offers from OfferManager.
   */
  public Ordering<HostOffer> getOfferOrder() {
    return offerOrder;
  }
}
