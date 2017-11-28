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
package org.apache.aurora.scheduler.scheduling;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;

/**
 * Injected into {@link TaskAssignerImpl}, this class scores the offers available and returns an
 * option containing the offer to use.
 */
public interface OfferSelector {

  /**
   * Score offers that fit within the given {@link ResourceRequest} and return an option containing
   * the offer to use for assignment.
   *
   * @param offers A stream of offers that match the given {@link ResourceRequest}.
   * @param resourceRequest The {@link ResourceRequest} for the task to assign.
   * @return An {@link Optional} containing the offer to use.
   */
  Optional<HostOffer> select(Iterable<HostOffer> offers, ResourceRequest resourceRequest);
}
