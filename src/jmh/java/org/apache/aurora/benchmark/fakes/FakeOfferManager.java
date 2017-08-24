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
package org.apache.aurora.benchmark.fakes;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.mesos.v1.Protos;

public class FakeOfferManager implements OfferManager {
  @Override
  public void addOffer(HostOffer offer) {
    // no-op
  }

  @Override
  public boolean cancelOffer(Protos.OfferID offerId) {
    return false;
  }

  @Override
  public void banOffer(Protos.OfferID offerId) {
    // no-op
  }

  @Override
  public void launchTask(Protos.OfferID offerId, Protos.TaskInfo taskInfo) throws LaunchException {
    // no-op
  }

  @Override
  public void banOfferForTaskGroup(Protos.OfferID offerId, TaskGroupKey groupKey) {
    // no-op
  }

  @Override
  public Iterable<HostOffer> getOffers(TaskGroupKey groupKey) {
    return null;
  }

  @Override
  public void hostAttributesChanged(PubsubEvent.HostAttributesChanged change) {
    // no-op
  }

  @Override
  public Iterable<HostOffer> getOffers() {
    return null;
  }

  @Override
  public Optional<HostOffer> getOffer(Protos.AgentID agentId) {
    return Optional.absent();
  }
}
