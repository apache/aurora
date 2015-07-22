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

import com.google.common.base.Function;
import com.google.common.base.Optional;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.mesos.Protos;

public class FakeOfferManager implements OfferManager {
  @Override
  public void addOffer(HostOffer offer) {
    // no-op
  }

  @Override
  public void cancelOffer(Protos.OfferID offer) {
    // no-op
  }

  @Override
  public boolean launchFirst(
      Function<HostOffer, TaskAssigner.Assignment> acceptor,
      TaskGroupKey groupKey) throws LaunchException {
    return false;
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
  public Optional<HostOffer> getOffer(Protos.SlaveID slaveId) {
    return Optional.absent();
  }
}
