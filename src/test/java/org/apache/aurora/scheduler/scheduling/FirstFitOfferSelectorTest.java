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

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FirstFitOfferSelectorTest extends EasyMockTest {

  private static final IAssignedTask TASK = makeTask("id", JOB).getAssignedTask();
  private static final ResourceRequest EMPTY_REQUEST = new ResourceRequest(
      TASK.getTask(),
      ResourceBag.EMPTY,
      empty());

  private OfferSelector firstFitOfferSelector;

  @Before
  public void setUp() {
    firstFitOfferSelector = new FirstFitOfferSelector();
  }

  @Test
  public void testNoOffers() {
    Iterable<HostOffer> offers = ImmutableList.of();

    control.replay();

    assertFalse(firstFitOfferSelector.select(offers, EMPTY_REQUEST).isPresent());
  }

  @Test
  public void testReturnFirstOffer() {
    HostOffer offerA = createMock(HostOffer.class);
    HostOffer offerB = createMock(HostOffer.class);
    Iterable<HostOffer> offers = ImmutableList.of(offerA, offerB);

    control.replay();

    assertEquals(offerA, firstFitOfferSelector.select(offers, EMPTY_REQUEST).get());
  }
}
