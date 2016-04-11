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
package org.apache.aurora.scheduler.events;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.resources.ResourceSlot;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class NotifyingSchedulingFilterTest extends EasyMockTest {

  private static final ITaskConfig TASK = ITaskConfig.build(new TaskConfig()
      .setNumCpus(1)
      .setRamMb(1024)
      .setDiskMb(1024));
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK);
  private static final UnusedResource RESOURCE = new UnusedResource(
      ResourceSlot.from(TASK),
      IHostAttributes.build(new HostAttributes().setHost("host").setMode(MaintenanceMode.NONE)));
  private static final ResourceRequest REQUEST =
      new ResourceRequest(TASK, AttributeAggregate.EMPTY);

  private static final Veto VETO_1 = Veto.insufficientResources("ram", 1);
  private static final Veto VETO_2 = Veto.insufficientResources("ram", 2);

  private SchedulingFilter filter;
  private EventSink eventSink;
  private SchedulingFilter delegate;

  @Before
  public void setUp() {
    delegate = createMock(SchedulingFilter.class);
    eventSink = createMock(EventSink.class);
    filter = new NotifyingSchedulingFilter(delegate, eventSink);
  }

  @Test
  public void testEvents() {
    Set<Veto> vetoes = ImmutableSet.of(VETO_1, VETO_2);
    expect(delegate.filter(RESOURCE, REQUEST)).andReturn(vetoes);
    eventSink.post(new Vetoed(GROUP_KEY, vetoes));

    control.replay();

    assertEquals(vetoes, filter.filter(RESOURCE, REQUEST));
  }

  @Test
  public void testNoVetoes() {
    Set<Veto> vetoes = ImmutableSet.of();
    expect(delegate.filter(RESOURCE, REQUEST)).andReturn(vetoes);

    control.replay();

    assertEquals(vetoes, filter.filter(RESOURCE, REQUEST));
  }
}
