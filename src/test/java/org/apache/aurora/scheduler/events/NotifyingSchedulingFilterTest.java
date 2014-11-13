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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.events.PubsubEvent.Vetoed;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
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
  private static final ResourceSlot TASK_RESOURCES = ResourceSlot.from(TASK);
  private static final String TASK_ID = "taskId";
  private static final IHostAttributes ATTRIBUTES =
      IHostAttributes.build(new HostAttributes().setHost("host").setMode(MaintenanceMode.NONE));

  private static final Veto VETO_1 = new Veto("veto1", 1);
  private static final Veto VETO_2 = new Veto("veto2", 2);

  private SchedulingFilter filter;
  private EventSink eventSink;
  private SchedulingFilter delegate;
  private AttributeAggregate emptyJob;

  @Before
  public void setUp() {
    delegate = createMock(SchedulingFilter.class);
    eventSink = createMock(EventSink.class);
    filter = new NotifyingSchedulingFilter(delegate, eventSink);
    emptyJob = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        createMock(AttributeStore.class));
  }

  @Test
  public void testEvents() {
    Set<Veto> vetoes = ImmutableSet.of(VETO_1, VETO_2);
    expect(delegate.filter(TASK_RESOURCES, ATTRIBUTES, TASK, TASK_ID, emptyJob)).andReturn(vetoes);
    eventSink.post(new Vetoed(TASK_ID, vetoes));

    control.replay();

    assertEquals(vetoes, filter.filter(TASK_RESOURCES, ATTRIBUTES, TASK, TASK_ID, emptyJob));
  }

  @Test
  public void testNoVetoes() {
    Set<Veto> vetoes = ImmutableSet.of();
    expect(delegate.filter(TASK_RESOURCES, ATTRIBUTES, TASK, TASK_ID, emptyJob)).andReturn(vetoes);

    control.replay();

    assertEquals(vetoes, filter.filter(TASK_RESOURCES, ATTRIBUTES, TASK, TASK_ID, emptyJob));
  }
}
