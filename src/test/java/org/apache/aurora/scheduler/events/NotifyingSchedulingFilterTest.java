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
import org.apache.aurora.scheduler.TaskVars;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class NotifyingSchedulingFilterTest extends EasyMockTest {

  private static final ITaskConfig TASK = ITaskConfig.build(new TaskConfig()
      .setResources(ImmutableSet.of(
          numCpus(1),
          ramMb(1024),
          diskMb(1024))));
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK);
  private static final UnusedResource RESOURCE = new UnusedResource(
      ResourceManager.bagFromResources(TASK.getResources()),
      IHostAttributes.build(new HostAttributes().setHost("host").setMode(MaintenanceMode.NONE)));
  private static final ResourceRequest REQUEST = TaskTestUtil.toResourceRequest(TASK);

  private static final Veto VETO_1 = Veto.insufficientResources("ram", 1);
  private static final Veto VETO_2 = Veto.insufficientResources("ram", 2);

  private SchedulingFilter filter;
  private NearestFit nearestFit;
  private TaskVars taskVars;
  private SchedulingFilter delegate;

  @Before
  public void setUp() {
    delegate = createMock(SchedulingFilter.class);
    nearestFit = createMock(NearestFit.class);
    taskVars = createMock(TaskVars.class);

    filter = new NotifyingSchedulingFilter(delegate, nearestFit, taskVars);
  }

  @Test
  public void testNotifies() {
    Set<Veto> vetoes = ImmutableSet.of(VETO_1, VETO_2);
    expect(delegate.filter(RESOURCE, REQUEST)).andReturn(vetoes);
    nearestFit.vetoed(GROUP_KEY, vetoes);
    taskVars.taskVetoed(vetoes);

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
