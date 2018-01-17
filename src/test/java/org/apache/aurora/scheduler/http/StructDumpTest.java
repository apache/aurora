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
package org.apache.aurora.scheduler.http;

import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StructDumpTest extends AbstractJettyTest {

  private static final String SLAVE_HOST = "fakehost";
  private static final String FAKE_TASKID = "fake-task-id";

  private static final IScheduledTask TASK = IScheduledTask.build(
      new ScheduledTask()
          .setStatus(ScheduleStatus.RUNNING)
          .setAssignedTask(
              new AssignedTask()
                  .setTaskId(FAKE_TASKID)
                  .setTask(new TaskConfig()
                      .setResources(ImmutableSet.of(
                          Resource.numCpus(1),
                          Resource.ramMb(4000),
                          Resource.diskMb(10000)
                          )))
                  .setSlaveHost(SLAVE_HOST)));

  @Test
  public void testGetUsage() {
    replayAndStart();

    ClientResponse response = getPlainRequestBuilder("/structdump")
        .get(ClientResponse.class);

    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testTaskConfigDoesNotIncludeMetadata() {
    storage.expectOperations();
    storage.expectTaskFetch(FAKE_TASKID, TASK);

    replayAndStart();

    ClientResponse response = getPlainRequestBuilder("/structdump/task/" + FAKE_TASKID)
            .get(ClientResponse.class);

    String htmlResponse = response.getEntity(String.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertFalse(htmlResponse.contains("__isset_bitfield"));
  }

  @Test
  public void testTaskConfigFieldRename() {
    storage.expectOperations();
    storage.expectTaskFetch(FAKE_TASKID, TASK);

    replayAndStart();

    ClientResponse response = getPlainRequestBuilder("/structdump/task/" + FAKE_TASKID)
            .get(ClientResponse.class);

    String htmlResponse = response.getEntity(String.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertFalse(htmlResponse.contains("setField_"));
    assertFalse(htmlResponse.contains("value_"));
  }
}
