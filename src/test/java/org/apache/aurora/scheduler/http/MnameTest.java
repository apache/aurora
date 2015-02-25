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

import java.util.Map;
import java.util.Set;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MnameTest extends JettyServerModuleTest {

  private static final String SLAVE_HOST = "fakehost";
  private static final int PORT = 50000;
  private static final String APP_URI = "http://" + SLAVE_HOST + ":" + PORT + "/";

  private static final IScheduledTask TASK = IScheduledTask.build(
      new ScheduledTask()
          .setStatus(ScheduleStatus.RUNNING)
          .setAssignedTask(
              new AssignedTask()
                  .setSlaveHost("fakehost")
                  .setAssignedPorts(ImmutableMap.of("http", 50000))));
  private static final Query.Builder TASK_QUERY =
      Query.instanceScoped(JobKeys.from("myrole", "test", "myjob"), 1).active();

  @Test
  public void testGetUsage() {
    replayAndStart();

    ClientResponse response = getRequestBuilder("/mname")
        .get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testHttpMethods() {
    storage.expectOperations();

    Set<String> methods =
        ImmutableSet.of(HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE);
    storage.expectTaskFetch(TASK_QUERY, TASK).times(methods.size());

    replayAndStart();

    for (String method : methods) {
      ClientResponse response = getRequestBuilder("/mname/myrole/test/myjob/1")
          .method(method, ClientResponse.class);
      assertEquals(Status.TEMPORARY_REDIRECT.getStatusCode(), response.getStatus());
      assertEquals(APP_URI, response.getHeaders().getFirst(HttpHeaders.LOCATION));
    }
  }

  @Test
  public void testForwardPathAndQuery() {
    storage.expectOperations();

    Set<String> methods =
        ImmutableSet.of(HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE);
    storage.expectTaskFetch(TASK_QUERY, TASK).times(methods.size());

    replayAndStart();

    String pathAndQuery = "path?query=2";

    for (String method : methods) {
      ClientResponse response = getRequestBuilder("/mname/myrole/test/myjob/1/" + pathAndQuery)
          .method(method, ClientResponse.class);
      assertEquals(Status.TEMPORARY_REDIRECT.getStatusCode(), response.getStatus());
      assertEquals(APP_URI + pathAndQuery, response.getHeaders().getFirst(HttpHeaders.LOCATION));
    }
  }

  @Test
  public void testInstanceAbsent() {
    storage.expectOperations();

    storage.expectTaskFetch(TASK_QUERY);

    replayAndStart();

    ClientResponse response = getRequestBuilder("/mname/myrole/test/myjob/1")
        .get(ClientResponse.class);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testInstanceNotRunning() {
    storage.expectOperations();

    IScheduledTask pending =
        IScheduledTask.build(TASK.newBuilder().setStatus(ScheduleStatus.PENDING));

    storage.expectTaskFetch(TASK_QUERY, pending);

    replayAndStart();

    ClientResponse response = getRequestBuilder("/mname/myrole/test/myjob/1")
        .get(ClientResponse.class);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testInstanceNoHttp() {
    storage.expectOperations();

    ScheduledTask builder = TASK.newBuilder();
    builder.getAssignedTask().setAssignedPorts(ImmutableMap.of("telnet", 80));
    IScheduledTask noHttp = IScheduledTask.build(builder);

    storage.expectTaskFetch(TASK_QUERY, noHttp);

    replayAndStart();

    ClientResponse response = getRequestBuilder("/mname/myrole/test/myjob/1")
        .get(ClientResponse.class);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testRedirectPort() {
    replayAndStart();

    assertEquals(Optional.<Integer>absent(), getRedirectPort(null));
    assertEquals(Optional.<Integer>absent(), getRedirectPort(ImmutableMap.<String, Integer>of()));
    assertEquals(Optional.<Integer>absent(), getRedirectPort(ImmutableMap.of("thrift", 5)));
    assertEquals(Optional.of(5), getRedirectPort(ImmutableMap.of("health", 5, "http", 6)));
    assertEquals(Optional.of(6), getRedirectPort(ImmutableMap.of("http", 6)));
    assertEquals(Optional.of(7), getRedirectPort(ImmutableMap.of("HTTP", 7)));
    assertEquals(Optional.of(8), getRedirectPort(ImmutableMap.of("web", 8)));
    assertEquals(Optional.of(9), getRedirectPort(ImmutableMap.of("admin", 9)));
  }

  private Optional<Integer> getRedirectPort(Map<String, Integer> ports) {
    return Mname.getRedirectPort(IAssignedTask.build(new AssignedTask().setAssignedPorts(ports)));
  }
}
