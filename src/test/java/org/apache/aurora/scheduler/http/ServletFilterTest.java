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

import javax.ws.rs.core.HttpHeaders;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServletFilterTest extends JettyServerModuleTest {

  protected ClientResponse get(String path) {
    return getRequestBuilder(path)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .get(ClientResponse.class);
  }

  private void assertContentEncoding(String path, Optional<String> encoding) {
    assertEquals(encoding.orNull(), get(path).getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING));
  }

  private void assertGzipEncoded(String path) {
    assertContentEncoding(path, Optional.of("gzip"));
  }

  @Test
  public void testGzipEncoding() throws Exception {
    replayAndStart();

    assertContentEncoding("/", Optional.<String>absent());
    assertGzipEncoded("/scheduler");
    assertGzipEncoded("/scheduler/");
    assertGzipEncoded("/scheduler/role");
    assertGzipEncoded("/scheduler/role/");
    assertGzipEncoded("/scheduler/role/env/");
    assertGzipEncoded("/scheduler/role/env/job");
    assertGzipEncoded("/scheduler/role/env/job/");
  }

  private void assertResponseStatus(String path, Status expectedStatus) {
    ClientResponse response = get(path);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());
  }

  private void setLeadingScheduler(String host, int port) {
    ServiceInstance instance = new ServiceInstance()
        .setAdditionalEndpoints(ImmutableMap.of("http", new Endpoint(host, port)));
    schedulerWatcher.getValue().onChange(ImmutableSet.of(instance));
  }

  private void leaderRedirectSmokeTest(Status expectedStatus) {
    assertResponseStatus("/scheduler", expectedStatus);
    assertResponseStatus("/scheduler/", expectedStatus);
    assertResponseStatus("/scheduler/role", expectedStatus);
    assertResponseStatus("/scheduler/role/env", expectedStatus);
    assertResponseStatus("/scheduler/role/env/job", expectedStatus);
  }

  @Test
  public void testLeaderRedirect() throws Exception {
    replayAndStart();

    assertResponseStatus("/", Status.OK);

    // Scheduler is assumed leader at this point, since no members are present in the service
    // (not even this process).
    leaderRedirectSmokeTest(Status.OK);

    // This process is leading
    setLeadingScheduler(httpServer.getHostName(), httpServer.getPort());
    leaderRedirectSmokeTest(Status.OK);

    setLeadingScheduler("otherHost", 1234);
    leaderRedirectSmokeTest(Status.FOUND);
    assertResponseStatus("/", Status.OK);
  }
}
