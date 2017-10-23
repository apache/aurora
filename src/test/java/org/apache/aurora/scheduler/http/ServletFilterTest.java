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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;

import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.BYPASS_LEADER_REDIRECT_HEADER_NAME;
import static org.junit.Assert.assertEquals;

public class ServletFilterTest extends AbstractJettyTest {

  protected ClientResponse get(String path) {
    return getRequestBuilder(path)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .get(ClientResponse.class);
  }

  protected ClientResponse post(String path, String body) {
    return getRequestBuilder(path)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .type(MediaType.TEXT_PLAIN_TYPE)
        .post(ClientResponse.class, body);
  }

  private void assertContentEncoding(ClientResponse response, Optional<String> encoding) {
    assertEquals(encoding.orNull(), response.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING));
  }

  private void assertGzipEncodedGet(String path) {
    assertContentEncoding(get(path), Optional.of("gzip"));
  }

  @Test
  public void testGzipEncoding() throws Exception {
    replayAndStart();

    assertGzipEncodedGet("/");
    assertGzipEncodedGet("/scheduler");
    assertGzipEncodedGet("/scheduler/");
    assertGzipEncodedGet("/scheduler/role");
    assertGzipEncodedGet("/scheduler/role/");
    assertGzipEncodedGet("/scheduler/role/env/");
    assertGzipEncodedGet("/scheduler/role/env/job");
    assertGzipEncodedGet("/scheduler/role/env/job/");

    assertGzipEncodedGet("/updates");
    assertGzipEncodedGet("/updates/");

    assertGzipEncodedGet("/assets/bower_components/angular/angular.js");

  }

  private void assertResponseStatus(
      String path,
      Status expectedStatus,
      Optional<URL> responseResource) throws IOException {

    ClientResponse response = get(path);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());

    if (responseResource.isPresent()) {
      assertEquals(
          Resources.toString(responseResource.get(), StandardCharsets.UTF_8),
          response.getEntity(String.class));
    }
  }

  private void leaderRedirectSmokeTest(Status expectedStatus, Optional<URL> responseResource)
      throws IOException {

    assertResponseStatus("/scheduler", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role/env", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role/env/job", expectedStatus, responseResource);

    assertResponseStatus("/updates", expectedStatus, responseResource);
    assertResponseStatus("/updates/", expectedStatus, responseResource);
  }

  @Test
  public void testLeaderRedirect() throws Exception {
    replayAndStart();

    assertResponseStatus("/", Status.OK, Optional.absent());

    // If there's no leader, we should send service unavailable and an error page.
    unsetLeadingSchduler();
    leaderRedirectSmokeTest(
        Status.SERVICE_UNAVAILABLE,
        Optional.of(
            Resources.getResource(
                LeaderRedirectFilter.class,
                LeaderRedirectFilter.NO_LEADER_PAGE)));

    // This process is leading
    setLeadingScheduler(httpServer.getHost(), httpServer.getPort());
    leaderRedirectSmokeTest(Status.OK, Optional.absent());

    setLeadingScheduler("otherHost", 1234);
    leaderRedirectSmokeTest(Status.TEMPORARY_REDIRECT, Optional.absent());
    assertResponseStatus("/", Status.OK, Optional.absent());
  }

  @Test
  public void testHeaderOverridesLeaderRedirect() throws Exception {
    replayAndStart();

    unsetLeadingSchduler();

    ClientResponse response = getRequestBuilder("/scheduler")
        .header(BYPASS_LEADER_REDIRECT_HEADER_NAME, "true")
        .get(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
}
